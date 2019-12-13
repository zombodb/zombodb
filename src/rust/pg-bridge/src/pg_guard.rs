#![allow(non_snake_case)]

use crate::log::{elog, ERROR};
use libc::sigset_t;
use std::any::Any;
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::raw::{c_int, c_void};
use std::panic::catch_unwind;
use std::sync::atomic::{compiler_fence, Ordering};
use std::thread::LocalKey;

extern "C" {
    fn sigsetjmp(env: *mut sigjmp_buf, savesigs: c_int) -> c_int;
    fn siglongjmp(env: *mut sigjmp_buf, val: c_int) -> c_void;
    static mut PG_exception_stack: *mut sigjmp_buf;
}

#[repr(C)]
struct sigjmp_buf {
    __jmpbuf: [i64; 9],
    __mask_was_saved: c_int,
    __saved_mask: sigset_t,
}

#[derive(Clone)]
struct JumpContext {
    jump_value: c_int,
}

fn inc_depth(depth: &'static LocalKey<Cell<usize>>) {
    depth.with(|depth| depth.replace(depth.get() + 1));
}

fn dec_depth(depth: &'static LocalKey<Cell<usize>>) {
    depth.with(|depth| depth.replace(depth.get() - 1));
}

fn get_depth(depth: &'static LocalKey<Cell<usize>>) -> usize {
    depth.with(|depth| depth.get())
}

pub fn guard<R, F: FnOnce() -> R>(f: F) -> R
where
    F: std::panic::UnwindSafe,
{
    thread_local! { static WRAP_DEPTH: Cell<usize> = Cell::new(0) }

    let result = catch_unwind(|| {
        unsafe {
            // remember where Postgres wants to longjmp to in the event of an elog(ERROR)
            let prev_exception_stack: *mut sigjmp_buf = PG_exception_stack;
            let mut wrapped_jmp_buff: MaybeUninit<sigjmp_buf> = MaybeUninit::uninit();

            compiler_fence(Ordering::SeqCst);
            let jumped = sigsetjmp(wrapped_jmp_buff.as_mut_ptr(), 0);
            if jumped != 0 {
                //
                // caught a longjmp
                //

                compiler_fence(Ordering::SeqCst);
                PG_exception_stack = prev_exception_stack;

                if get_depth(&WRAP_DEPTH) > 0 {
                    // rethrow it as a Rust panic
                    compiler_fence(Ordering::SeqCst);
                    dec_depth(&WRAP_DEPTH);

                    compiler_fence(Ordering::SeqCst);
                    panic!(JumpContext { jump_value: jumped });
                } else {
                    // at the top-level, so longjmp back into Postgres
                    compiler_fence(Ordering::SeqCst);
                    siglongjmp(PG_exception_stack, jumped);
                    unreachable!("siglongjmp failed");
                }
            }

            let mut local_sigjmp_buf = wrapped_jmp_buff.assume_init();
            PG_exception_stack = &mut local_sigjmp_buf;

            compiler_fence(Ordering::SeqCst);
            inc_depth(&WRAP_DEPTH);

            // execute the actual function we're wrapping
            let result = catch_unwind(|| f());

            compiler_fence(Ordering::SeqCst);
            dec_depth(&WRAP_DEPTH);

            let result = handle_result(result, get_depth(&WRAP_DEPTH), PG_exception_stack);

            // restore Postgres exception stack pointer
            compiler_fence(Ordering::SeqCst);
            PG_exception_stack = prev_exception_stack;

            result
        }
    });

    handle_result(result, get_depth(&WRAP_DEPTH), unsafe {
        PG_exception_stack
    })
}

fn handle_result<R>(
    result: Result<R, Box<dyn Any + Send>>,
    depth: usize,
    jmp_buff: *mut sigjmp_buf,
) -> R {
    match result {
        Ok(result) => result,
        Err(e) => {
            maybe_panic_or_elog(depth, jmp_buff, e);
            unreachable!("maybe_panic_or_elog somehow returned");
        }
    }
}

fn maybe_panic_or_elog(depth: usize, jmp_buff: *mut sigjmp_buf, e: Box<dyn Any + Send>) {
    match downcast_err(e.deref()) {
        Ok(message) => {
            if depth == 0 {
                elog(ERROR, message);
                unreachable!("elog(ERROR, {:?}) failed", message);
            } else {
                panic!(format!("{}", message));
            }
        }

        Err(cxt) => {
            if depth == 0 {
                unsafe {
                    siglongjmp(jmp_buff, cxt.jump_value);
                }
                unreachable!("siglongjmp failed");
            } else {
                panic!(cxt);
            }
        }
    }
}

fn downcast_err(e: &(dyn Any + Send)) -> Result<&str, JumpContext> {
    if let Some(cxt) = e.downcast_ref::<JumpContext>() {
        Err(cxt.clone())
    } else if let Some(s) = e.downcast_ref::<&str>() {
        Ok(s)
    } else if let Some(s) = e.downcast_ref::<String>() {
        Ok(&s[..])
    } else {
        Ok("Box<Any>")
    }
}
