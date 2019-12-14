#![allow(non_snake_case)]

use crate::log::{elog, ERROR};
use libc::sigset_t;
use std::any::Any;
use std::cell::Cell;
use std::mem::MaybeUninit;
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

thread_local! { static PANIC_LOCATION: Cell<Option<String>> = Cell::new(None) }

fn take_panic_location() -> String {
    PANIC_LOCATION.with(|p| match p.take() {
        Some(s) => s,
        None => "<unknown>".to_string(),
    })
}

pub fn register_panic_handler() {
    std::panic::set_hook(Box::new(|info| {
        PANIC_LOCATION.with(|p| {
            let newval = Some(match p.take() {
                Some(s) => s,
                None => match info.location() {
                    Some(location) => format!("{}", location),
                    None => "<unknown>".to_string(),
                },
            });

            p.replace(newval)
        });
    }))
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
            // remember where Postgres would like to jump to
            compiler_fence(Ordering::SeqCst);
            let prev_exception_stack = PG_exception_stack;

            let result = catch_unwind(|| {
                let mut jmp_buff = MaybeUninit::uninit();

                compiler_fence(Ordering::SeqCst);
                let jump_value = sigsetjmp(jmp_buff.as_mut_ptr(), 0);

                compiler_fence(Ordering::SeqCst);
                if jump_value != 0 {
                    // caught an elog(ERROR), so convert it into a panic!()
                    panic!(JumpContext { jump_value });
                }

                // lie to Postgres about where it should jump when it elog(ERROR)'s
                compiler_fence(Ordering::SeqCst);
                PG_exception_stack = jmp_buff.as_mut_ptr();
                inc_depth(&WRAP_DEPTH);

                // run our wrapped function
                compiler_fence(Ordering::SeqCst);
                f()
            });

            // restore Postgres' understanding of where it should longjmp
            compiler_fence(Ordering::SeqCst);
            dec_depth(&WRAP_DEPTH);
            PG_exception_stack = prev_exception_stack;

            match result {
                Ok(result) => result,
                Err(e) => rethrow_error(e),
            }
        }
    });

    match result {
        Ok(result) => result,
        Err(e) => {
            if get_depth(&WRAP_DEPTH) == 0 {
                let location = take_panic_location();

                // we're not wrapping a function
                match downcast_err(e) {
                    //
                    // the error type is a String, so translate it into an elog(ERROR)
                    //
                    Ok(message) => {
                        elog(ERROR, format!("{} at {}", message, location).as_str());
                        unreachable!("elog(ERROR) failed");
                    }

                    //
                    // the error is a JumpContext, so we need to longjmp to that location
                    //
                    Err(jump_context) => unsafe {
                        compiler_fence(Ordering::SeqCst);
                        siglongjmp(PG_exception_stack, jump_context.jump_value);
                        unreachable!("siglongjmp failed");
                    },
                }
            } else {
                // we're at least one level deep in nesting so rethrow the error
                rethrow_error(e)
            }
        }
    }
}

///
/// rethrow whatever the `e` error is as a Rust `panic!()`
///
fn rethrow_error(e: Box<dyn Any + Send>) -> ! {
    match downcast_err(e) {
        Ok(message) => panic!(message.to_string()),
        Err(jump_context) => panic!(jump_context),
    }
}

///
/// convert types of `e` that we understand/expect into either a
/// `Ok(String)` or a `Err<JumpContext>`
///
fn downcast_err(e: Box<dyn Any + Send>) -> Result<String, JumpContext> {
    if let Some(cxt) = e.downcast_ref::<JumpContext>() {
        Err(cxt.clone())
    } else if let Some(s) = e.downcast_ref::<&str>() {
        Ok(s.to_string())
    } else if let Some(s) = e.downcast_ref::<String>() {
        Ok(s.to_string())
    } else {
        Ok("Box<Any>".to_string())
    }
}
