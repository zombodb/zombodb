#![allow(non_snake_case)]

use std::mem::MaybeUninit;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{compiler_fence, Ordering};

use libc::*;

use crate::log::{elog, ERROR};

mod log;

extern "C" {
    fn sigsetjmp(env: *mut sigjmp_buf, savesigs: c_int) -> c_int;
    fn siglongjmp(env: *mut sigjmp_buf, val: c_int) -> c_void;
}

extern "C" {
    static mut PG_exception_stack: *mut sigjmp_buf;
    static mut error_context_stack: *mut ErrorContextCallback;
}

#[repr(C)]
struct ErrorContextCallback {
    previous: *mut ErrorContextCallback,
    callback: extern "C" fn(arg: *mut c_void),
    arg: *mut c_void,
}

#[repr(C)]
struct sigjmp_buf {
    __jmpbuf: [i64; 9],
    __mask_was_saved: c_int,
    __saved_mask: sigset_t,
}

struct JumpContext {
    jump_value: c_int,
}

pub fn jmp_wrapper<R, F: FnOnce() -> R>(f: F) -> R {
    unsafe {
        let prev_exception_stack: *mut sigjmp_buf = PG_exception_stack;
        let prev_context_stack: *mut ErrorContextCallback = error_context_stack;
        let mut wrapped_jmp_buff: MaybeUninit<sigjmp_buf> = MaybeUninit::uninit();

        compiler_fence(Ordering::SeqCst);
        let jumped = sigsetjmp(wrapped_jmp_buff.as_mut_ptr(), 0);
        if jumped != 0 {
            // caught a longjmp from a Postgres function
            PG_exception_stack = prev_exception_stack;
            error_context_stack = prev_context_stack;

            compiler_fence(Ordering::SeqCst);
            panic!(JumpContext { jump_value: jumped });
        }

        let mut local_sigjmp_buf = wrapped_jmp_buff.assume_init();
        PG_exception_stack = &mut local_sigjmp_buf;

        compiler_fence(Ordering::SeqCst);
        let result = f();

        compiler_fence(Ordering::SeqCst);
        PG_exception_stack = prev_exception_stack;
        error_context_stack = prev_context_stack;

        result
    }
}

pub fn register_panic_handler() {
    std::panic::set_hook(Box::new(|info| {
        compiler_fence(Ordering::SeqCst);
        if let Some(panic_context) = info.payload().downcast_ref::<JumpContext>() {
            // the panic came from a pg longjmp... so unwrap it and rethrow
            unsafe {
                siglongjmp(PG_exception_stack, panic_context.jump_value);
            }
        } else {
            // it's a normal Rust panic

            // convert the panic payload into a message
            let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
                s
            } else if let Some(s) = info.payload().downcast_ref::<String>() {
                &s[..]
            } else {
                "Box<Any>"
            };

            // and raise an error with that message within Postgres
            // this gets us out of Rust and back into Postgres-land where it'll
            // do its normal error handling and transaction cleanup
            elog(ERROR, format!("{}: {}", info, message).as_str());
        }

        unreachable!("failed to properly handle a panic");
    }));
}
