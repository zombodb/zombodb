use std::panic::{UnwindSafe, catch_unwind};
use crate::log::{elog, ERROR};


pub fn panic_guard<F: FnOnce() -> R + UnwindSafe, R>(user_function: F) -> R {
    let result = catch_unwind(|| {
        user_function()
    });
    let result = result.map_err(|e| match e.downcast_ref::<&str>() {
        Some(andstr) => Some(andstr.to_string()),
        None => match e.downcast_ref::<String>() {
            Some(string) => Some(string.to_string()),
            None => None,
        },
    });

    if result.is_ok() {
        return result.unwrap();
    } else {
        match result.err().unwrap() {
            Some(message) => {
                elog(ERROR, message.as_str())
            }
            None => elog(ERROR, "panic in rust code")
        }
        // the elog(ERROR, ...) calls above cause Postgres to longjmp away from here
        unreachable!("failed to propagate a panic into an elog(ERROR)");
    }
}