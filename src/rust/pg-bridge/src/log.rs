#![allow(dead_code)]
use crate as pg_bridge;
use pg_guard::*;
use std::ffi::CString;
use std::os::raw::c_char;

pub static DEBUG5: i32 = 10;
pub static DEBUG4: i32 = 11;
pub static DEBUG3: i32 = 12;
pub static DEBUG2: i32 = 13;
pub static DEBUG1: i32 = 14;
pub static LOG: i32 = 15;
pub static LOG_SERVER_ONLY: i32 = 16;
pub static COMMERROR: i32 = LOG_SERVER_ONLY;
pub static INFO: i32 = 17;
pub static NOTICE: i32 = 18;
pub static WARNING: i32 = 19;
pub static ERROR: i32 = 20;
pub static PGERROR: i32 = 20;
pub static FATAL: i32 = 21;
pub static PANIC: i32 = 22;

#[pg_guard]
extern "C" {
    fn zdb_log_proxy(level: i32, message: *const c_char);
}

pub fn elog(level: i32, message: &str) {
    unsafe {
        zdb_log_proxy(level, CString::new(message).unwrap().as_ptr());
    }
}
