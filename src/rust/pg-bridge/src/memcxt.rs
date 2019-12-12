#![allow(dead_code)]
use elog_guard::*;
use std::ffi::c_void;

#[elog_guard]
extern "C" {
    pub fn palloc(size: usize) -> *mut u8;
    pub fn pfree(data: *mut c_void);
}
