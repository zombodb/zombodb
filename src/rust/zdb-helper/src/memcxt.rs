#![allow(dead_code)]
use std::ffi::c_void;
use elog_guard::*;

#[elog_guard]
extern "C" {
    pub fn palloc(size: usize) -> *mut u8;
    pub fn pfree(data: *mut c_void);
}
