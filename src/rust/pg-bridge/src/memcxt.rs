#![allow(dead_code)]
use crate as pg_bridge;
use pg_guard::*;
use std::ffi::c_void;

#[pg_guard]
extern "C" {
    pub fn palloc(size: usize) -> *mut u8;
    pub fn pfree(data: *mut c_void);
}
