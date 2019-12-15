//
// we allow improper_ctypes just to eliminate these warnings:
//      = note: `#[warn(improper_ctypes)]` on by default
//      = note: 128-bit integers don't currently have a known stable ABI

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(non_upper_case_globals)]
#[allow(improper_ctypes)]
mod pg10;

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(non_upper_case_globals)]
#[allow(improper_ctypes)]
mod pg11;

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(non_upper_case_globals)]
#[allow(improper_ctypes)]
mod pg12;
