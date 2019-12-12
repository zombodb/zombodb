use std::ffi::c_void;

extern "C" {
    #[allow(dead_code)]
    pub fn palloc(size: usize) -> *mut u8;

    #[allow(dead_code)]
    pub fn pfree(data: *mut c_void);
}
