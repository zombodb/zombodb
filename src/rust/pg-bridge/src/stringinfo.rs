#![allow(dead_code, non_snake_case)]

use crate::pg_sys::externs;
use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

#[derive(Debug)]
pub struct StringInfo {
    sid: externs::StringInfo,
    is_from_pg: bool,
}

pub trait ToPostgres {
    fn to_postgres(self) -> externs::StringInfo;
}

impl ToPostgres for StringInfo {
    fn to_postgres(self) -> externs::StringInfo {
        // TODO: doesn't 'self' (which is a StringInfo) get leaked here?
        let rc = self.sid;
        std::mem::forget(self);
        rc
    }
}

impl ToPostgres for String {
    fn to_postgres(self) -> externs::StringInfo {
        StringInfo::from(self).to_postgres()
    }
}

impl ToPostgres for &str {
    fn to_postgres(self) -> externs::StringInfo {
        StringInfo::from(self).to_postgres()
    }
}

impl ToPostgres for Vec<u8> {
    fn to_postgres(self) -> externs::StringInfo {
        StringInfo::from(self).to_postgres()
    }
}

impl ToPostgres for &[u8] {
    fn to_postgres(self) -> externs::StringInfo {
        StringInfo::from(self).to_postgres()
    }
}

impl ToString for StringInfo {
    fn to_string(&self) -> String {
        unsafe {
            CStr::from_bytes_with_nul_unchecked(std::slice::from_raw_parts(
                (*self.sid).data as *const u8,
                (*self.sid).len as usize + 1, // + 1 to include the null byte
            ))
            .to_string_lossy()
            .to_string()
        }
    }
}

impl StringInfo {
    pub fn new() -> Self {
        StringInfo {
            sid: unsafe { externs::makeStringInfo() },
            is_from_pg: false,
        }
    }

    pub fn from_pg(sid: externs::StringInfo) -> Option<Self> {
        if sid.is_null() {
            None
        } else {
            Some(StringInfo {
                sid,
                is_from_pg: true,
            })
        }
    }

    pub fn len(&self) -> i32 {
        unsafe { &mut *self.sid }.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, ch: char) {
        unsafe { externs::appendStringInfoChar(self.sid, ch as c_char) }
    }

    pub fn push_str(&mut self, s: &str) {
        unsafe {
            externs::appendBinaryStringInfo(self.sid, s.as_ptr() as *const c_char, s.len() as i32)
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) {
        unsafe {
            externs::appendBinaryStringInfo(
                self.sid,
                bytes.as_ptr() as *const c_char,
                bytes.len() as i32,
            )
        }
    }

    pub fn reset(&mut self) {
        unsafe { externs::resetStringInfo(self.sid) }
    }

    pub fn enlarge(&mut self, needed: i32) {
        unsafe { externs::enlargeStringInfo(self.sid, needed) }
    }
}

impl Default for StringInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl From<String> for StringInfo {
    fn from(s: String) -> Self {
        StringInfo::from(s.as_str())
    }
}

impl From<&str> for StringInfo {
    fn from(s: &str) -> Self {
        let mut rc = StringInfo::new();
        rc.push_str(s);
        rc
    }
}

impl From<Vec<u8>> for StringInfo {
    fn from(v: Vec<u8>) -> Self {
        let mut rc = StringInfo::new();
        rc.push_bytes(v.as_slice());
        rc
    }
}

impl From<&[u8]> for StringInfo {
    fn from(v: &[u8]) -> Self {
        let mut rc = StringInfo::new();
        rc.push_bytes(v);
        rc
    }
}

impl Drop for StringInfo {
    fn drop(&mut self) {
        if !self.is_from_pg {
            // instruct Rust to free self.sid.data, and self.sid
            // via Postgres' pfree()
            unsafe {
                if !self.sid.is_null() {
                    if !(*self.sid).data.is_null() {
                        externs::pfree((*self.sid).data as *mut c_void);
                    }
                    externs::pfree(self.sid as *mut c_void);
                }
            }
        }
    }
}
