use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

use crate::memcxt::pfree;

extern "C" {
    fn makeStringInfo() -> PostgresStringInfo;
    fn enlargeStringInfo(str: PostgresStringInfo, needed: i32);
    fn resetStringInfo(str: PostgresStringInfo);

    fn appendStringInfoChar(str: PostgresStringInfo, ch: c_char);
    fn appendBinaryStringInfo(str: PostgresStringInfo, data: *const c_char, datalen: i32);
}

#[repr(C)]
#[derive(Debug)]
pub struct PostgresStringInfoData {
    data: *mut c_char,
    len: i32,
    maxlen: i32,
    cursor: i32,
}

pub type PostgresStringInfo = *mut PostgresStringInfoData;

#[derive(Debug)]
pub struct StringInfo {
    sid: PostgresStringInfo,
    is_from_pg: bool,
}

pub trait ReturnToPostgres {
    fn to_pg(self) -> PostgresStringInfo;
}

impl ReturnToPostgres for StringInfo {
    fn to_pg(self) -> *mut PostgresStringInfoData {
        // TODO: doesn't 'self' (which is a StringInfo) get leaked here?
        let rc = self.sid;
        std::mem::forget(self);
        rc
    }
}

impl ReturnToPostgres for String {
    fn to_pg(self) -> *mut PostgresStringInfoData {
        StringInfo::from(self).to_pg()
    }
}

impl ReturnToPostgres for &str {
    fn to_pg(self) -> *mut PostgresStringInfoData {
        StringInfo::from(self).to_pg()
    }
}

impl ReturnToPostgres for Vec<u8> {
    fn to_pg(self) -> *mut PostgresStringInfoData {
        StringInfo::from(self).to_pg()
    }
}

impl ReturnToPostgres for &[u8] {
    fn to_pg(self) -> *mut PostgresStringInfoData {
        StringInfo::from(self).to_pg()
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
    #[allow(dead_code)]
    pub fn new() -> Self {
        StringInfo {
            sid: unsafe { makeStringInfo() },
            is_from_pg: false,
        }
    }

    #[allow(dead_code)]
    pub fn from_pg(sid: PostgresStringInfo) -> Option<Self> {
        if sid.is_null() {
            None
        } else {
            Some(StringInfo {
                sid,
                is_from_pg: true,
            })
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> i32 {
        unsafe { &mut *self.sid }.len
    }

    #[allow(dead_code)]
    pub fn push(&mut self, ch: char) {
        unsafe { appendStringInfoChar(self.sid, ch as c_char) }
    }

    #[allow(dead_code)]
    pub fn push_str(&mut self, s: &str) {
        unsafe { appendBinaryStringInfo(self.sid, s.as_ptr() as *const c_char, s.len() as i32) }
    }

    #[allow(dead_code)]
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        unsafe {
            appendBinaryStringInfo(
                self.sid,
                bytes.as_ptr() as *const c_char,
                bytes.len() as i32,
            )
        }
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        unsafe { resetStringInfo(self.sid) }
    }

    #[allow(dead_code)]
    pub fn enlarge(&mut self, needed: i32) {
        unsafe { enlargeStringInfo(self.sid, needed) }
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
        if self.is_from_pg {
            // don't let Rust try to free self.sid as it's owned by Postgres
            std::mem::forget(self.sid)
        } else {
            // instruct Rust to free self.sid.data, and self.sid
            // via Postgres' pfree()
            unsafe {
                if !self.sid.is_null() {
                    if !(*self.sid).data.is_null() {
                        pfree((*self.sid).data as *mut c_void);
                    }
                    pfree(self.sid as *mut c_void);
                }
            }
        }
    }
}
