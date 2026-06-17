use crate::Database;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

pub struct LightningConnection {
    conn: crate::Connection,
}

/// Convert a C string pointer to a Rust &str.
///
/// # Safety
/// The caller must ensure:
/// - `ptr` is non-null and points to a valid, null-terminated C string.
/// - The memory pointed to by `ptr` remains valid for the lifetime of the
///   returned `&str`. For FFI callers, this means the C caller must not free
///   the string while the returned reference is in use.
/// - The string content is valid UTF-8 (or the function will return an error).
unsafe fn c_str_to_str<'a>(ptr: *const c_char) -> Result<&'a str, crate::LightningError> {
    if ptr.is_null() {
        return Err(crate::LightningError::Internal("null pointer".into()));
    }
    let c_str = unsafe { CStr::from_ptr(ptr) };
    c_str
        .to_str()
        .map_err(|_| crate::LightningError::Internal("invalid UTF-8 from C caller".into()))
}

fn c_string_from_str(s: &str) -> Result<CString, crate::LightningError> {
    CString::new(s)
        .map_err(|_| crate::LightningError::Internal("string contains null byte".into()))
}

#[no_mangle]
pub extern "C" fn lightning_open(path: *const c_char) -> *mut LightningConnection {
    let path_str = match unsafe { c_str_to_str(path) } {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let db = match Database::new(
        std::path::Path::new(path_str),
        crate::SystemConfig::default(),
    ) {
        Ok(db) => db,
        Err(_) => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(LightningConnection { conn: db.connect() }))
}

#[no_mangle]
pub extern "C" fn lightning_query(
    conn_ptr: *mut LightningConnection,
    query: *const c_char,
) -> *mut c_char {
    if conn_ptr.is_null() {
        return std::ptr::null_mut();
    }
    let conn_wrapper = unsafe { &*conn_ptr };
    let query_str = match unsafe { c_str_to_str(query) } {
        Ok(s) => s,
        Err(e) => {
            let err_json = format!("{{\"error\": \"{e}\"}}");
            return match c_string_from_str(&err_json) {
                Ok(c) => c.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };
        }
    };

    let conn_obj = &conn_wrapper.conn;
    match conn_obj.query(query_str) {
        Ok(res) => {
            let res_json = match serde_json::to_string(&res) {
                Ok(j) => j,
                Err(e) => {
                    let err_json = format!("{{\"error\": \"{e}\"}}");
                    return match c_string_from_str(&err_json) {
                        Ok(c) => c.into_raw(),
                        Err(_) => std::ptr::null_mut(),
                    };
                }
            };
            match c_string_from_str(&res_json) {
                Ok(c_res) => c_res.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(e) => {
            let err_json = format!("{{\"error\": \"{e}\"}}");
            match c_string_from_str(&err_json) {
                Ok(c_res) => c_res.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn lightning_close(conn: *mut LightningConnection) {
    if !conn.is_null() {
        unsafe {
            let _ = Box::from_raw(conn);
        }
    }
}

/// Free a string returned by `lightning_query`.
///
/// # Safety
/// `s` must be a pointer previously returned by `lightning_query`.
/// Passing any other pointer is undefined behavior. After this call,
/// `s` must not be used again.
#[no_mangle]
pub unsafe extern "C" fn lightning_free_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}
