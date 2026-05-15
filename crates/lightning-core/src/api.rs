use crate::Database;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

pub struct LightningConnection {
    conn: crate::Connection,
}

#[no_mangle]
pub extern "C" fn lightning_open(path: *const c_char) -> *mut LightningConnection {
    // SAFETY: SAFETY: `path` is a valid C string from the C caller.
    let c_str = unsafe { CStr::from_ptr(path) };
    let path_str = c_str.to_str().unwrap();
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
) -> *const c_char {
    // SAFETY: SAFETY: `conn_ptr` points to a valid Arc<Connection> wrapper.
    let conn_wrapper = unsafe { &*conn_ptr };
    // SAFETY: SAFETY: `query` is a valid C string from the caller.
    let c_str = unsafe { CStr::from_ptr(query) };
    let query_str = c_str.to_str().unwrap();

    // Use the persistent session context!
    let conn_obj = &conn_wrapper.conn;
    match conn_obj.query(query_str) {
        Ok(res) => {
            let res_json = serde_json::to_string(&res).unwrap();
            let c_res = CString::new(res_json).unwrap();
            c_res.into_raw()
        }
        Err(e) => {
            let err_json = format!("{{\"error\": \"{e}\"}}");
            let c_res = CString::new(err_json).unwrap();
            c_res.into_raw()
        }
    }
}

#[no_mangle]
pub extern "C" fn lightning_close(conn: *mut LightningConnection) {
    if !conn.is_null() {
        // SAFETY: SAFETY: Result string is Rust-allocated; CString::new is infallible for valid UTF-8.
        unsafe {
            let _ = Box::from_raw(conn);
        }
    }
}

#[no_mangle]
pub extern "C" fn lightning_free_string(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: SAFETY: Same pattern — CString from Rust-allocated data.
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}
