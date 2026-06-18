#![allow(clippy::not_unsafe_ptr_arg_deref)]
use crate::{Connection, Database, QueryResult, SyncMode, SystemConfig};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;

#[repr(C)]
pub struct kuzu_database {
    pub database: *mut Arc<Database>,
}

#[repr(C)]
pub struct kuzu_connection {
    pub connection: *mut Connection,
}

#[repr(C)]
pub struct kuzu_query_result {
    pub query_result: *mut QueryResult,
}

#[repr(C)]
pub struct kuzu_system_config {
    pub buffer_pool_size: u64,
    pub max_num_threads: u64,
    pub read_only: bool,
}

#[no_mangle]
pub extern "C" fn lightning_database_init(
    path: *const c_char,
    config: kuzu_system_config,
) -> *mut kuzu_database {
    if path.is_null() {
        return std::ptr::null_mut();
    }
    let path_str = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };
    let sys_config = SystemConfig {
        buffer_pool_size: config.buffer_pool_size,
        max_num_threads: config.max_num_threads as u32,
        read_only: config.read_only,
        sync_mode: SyncMode::Normal,
        vacuum_interval_ms: 1000,
        prefetch_enabled: true,
        prefetch_depth: 2,
        prefetch_confidence: 0.15,
        slow_query_threshold_ms: 100,
        copy_base_dir: None,
        wasm_base_dir: None,
        embedding_dim: 384,
    };

    match Database::new(path_str, sys_config) {
        Ok(db) => {
            let db_ptr = Box::into_raw(Box::new(db));
            Box::into_raw(Box::new(kuzu_database { database: db_ptr }))
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn lightning_database_destroy(database: *mut kuzu_database) {
    if !database.is_null() {
        let db = unsafe { Box::from_raw(database) };
        let _ = unsafe { Box::from_raw(db.database) };
    }
}

#[no_mangle]
pub extern "C" fn lightning_connection_init(database: *mut kuzu_database) -> *mut kuzu_connection {
    if database.is_null() {
        return std::ptr::null_mut();
    }
    let db = unsafe { &*(*database).database };
    let conn = Box::new(Connection::new(Arc::clone(db)));
    Box::into_raw(Box::new(kuzu_connection {
        connection: Box::into_raw(conn),
    }))
}

#[no_mangle]
pub extern "C" fn lightning_connection_destroy(connection: *mut kuzu_connection) {
    if !connection.is_null() {
        let conn = unsafe { Box::from_raw(connection) };
        let _ = unsafe { Box::from_raw(conn.connection) };
    }
}

#[no_mangle]
pub extern "C" fn lightning_connection_query(
    connection: *mut kuzu_connection,
    query: *const c_char,
) -> *mut kuzu_query_result {
    if connection.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let conn = unsafe { &*(*connection).connection };
    let query_str = unsafe { CStr::from_ptr(query).to_string_lossy() };

    match conn.query(&query_str) {
        Ok(res) => Box::into_raw(Box::new(kuzu_query_result {
            query_result: Box::into_raw(Box::new(res)),
        })),
        Err(e) => {
            let error_res = QueryResult::new_error(e.to_string());
            Box::into_raw(Box::new(kuzu_query_result {
                query_result: Box::into_raw(Box::new(error_res)),
            }))
        }
    }
}

#[no_mangle]
pub extern "C" fn lightning_query_result_destroy(query_result: *mut kuzu_query_result) {
    if !query_result.is_null() {
        let res = unsafe { Box::from_raw(query_result) };
        let _ = unsafe { Box::from_raw(res.query_result) };
    }
}

#[no_mangle]
pub extern "C" fn lightning_query_result_is_success(query_result: *mut kuzu_query_result) -> bool {
    if query_result.is_null() {
        return false;
    }
    let res = unsafe { &*(*query_result).query_result };
    res.is_success()
}

#[no_mangle]
pub extern "C" fn lightning_query_result_get_error_message(
    query_result: *mut kuzu_query_result,
) -> *mut c_char {
    if query_result.is_null() {
        return std::ptr::null_mut();
    }
    let res = unsafe { &*(*query_result).query_result };
    if let Some(msg) = res.error_message() {
        CString::new(msg).unwrap_or_else(|_| CString::new("error").expect("infallible: 'error' has no null bytes")).into_raw()
    } else {
        std::ptr::null_mut()
    }
}

/// Free a string returned by a `lightning_*` function.
///
/// # Safety
/// `s` must be a pointer previously returned by a `lightning_*` function
/// that returns `*mut c_char` (e.g., `lightning_query_result_get_error_message`).
/// Passing any other pointer (stack-allocated, malloc'd, already freed, etc.)
/// is undefined behavior. After this call, `s` must not be used again.
#[no_mangle]
pub unsafe extern "C" fn lightning_destroy_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}


