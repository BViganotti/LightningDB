use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use crate::auth::middleware::AuthenticatedUserExtractor;
use crate::auth::models::Role;
use crate::error::AppError;
use crate::extract::RequestId;
use crate::models::request::{
    CreateApiKeyRequest, CreateUserRequest, ResetPasswordRequest, UpdateUserRequest,
};
use crate::models::response::{
    ApiKeyItem, ApiKeyListResponse, ApiResponse, CreateApiKeyResponse, CreateUserResponse,
    ResponseMeta, UserListItem, UserListResponse,
};
use crate::server::AppState;

fn into_user_list_item(user: &crate::auth::models::User) -> UserListItem {
    UserListItem {
        id: user.id.clone(),
        username: user.username.clone(),
        role: user.role.to_string(),
        enabled: user.enabled,
        created_at: user.created_at,
        last_login: user.last_login,
    }
}

fn into_api_key_item(key: &crate::auth::models::ApiKey) -> ApiKeyItem {
    ApiKeyItem {
        id: key.id.clone(),
        name: key.name.clone(),
        prefix: key.prefix.clone(),
        created_at: key.created_at,
        expires_at: key.expires_at,
        revoked: key.revoked,
    }
}

pub async fn list_users_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<UserListResponse>>, AppError> {
    let start = std::time::Instant::now();
    let users = state.auth_store.list_users();
    let items: Vec<UserListItem> = users.iter().map(into_user_list_item).collect();

    Ok(Json(ApiResponse {
        data: UserListResponse { users: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn create_user_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Json(req): Json<CreateUserRequest>,
) -> Result<Json<ApiResponse<CreateUserResponse>>, AppError> {
    let start = std::time::Instant::now();

    if req.username.is_empty() {
        return Err(AppError::BadRequest("username is required".to_string()));
    }
    if req.password.len() < 8 {
        return Err(AppError::BadRequest(
            "password must be at least 8 characters".to_string(),
        ));
    }

    let role = match req.role.as_deref() {
        Some(r) => r.parse::<Role>().map_err(|e| AppError::BadRequest(e))?,
        None => Role::Reader,
    };

    let user = state
        .auth_store
        .create_user(&req.username, &req.password, role)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        user_id = %user.id,
        username = %user.username,
        role = %user.role,
        "User created"
    );

    Ok(Json(ApiResponse {
        data: CreateUserResponse { id: user.id },
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn update_user_handler(
    auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path(user_id): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    if auth.0.user_id == user_id {
        if req.enabled == Some(false) {
            return Err(AppError::BadRequest("cannot disable yourself".to_string()));
        }
        if let Some(ref r) = req.role {
            let new_role = r.parse::<Role>().map_err(|e| AppError::BadRequest(e))?;
            if new_role < Role::Admin {
                return Err(AppError::BadRequest("cannot demote yourself below admin".to_string()));
            }
        }
    }

    let role = match req.role.as_deref() {
        Some(r) => Some(r.parse::<Role>().map_err(|e| AppError::BadRequest(e))?),
        None => None,
    };

    state
        .auth_store
        .update_user(&user_id, role, req.enabled)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        user_id = %user_id,
        "User updated"
    );

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn delete_user_handler(
    auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    if auth.0.user_id == user_id {
        return Err(AppError::BadRequest("cannot delete yourself".to_string()));
    }

    state
        .auth_store
        .delete_user(&user_id)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        user_id = %user_id,
        "User deleted"
    );

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn reset_password_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path(user_id): Path<String>,
    Json(req): Json<ResetPasswordRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    if req.password.len() < 8 {
        return Err(AppError::BadRequest(
            "password must be at least 8 characters".to_string(),
        ));
    }

    state
        .auth_store
        .reset_password(&user_id, &req.password)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        user_id = %user_id,
        "Password reset"
    );

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn list_api_keys_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<ApiKeyListResponse>>, AppError> {
    let start = std::time::Instant::now();

    let keys = state.auth_store.list_api_keys(&user_id);
    let items: Vec<ApiKeyItem> = keys.iter().map(into_api_key_item).collect();

    Ok(Json(ApiResponse {
        data: ApiKeyListResponse { keys: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn create_api_key_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path(user_id): Path<String>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<Json<ApiResponse<CreateApiKeyResponse>>, AppError> {
    let start = std::time::Instant::now();

    if req.name.is_empty() {
        return Err(AppError::BadRequest("name is required".to_string()));
    }

    let role_override = match req.role_override.as_deref() {
        Some(r) => Some(r.parse::<Role>().map_err(|e| AppError::BadRequest(e))?),
        None => None,
    };

    let (key, api_key) = state
        .auth_store
        .create_api_key(&user_id, &req.name, role_override, req.expires_at)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        user_id = %user_id,
        key_id = %api_key.id,
        "API key created"
    );

    Ok(Json(ApiResponse {
        data: CreateApiKeyResponse {
            id: api_key.id,
            key,
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}

pub async fn delete_api_key_handler(
    _auth: AuthenticatedUserExtractor,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Path((_user_id, key_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    state
        .auth_store
        .revoke_api_key(&key_id)
        .map_err(|e| AppError::BadRequest(e))?;

    tracing::info!(
        request_id = %request_id,
        key_id = %key_id,
        "API key revoked"
    );

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: start.elapsed().as_millis() as u64,
        },
    }))
}
