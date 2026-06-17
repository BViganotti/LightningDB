use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::Json;
use std::net::SocketAddr;

use crate::auth::jwt;
use crate::auth::models::AuthMode;
use crate::error::AppError;
use crate::extract::RequestId;
use crate::models::request::{LoginRequest, RefreshTokenRequest, LogoutRequest};
use crate::models::response::{ApiResponse, LoginResponse, MeResponse, ResponseMeta};
use crate::server::AppState;

pub async fn login_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    RequestId(request_id): RequestId,
    Json(req): Json<LoginRequest>,
) -> Result<Json<ApiResponse<LoginResponse>>, AppError> {
    if state.config.auth_mode != AuthMode::Jwt {
        return Err(AppError::Unauthorized(
            "authentication is not available in current mode".to_string(),
        ));
    }

    if req.username.is_empty() || req.password.is_empty() {
        return Err(AppError::BadRequest("username and password are required".to_string()));
    }

    let user = state.auth_store.try_login(&req.username, &req.password, remote_addr.ip()).map_err(|e| {
        if e.contains("locked") {
            AppError::TooManyRequests(e)
        } else {
            AppError::Unauthorized(e)
        }
    })?;

    let access_token = jwt::create_access_token(
        &user.id,
        &user.role,
        state.auth_store.jwt_secret(),
        state.auth_store.access_token_ttl_secs(),
    )
    .map_err(|e| AppError::Internal(e))?;

    let (refresh_token, refresh_hash) = jwt::create_refresh_token(state.auth_store.jwt_secret());
    let _ = state.auth_store.store_refresh_token(
        &user.id,
        &refresh_hash,
        state.auth_store.refresh_token_ttl_secs(),
    );

    let _ = state.auth_store.record_login(&user.id);

    tracing::info!(
        request_id = %request_id,
        user_id = %user.id,
        username = %user.username,
        "User logged in"
    );

    Ok(Json(ApiResponse {
        data: LoginResponse {
            access_token,
            refresh_token,
            expires_in: state.auth_store.access_token_ttl_secs(),
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: 0,
        },
    }))
}

pub async fn refresh_handler(
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Json(req): Json<RefreshTokenRequest>,
) -> Result<Json<ApiResponse<LoginResponse>>, AppError> {
    if state.config.auth_mode != AuthMode::Jwt {
        return Err(AppError::Unauthorized(
            "authentication is not available in current mode".to_string(),
        ));
    }

    let (user, token_id) = state
        .auth_store
        .validate_refresh_token(&req.refresh_token)
        .map_err(|_| AppError::Unauthorized("invalid or expired refresh token".to_string()))?;

    state
        .auth_store
        .revoke_refresh_token(&req.refresh_token)
        .ok();

    let access_token = jwt::create_access_token(
        &user.id,
        &user.role,
        state.auth_store.jwt_secret(),
        state.auth_store.access_token_ttl_secs(),
    )
    .map_err(|e| AppError::Internal(e))?;

    let (new_refresh_token, new_hash) = jwt::create_refresh_token(state.auth_store.jwt_secret());
    let _ = state.auth_store.store_refresh_token(
        &user.id,
        &new_hash,
        state.auth_store.refresh_token_ttl_secs(),
    );

    tracing::info!(
        request_id = %request_id,
        user_id = %user.id,
        token_id = %token_id,
        "Refresh token rotated"
    );

    Ok(Json(ApiResponse {
        data: LoginResponse {
            access_token,
            refresh_token: new_refresh_token,
            expires_in: state.auth_store.access_token_ttl_secs(),
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: 0,
        },
    }))
}

pub async fn logout_handler(
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Json(req): Json<LogoutRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    if state.config.auth_mode != AuthMode::Jwt {
        return Err(AppError::Unauthorized(
            "authentication is not available in current mode".to_string(),
        ));
    }

    let _ = state.auth_store.revoke_refresh_token(&req.refresh_token);

    tracing::info!(request_id = %request_id, "User logged out");

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: 0,
        },
    }))
}

pub async fn me_handler(
    auth: crate::auth::middleware::AuthenticatedUserExtractor,
    State(_state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<MeResponse>>, AppError> {
    let user = auth.0;

    Ok(Json(ApiResponse {
        data: MeResponse {
            user_id: user.user_id,
            username: user.username,
            role: user.role.to_string(),
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: 0,
        },
    }))
}
