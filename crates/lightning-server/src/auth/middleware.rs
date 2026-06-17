use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::extract::State;
use axum::http::{header, request::Parts, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum::middleware::Next;
use axum::extract::Request;

use crate::auth::jwt;
use crate::auth::models::{AuthMethod, AuthenticatedUser, AuthMode, Role};
use crate::auth::store::AuthStore;
use crate::models::response::ErrorResponse;
use crate::server::AppState;

const PUBLIC_PATHS: &[&str] = &[
    "/health",
    "/metrics",
    "/v1/auth/login",
];

#[derive(Debug, Clone)]
pub struct AuthenticatedUserExtractor(pub AuthenticatedUser);

impl<S> FromRequestParts<S> for AuthenticatedUserExtractor
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthenticatedUser>()
            .cloned()
            .map(AuthenticatedUserExtractor)
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse {
                        error: "Authentication required".to_string(),
                        code: Some("AUTH_REQUIRED".to_string()),
                        details: None,
                        request_id: None,
                    }),
                )
                    .into_response()
            })
    }
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, Response> {
    let path = req.uri().path();

    if PUBLIC_PATHS.iter().any(|p| path == *p || path.starts_with(&format!("{p}/"))) {
        return Ok(next.run(req).await);
    }

    match state.auth_mode {
        AuthMode::None => {
            let anonymous = AuthenticatedUser {
                user_id: String::new(),
                username: "anonymous".to_string(),
                role: Role::Reader,
                auth_method: AuthMethod::Token,
            };
            req.extensions_mut().insert(anonymous);
            Ok(next.run(req).await)
        }
        AuthMode::Token => {
            let expected = state
                .config
                .auth_token
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_default();

            let provided = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|s| s.trim().to_string());

            match provided {
                Some(token) if token == expected => {
                    let user = AuthenticatedUser {
                        user_id: String::new(),
                        username: "token-user".to_string(),
                        role: Role::Admin,
                        auth_method: AuthMethod::Token,
                    };
                    req.extensions_mut().insert(user);
                    Ok(next.run(req).await)
                }
                _ => Err(unauthorized_response("invalid auth token")),
            }
        }
        AuthMode::Jwt => {
            let token = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|s| s.trim().to_string());

            match token {
                Some(token) => {
                    let auth_result = authenticate_jwt(&token, &state.auth_store).await;
                    match auth_result {
                        Ok(user) => {
                            req.extensions_mut().insert(user);
                            Ok(next.run(req).await)
                        }
                        Err(_) => {
                            let api_key_result =
                                state.auth_store.authenticate_api_key(&token);
                            match api_key_result {
                                Ok(user) => {
                                    req.extensions_mut().insert(user);
                                    Ok(next.run(req).await)
                                }
                                Err(e) => Err(unauthorized_response(&e)),
                            }
                        }
                    }
                }
                None => Err(unauthorized_response("authorization header required (Bearer token)")),
            }
        }
    }
}

async fn authenticate_jwt(token: &str, store: &AuthStore) -> Result<AuthenticatedUser, String> {
    let claims = jwt::validate_access_token(token, store.jwt_secret())?;
    let user = store
        .get_user_by_id(&claims.sub)
        .ok_or_else(|| "user not found".to_string())?;
    if !user.enabled {
        return Err("user is disabled".to_string());
    }
    Ok(AuthenticatedUser {
        user_id: user.id,
        username: user.username,
        role: user.role,
        auth_method: AuthMethod::Jwt,
    })
}

fn unauthorized_response(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            error: message.to_string(),
            code: Some("UNAUTHORIZED".to_string()),
            details: None,
            request_id: None,
        }),
    )
        .into_response()
}

async fn check_role(req: Request, next: Next, required: Role) -> Result<Response, Response> {
    let user = req
        .extensions()
        .get::<AuthenticatedUser>()
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Authentication required".to_string(),
                    code: Some("AUTH_REQUIRED".to_string()),
                    details: None,
                    request_id: None,
                }),
            )
                .into_response()
        })?;

    if user.role.has_at_least(&required) {
        Ok(next.run(req).await)
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: format!(
                    "insufficient permissions: required at least '{required}', got '{}'",
                    user.role
                ),
                code: Some("FORBIDDEN".to_string()),
                details: None,
                request_id: None,
            }),
        )
            .into_response())
    }
}

pub async fn require_reader_role(req: Request, next: Next) -> Result<Response, Response> {
    check_role(req, next, Role::Reader).await
}

pub async fn require_writer_role(req: Request, next: Next) -> Result<Response, Response> {
    check_role(req, next, Role::Writer).await
}

pub async fn require_admin_role(req: Request, next: Next) -> Result<Response, Response> {
    check_role(req, next, Role::Admin).await
}
