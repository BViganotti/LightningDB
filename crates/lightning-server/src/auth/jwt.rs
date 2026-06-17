use base64::Engine;
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use sha2::Sha256;

use crate::auth::models::{AuthMethod, AuthenticatedUser, JwtClaims, Role};

type HmacSha256 = Hmac<Sha256>;

pub fn create_access_token(
    user_id: &str,
    role: &Role,
    secret: &[u8],
    ttl_secs: u64,
) -> Result<String, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("time error: {e}"))?
        .as_secs() as usize;

    let claims = JwtClaims {
        sub: user_id.to_string(),
        role: *role,
        exp: now + ttl_secs as usize,
        iat: now,
        jti: uuid::Uuid::new_v4().to_string(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .map_err(|e| format!("JWT encode failed: {e}"))
}

pub fn validate_access_token(token: &str, secret: &[u8]) -> Result<JwtClaims, String> {
    // Per-deployment aud/iss validation should be added here when
    // LIGHTNING_JWT_AUDIENCE / LIGHTNING_JWT_ISSUER configs are introduced.
    // Until then, the token's exp, iat, and signature are still validated.
    let mut validation = Validation::default();
    validation.leeway = 0;
    let token_data = decode::<JwtClaims>(
        token,
        &DecodingKey::from_secret(secret),
        &validation,
    )
    .map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => "token expired".to_string(),
        jsonwebtoken::errors::ErrorKind::InvalidToken => "invalid token".to_string(),
        _ => format!("token validation failed: {e}"),
    })?;

    Ok(token_data.claims)
}

pub fn hmac_hash(input: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC key");
    mac.update(input.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn create_refresh_token(secret: &[u8]) -> (String, String) {
    let mut rng = rand::rngs::OsRng;
    let mut bytes = [0u8; 48];
    rng.fill_bytes(&mut bytes);
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let token = engine.encode(bytes);
    let hash = hmac_hash(&token, secret);
    (token, hash)
}

pub fn generate_api_key(secret: &[u8]) -> (String, String) {
    let mut rng = rand::rngs::OsRng;
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    let raw = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes);
    let key = format!("ld_{raw}");
    let hash = hmac_hash(&key, secret);
    (key, hash)
}

pub fn api_key_hash(key: &str, secret: &[u8]) -> String {
    hmac_hash(key, secret)
}

#[allow(dead_code)]
pub fn create_authenticated_user_from_jwt(
    claims: &JwtClaims,
    username: &str,
) -> AuthenticatedUser {
    AuthenticatedUser {
        user_id: claims.sub.clone(),
        username: username.to_string(),
        role: claims.role,
        auth_method: AuthMethod::Jwt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_roundtrip() {
        let secret = b"test-secret-key-that-is-at-least-32-bytes-long!";
        let token = create_access_token("user123", &Role::Admin, secret, 3600).unwrap();
        let claims = validate_access_token(&token, secret).unwrap();
        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.role, Role::Admin);
    }

    #[test]
    fn test_expired_token() {
        use jsonwebtoken::{encode as jwt_encode, EncodingKey, Header};
        let secret = b"test-secret-key-that-is-at-least-32-bytes-long!";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let claims = JwtClaims {
            sub: "user123".to_string(),
            role: Role::Reader,
            exp: now - 3600,
            iat: now - 7200,
            jti: uuid::Uuid::new_v4().to_string(),
        };
        let token = jwt_encode(&Header::default(), &claims, &EncodingKey::from_secret(secret)).unwrap();
        let result = validate_access_token(&token, secret);
        assert!(result.is_err());
    }

    #[test]
    fn test_refresh_token_creation() {
        let secret = b"test-secret-key-that-is-at-least-32-bytes-long!";
        let (token, hash) = create_refresh_token(secret);
        assert!(!token.is_empty());
        assert_eq!(hash, hmac_hash(&token, secret));
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_api_key_generation() {
        let secret = b"test-secret-key-that-is-at-least-32-bytes-long!";
        let (key, hash) = generate_api_key(secret);
        assert!(key.starts_with("ld_"));
        assert_eq!(hash, api_key_hash(&key, secret));
    }

    #[test]
    fn test_hmac_determinism() {
        let secret = b"test-secret-key-that-is-at-least-32-bytes-long!";
        let input = "hello-world";
        assert_eq!(hmac_hash(input, secret), hmac_hash(input, secret));
    }

    #[test]
    fn test_hmac_differs_with_secret() {
        let input = "hello-world";
        let h1 = hmac_hash(input, b"secret-one-that-is-at-least-32-bytes!");
        let h2 = hmac_hash(input, b"secret-two-that-is-at-least-32-bytes!");
        assert_ne!(h1, h2);
    }
}
