use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::RngCore;
use zeroize::Zeroize;

use crate::auth::jwt;
use crate::auth::models::{
    ApiKey, AuthenticatedUser, AuthMethod, Role, StoredData, StoredRefreshToken, User,
};
use crate::auth::password;

const MAX_LOGIN_ATTEMPTS: u32 = 5;
const LOGIN_WINDOW_SECS: i64 = 900;
const LOCKOUT_DURATION_SECS: i64 = 900;

pub struct AuthStore {
    data: Arc<RwLock<StoredData>>,
    path: PathBuf,
    jwt_secret: Vec<u8>,
    access_token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
    login_attempts: parking_lot::Mutex<HashMap<String, LoginRecord>>,
}

struct LoginRecord {
    timestamps: Vec<i64>,
    locked_until: Option<i64>,
}

impl AuthStore {
    pub fn new(
        path: PathBuf,
        jwt_secret: Vec<u8>,
        access_token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
    ) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create auth data directory: {e}"))?;
        }

        let data = if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("failed to read auth data file: {e}"))?;
            serde_json::from_str(&content)
                .map_err(|e| format!("failed to parse auth data file: {e}"))?
        } else {
            StoredData::default()
        };

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            path,
            jwt_secret,
            access_token_ttl_secs,
            refresh_token_ttl_secs,
            login_attempts: parking_lot::Mutex::new(HashMap::new()),
        })
    }

    pub fn jwt_secret(&self) -> &[u8] {
        &self.jwt_secret
    }

    pub fn access_token_ttl_secs(&self) -> u64 {
        self.access_token_ttl_secs
    }

    pub fn refresh_token_ttl_secs(&self) -> u64 {
        self.refresh_token_ttl_secs
    }

    fn persist(&self) -> Result<(), String> {
        let data = self.data.read();
        let content = serde_json::to_string_pretty(&*data)
            .map_err(|e| format!("failed to serialize auth data: {e}"))?;
        let tmp_path = self.path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &content)
            .map_err(|e| format!("failed to write auth data: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o600);
            let _ = std::fs::set_permissions(&tmp_path, perm);
        }
        std::fs::rename(&tmp_path, &self.path)
            .map_err(|e| format!("failed to rename auth data file: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o600);
            let _ = std::fs::set_permissions(&self.path, perm);
        }
        Ok(())
    }

    pub fn try_login(&self, username: &str, password: &str) -> Result<User, String> {
        let user = {
            let mut attempts = self.login_attempts.lock();
            let now = now_secs();

            let record = attempts.entry(username.to_string()).or_insert(LoginRecord {
                timestamps: Vec::new(),
                locked_until: None,
            });

            if let Some(locked_until) = record.locked_until {
                if now < locked_until {
                    return Err("account temporarily locked due to too many failed attempts".to_string());
                }
                *record = LoginRecord { timestamps: Vec::new(), locked_until: None };
            }

            record.timestamps.retain(|t| *t > now - LOGIN_WINDOW_SECS);
            record.timestamps.push(now);

            if record.timestamps.len() as u32 >= MAX_LOGIN_ATTEMPTS {
                record.locked_until = Some(now + LOCKOUT_DURATION_SECS);
                record.timestamps.clear();
                return Err("account temporarily locked due to too many failed attempts".to_string());
            }

            let data = self.data.read();
            let user = data.users.iter()
                .find(|u| u.username == username && u.enabled)
                .ok_or_else(|| "invalid username or password".to_string())?;
            if !password::verify_password(password, &user.password_hash)
                .map_err(|_| "invalid username or password".to_string())?
            {
                return Err("invalid username or password".to_string());
            }
            user.clone()
        };

        self.login_attempts.lock().remove(username);
        Ok(user)
    }

    pub fn bootstrap_admin(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<User, String> {
        let mut data = self.data.write();
        if data.users.iter().any(|u| u.username == username) {
            return Err(format!("user '{username}' already exists"));
        }
        if data.users.iter().any(|u| u.role == Role::Admin) {
            return Err("an admin user already exists, cannot bootstrap another".to_string());
        }

        let password_hash = password::hash_password(password)?;
        let user = User {
            id: generate_id("u"),
            username: username.to_string(),
            password_hash,
            role,
            enabled: true,
            created_at: now_secs(),
            last_login: None,
        };
        data.users.push(user.clone());
        drop(data);
        self.persist()?;
        Ok(user)
    }

    pub fn record_login(&self, user_id: &str) -> Result<(), String> {
        let mut data = self.data.write();
        if let Some(user) = data.users.iter_mut().find(|u| u.id == user_id) {
            user.last_login = Some(now_secs());
        }
        drop(data);
        self.persist()
    }

    pub fn get_user_by_id(&self, user_id: &str) -> Option<User> {
        let data = self.data.read();
        data.users.iter().find(|u| u.id == user_id).cloned()
    }

    #[allow(dead_code)]
    pub fn get_user_by_username(&self, username: &str) -> Option<User> {
        let data = self.data.read();
        data.users.iter().find(|u| u.username == username).cloned()
    }

    pub fn list_users(&self) -> Vec<User> {
        let data = self.data.read();
        let mut users = data.users.clone();
        users.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        users
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<User, String> {
        let mut data = self.data.write();
        if data.users.iter().any(|u| u.username == username) {
            return Err(format!("user '{username}' already exists"));
        }
        let password_hash = password::hash_password(password)?;
        let user = User {
            id: generate_id("u"),
            username: username.to_string(),
            password_hash,
            role,
            enabled: true,
            created_at: now_secs(),
            last_login: None,
        };
        data.users.push(user.clone());
        drop(data);
        self.persist()?;
        Ok(user)
    }

    pub fn update_user(
        &self,
        user_id: &str,
        role: Option<Role>,
        enabled: Option<bool>,
    ) -> Result<User, String> {
        let mut data = self.data.write();
        let user = data
            .users
            .iter_mut()
            .find(|u| u.id == user_id)
            .ok_or_else(|| format!("user '{user_id}' not found"))?;
        if let Some(role) = role {
            user.role = role;
        }
        if let Some(enabled) = enabled {
            user.enabled = enabled;
        }
        let result = user.clone();
        drop(data);
        self.persist()?;
        Ok(result)
    }

    pub fn delete_user(&self, user_id: &str) -> Result<(), String> {
        let mut data = self.data.write();
        let len_before = data.users.len();
        data.users.retain(|u| u.id != user_id);
        data.refresh_tokens.retain(|t| t.user_id != user_id);
        data.api_keys.retain(|k| k.user_id != user_id);
        if data.users.len() == len_before {
            return Err(format!("user '{user_id}' not found"));
        }
        drop(data);
        self.persist()
    }

    pub fn reset_password(&self, user_id: &str, new_password: &str) -> Result<(), String> {
        let password_hash = password::hash_password(new_password)?;
        let mut data = self.data.write();
        let user = data
            .users
            .iter_mut()
            .find(|u| u.id == user_id)
            .ok_or_else(|| format!("user '{user_id}' not found"))?;
        user.password_hash = password_hash;
        for token in data.refresh_tokens.iter_mut().filter(|t| t.user_id == user_id) {
            token.revoked = true;
        }
        drop(data);
        self.persist()
    }

    pub fn store_refresh_token(&self, user_id: &str, token_hash: &str, ttl_secs: u64) -> Result<String, String> {
        let mut data = self.data.write();
        let now = now_secs();
        data.refresh_tokens
            .retain(|t| t.expires_at > now || t.user_id != user_id);
        let id = generate_id("rt");
        let stored = StoredRefreshToken {
            id: id.clone(),
            user_id: user_id.to_string(),
            token_hash: token_hash.to_string(),
            expires_at: now + ttl_secs as i64,
            revoked: false,
            created_at: now,
        };
        data.refresh_tokens.push(stored);
        drop(data);
        self.persist()?;
        Ok(id)
    }

    pub fn validate_refresh_token(
        &self,
        raw_token: &str,
    ) -> Result<(User, String), String> {
        let token_hash = jwt::hmac_hash(raw_token, &self.jwt_secret);
        let data = self.data.read();
        let now = now_secs();
        let stored = data
            .refresh_tokens
            .iter()
            .find(|t| t.token_hash == token_hash && !t.revoked && t.expires_at > now)
            .ok_or_else(|| "invalid or expired refresh token".to_string())?;
        let user = data
            .users
            .iter()
            .find(|u| u.id == stored.user_id && u.enabled)
            .ok_or_else(|| "user not found or disabled".to_string())?;
        Ok((user.clone(), stored.id.clone()))
    }

    pub fn revoke_refresh_token(&self, raw_token: &str) -> Result<(), String> {
        let token_hash = jwt::hmac_hash(raw_token, &self.jwt_secret);
        let mut data = self.data.write();
        let stored = data
            .refresh_tokens
            .iter_mut()
            .find(|t| t.token_hash == token_hash)
            .ok_or_else(|| "refresh token not found".to_string())?;
        stored.revoked = true;
        drop(data);
        self.persist()
    }

    pub fn create_api_key(
        &self,
        user_id: &str,
        name: &str,
        role_override: Option<Role>,
        expires_at: Option<i64>,
    ) -> Result<(String, ApiKey), String> {
        let mut data = self.data.write();
        if !data.users.iter().any(|u| u.id == user_id && u.enabled) {
            return Err("user not found or disabled".to_string());
        }
        let (key, key_hash) = jwt::generate_api_key(&self.jwt_secret);
        let prefix = key.chars().take(12).collect::<String>();
        let api_key = ApiKey {
            id: generate_id("ak"),
            user_id: user_id.to_string(),
            key_hash,
            name: name.to_string(),
            prefix,
            role_override,
            expires_at,
            created_at: now_secs(),
            revoked: false,
        };
        data.api_keys.push(api_key.clone());
        drop(data);
        self.persist()?;
        Ok((key, api_key))
    }

    pub fn list_api_keys(&self, user_id: &str) -> Vec<ApiKey> {
        let data = self.data.read();
        data.api_keys
            .iter()
            .filter(|k| k.user_id == user_id)
            .cloned()
            .collect()
    }

    pub fn revoke_api_key(&self, key_id: &str) -> Result<(), String> {
        let mut data = self.data.write();
        let key = data
            .api_keys
            .iter_mut()
            .find(|k| k.id == key_id)
            .ok_or_else(|| format!("API key '{key_id}' not found"))?;
        key.revoked = true;
        drop(data);
        self.persist()
    }

    pub fn authenticate_api_key(&self, key: &str) -> Result<AuthenticatedUser, String> {
        let key_hash = jwt::api_key_hash(key, &self.jwt_secret);
        let data = self.data.read();
        let stored = data
            .api_keys
            .iter()
            .find(|k| k.key_hash == key_hash && !k.revoked)
            .ok_or_else(|| "invalid API key".to_string())?;
        if let Some(expires) = stored.expires_at {
            if now_secs() > expires {
                return Err("API key expired".to_string());
            }
        }
        let user = data
            .users
            .iter()
            .find(|u| u.id == stored.user_id && u.enabled)
            .ok_or_else(|| "user not found or disabled".to_string())?;
        let role = stored.role_override.unwrap_or(user.role);
        Ok(AuthenticatedUser {
            user_id: user.id.clone(),
            username: user.username.clone(),
            role,
            auth_method: AuthMethod::ApiKey,
        })
    }

    #[allow(dead_code)]
    pub fn cleanup_expired_refresh_tokens(&self) -> Result<(), String> {
        let mut data = self.data.write();
        let now = now_secs();
        let before = data.refresh_tokens.len();
        data.refresh_tokens.retain(|t| t.expires_at > now);
        if data.refresh_tokens.len() < before {
            drop(data);
            self.persist()
        } else {
            Ok(())
        }
    }
}

impl Drop for AuthStore {
    fn drop(&mut self) {
        self.jwt_secret.zeroize();
    }
}

fn generate_id(prefix: &str) -> String {
    let mut rng = rand::rngs::OsRng;
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    use base64::Engine;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let encoded = engine.encode(bytes);
    let suffix: String = encoded.chars().take(16).collect();
    format!("{prefix}_{suffix}")
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
