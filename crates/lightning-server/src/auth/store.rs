use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use lightning::Database;
use lightning::Connection;
use lightning_core::Value;
use parking_lot::RwLock;
use rand::RngCore;
use zeroize::Zeroize;

use crate::auth::cache::{CacheResult, TokenCache};
use crate::auth::jwt;
use crate::auth::models::{
    ApiKey, AuthenticatedUser, AuthMethod, Role, StoredRefreshToken, User,
};
use crate::auth::password;

const AUTH_USERS_TABLE: &str = "auth_users";
const AUTH_TOKENS_TABLE: &str = "auth_refresh_tokens";
const AUTH_API_KEYS_TABLE: &str = "auth_api_keys";
static DUMMY_PASSWORD_HASH: OnceLock<String> = OnceLock::new();

const MAX_LOGIN_ATTEMPTS: u32 = 5;
const LOGIN_WINDOW_SECS: i64 = 900;
const LOCKOUT_DURATION_SECS: i64 = 900;

pub struct AuthStore {
    #[allow(dead_code)]
    db: Arc<Database>,
    conn: Connection,
    users_cache: RwLock<HashMap<String, User>>,
    usernames_cache: RwLock<HashMap<String, String>>,
    tokens_cache: RwLock<HashMap<String, StoredRefreshToken>>,
    api_keys_cache: RwLock<HashMap<String, ApiKey>>,
    token_bloom: TokenCache,
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
        db: Arc<Database>,
        jwt_secret: Vec<u8>,
        access_token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
    ) -> Result<Self, String> {
        let conn = db.connect();
        Self::ensure_system_tables(&conn)?;

        let users = Self::load_users(&conn)?;
        let tokens = Self::load_tokens(&conn)?;
        let api_keys = Self::load_api_keys(&conn)?;

        let mut users_cache = HashMap::new();
        let mut usernames_cache = HashMap::new();
        for u in &users {
            users_cache.insert(u.id.clone(), u.clone());
            usernames_cache.insert(u.username.clone(), u.id.clone());
        }

        let mut tokens_cache = HashMap::new();
        for t in &tokens {
            tokens_cache.insert(t.token_hash.clone(), t.clone());
        }

        let mut api_keys_cache = HashMap::new();
        for k in &api_keys {
            api_keys_cache.insert(k.key_hash.clone(), k.clone());
        }

        let token_bloom = TokenCache::new();
        let all_hashes: Vec<String> = tokens.iter().map(|t| t.token_hash.clone()).collect();
        token_bloom.rebuild(&all_hashes, &[]);

        Ok(Self {
            db,
            conn,
            users_cache: RwLock::new(users_cache),
            usernames_cache: RwLock::new(usernames_cache),
            tokens_cache: RwLock::new(tokens_cache),
            api_keys_cache: RwLock::new(api_keys_cache),
            token_bloom,
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

    fn ensure_system_tables(conn: &Connection) -> Result<(), String> {
        let db = conn.client_context().database.clone();
        let storage = db.storage_manager.read();
        let users_exist = storage.node_tables.contains_key(AUTH_USERS_TABLE);
        let tokens_exist = storage.node_tables.contains_key(AUTH_TOKENS_TABLE);
        let keys_exist = storage.node_tables.contains_key(AUTH_API_KEYS_TABLE);
        drop(storage);

        if !users_exist {
            let q = format!(
                "CREATE NODE TABLE {AUTH_USERS_TABLE} (id STRING, username STRING, \
                 password_hash STRING, role STRING, enabled BOOL, \
                 created_at INT64, last_login INT64, PRIMARY KEY (id))"
            );
            conn.execute(&q, None)
                .map_err(|e| format!("failed to create auth users table: {e}"))?;
        }
        if !tokens_exist {
            let q = format!(
                "CREATE NODE TABLE {AUTH_TOKENS_TABLE} (id STRING, user_id STRING, \
                 token_hash STRING, expires_at INT64, created_at INT64, PRIMARY KEY (id))"
            );
            conn.execute(&q, None)
                .map_err(|e| format!("failed to create auth tokens table: {e}"))?;
        }
        if !keys_exist {
            let q = format!(
                "CREATE NODE TABLE {AUTH_API_KEYS_TABLE} (id STRING, user_id STRING, \
                 key_hash STRING, name STRING, prefix STRING, role_override STRING, \
                 expires_at INT64, created_at INT64, PRIMARY KEY (id))"
            );
            conn.execute(&q, None)
                .map_err(|e| format!("failed to create auth api keys table: {e}"))?;
        }
        Ok(())
    }

    fn load_users(conn: &Connection) -> Result<Vec<User>, String> {
        let q = format!(
            "MATCH (u:{AUTH_USERS_TABLE}) \
             RETURN u.id, u.username, u.password_hash, u.role, \
             u.enabled, u.created_at, u.last_login"
        );
        let result = conn
            .execute(&q, None)
            .map_err(|e| format!("failed to load users: {e}"))?;
        let mut users = Vec::new();
        for batch in &result.batches {
            let ids = as_string_array(batch.column(0))?;
            let usernames = as_string_array(batch.column(1))?;
            let passwords = as_string_array(batch.column(2))?;
            let roles = as_string_array(batch.column(3))?;
            let enabled = as_bool_array(batch.column(4))?;
            let created = as_int_array(batch.column(5))?;
            let last_login = as_int_array(batch.column(6))?;
            for i in 0..batch.num_rows() {
                let ll = last_login.value(i);
                users.push(User {
                    id: ids.value(i).to_string(),
                    username: usernames.value(i).to_string(),
                    password_hash: passwords.value(i).to_string(),
                    role: roles
                        .value(i)
                        .parse::<Role>()
                        .map_err(|e| format!("invalid role in auth store: {e}"))?,
                    enabled: enabled.value(i),
                    created_at: created.value(i),
                    last_login: if ll == 0 { None } else { Some(ll) },
                });
            }
        }
        Ok(users)
    }

    fn load_tokens(conn: &Connection) -> Result<Vec<StoredRefreshToken>, String> {
        let q = format!(
            "MATCH (t:{AUTH_TOKENS_TABLE}) \
             RETURN t.id, t.user_id, t.token_hash, t.expires_at, t.created_at"
        );
        let result = conn
            .execute(&q, None)
            .map_err(|e| format!("failed to load tokens: {e}"))?;
        let mut tokens = Vec::new();
        let now = now_secs();
        for batch in &result.batches {
            let ids = as_string_array(batch.column(0))?;
            let user_ids = as_string_array(batch.column(1))?;
            let hashes = as_string_array(batch.column(2))?;
            let expires = as_int_array(batch.column(3))?;
            let created = as_int_array(batch.column(4))?;
            for i in 0..batch.num_rows() {
                if expires.value(i) <= now {
                    continue;
                }
                tokens.push(StoredRefreshToken {
                    id: ids.value(i).to_string(),
                    user_id: user_ids.value(i).to_string(),
                    token_hash: hashes.value(i).to_string(),
                    expires_at: expires.value(i),
                    revoked: false,
                    created_at: created.value(i),
                });
            }
        }
        Ok(tokens)
    }

    fn load_api_keys(conn: &Connection) -> Result<Vec<ApiKey>, String> {
        let q = format!(
            "MATCH (k:{AUTH_API_KEYS_TABLE}) \
             RETURN k.id, k.user_id, k.key_hash, k.name, k.prefix, \
             k.role_override, k.expires_at, k.created_at"
        );
        let result = conn
            .execute(&q, None)
            .map_err(|e| format!("failed to load api keys: {e}"))?;
        let mut keys = Vec::new();
        let now = now_secs();
        for batch in &result.batches {
            let ids = as_string_array(batch.column(0))?;
            let user_ids = as_string_array(batch.column(1))?;
            let hashes = as_string_array(batch.column(2))?;
            let names = as_string_array(batch.column(3))?;
            let prefixes = as_string_array(batch.column(4))?;
            let overrides = as_string_array(batch.column(5))?;
            let expires = as_int_array(batch.column(6))?;
            let created = as_int_array(batch.column(7))?;
            for i in 0..batch.num_rows() {
                let exp = expires.value(i);
                if exp != 0 && exp <= now {
                    continue;
                }
                keys.push(ApiKey {
                    id: ids.value(i).to_string(),
                    user_id: user_ids.value(i).to_string(),
                    key_hash: hashes.value(i).to_string(),
                    name: names.value(i).to_string(),
                    prefix: prefixes.value(i).to_string(),
                    role_override: {
                        let r = overrides.value(i);
                        if r.is_empty() {
                            None
                        } else {
                            Some(r.parse::<Role>().map_err(|e| format!("invalid role in api key: {e}"))?)
                        }
                    },
                    expires_at: if exp == 0 { None } else { Some(exp) },
                    created_at: created.value(i),
                    revoked: false,
                });
            }
        }
        Ok(keys)
    }

    // ------------------------------------------------------------------
    //  Login / Lockout
    // ------------------------------------------------------------------

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
                    return Err(
                        "account temporarily locked due to too many failed attempts".to_string(),
                    );
                }
                *record = LoginRecord {
                    timestamps: Vec::new(),
                    locked_until: None,
                };
            }

            record.timestamps.retain(|t| *t > now - LOGIN_WINDOW_SECS);
            record.timestamps.push(now);

            if record.timestamps.len() as u32 >= MAX_LOGIN_ATTEMPTS {
                record.locked_until = Some(now + LOCKOUT_DURATION_SECS);
                record.timestamps.clear();
                return Err(
                    "account temporarily locked due to too many failed attempts".to_string(),
                );
            }

            let users = self.users_cache.read();
            let (user, stored_hash) = match users
                .values()
                .find(|u| u.username == username && u.enabled)
            {
                Some(u) => (Some(u.clone()), u.password_hash.clone()),
                // Always run Argon2 even for unknown usernames to prevent
                // timing side-channel that leaks which usernames exist.
                None => {
                    let dummy_hash = DUMMY_PASSWORD_HASH.get_or_init(|| {
                        password::hash_password("dummy_timing_marker")
                            .unwrap_or_else(|_| "$argon2id$v=19$m=65536,t=3,p=4$c2FsdHlzYWx0eXNhbHR5c2FsdA$3m8P7YN5GpIgJxmQhSn3qJnNBVpqHsp+Lq/hbp7Epl4".to_string())
                    });
                    (None, dummy_hash.clone())
                },
            };
            if !password::verify_password(password, &stored_hash)
                .map_err(|_| "invalid username or password".to_string())?
            {
                return Err("invalid username or password".to_string());
            }
            user.ok_or_else(|| "invalid username or password".to_string())?
        };

        self.login_attempts.lock().remove(username);
        Ok(user)
    }

    pub fn record_login(&self, user_id: &str) -> Result<(), String> {
        let now = now_secs();
        {
            let mut users = self.users_cache.write();
            if let Some(user) = users.get_mut(user_id) {
                user.last_login = Some(now);
            } else {
                return Ok(());
            }
        }
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(user_id.to_string()));
        params.insert("last_login".to_string(), Value::Number(now as f64));
        let q = format!(
            "MATCH (u:{AUTH_USERS_TABLE} {{id: $id}}) SET u.last_login = $last_login"
        );
        self.conn
            .execute(&q, Some(params))
            .map_err(|e| format!("failed to record login: {e}"))?;
        Ok(())
    }

    // ------------------------------------------------------------------
    //  User CRUD
    // ------------------------------------------------------------------

    pub fn bootstrap_admin(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<User, String> {
        {
            let users = self.users_cache.read();
            if users.values().any(|u| u.username == username) {
                return Err(format!("user '{username}' already exists"));
            }
            if users.values().any(|u| u.role == Role::Admin) {
                return Err("an admin user already exists, cannot bootstrap another".to_string());
            }
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

        Self::db_create_user(&self.conn, &user)?;
        self.users_cache
            .write()
            .insert(user.id.clone(), user.clone());
        self.usernames_cache
            .write()
            .insert(user.username.clone(), user.id.clone());
        Ok(user)
    }

    pub fn get_user_by_id(&self, user_id: &str) -> Option<User> {
        self.users_cache.read().get(user_id).cloned()
    }

    #[allow(dead_code)]
    pub fn get_user_by_username(&self, username: &str) -> Option<User> {
        let guard = self.usernames_cache.read();
        let id = guard.get(username)?.clone();
        drop(guard);
        self.users_cache.read().get(&id).cloned()
    }

    pub fn list_users(&self) -> Vec<User> {
        let users = self.users_cache.read();
        let mut users: Vec<User> = users.values().cloned().collect();
        users.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        users
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<User, String> {
        {
            let users = self.users_cache.read();
            if users.values().any(|u| u.username == username) {
                return Err(format!("user '{username}' already exists"));
            }
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

        Self::db_create_user(&self.conn, &user)?;
        self.users_cache
            .write()
            .insert(user.id.clone(), user.clone());
        self.usernames_cache
            .write()
            .insert(user.username.clone(), user.id.clone());
        Ok(user)
    }

    pub fn update_user(
        &self,
        user_id: &str,
        role: Option<Role>,
        enabled: Option<bool>,
    ) -> Result<User, String> {
        let updated = {
            let users = self.users_cache.read();
            let u = users
                .get(user_id)
                .ok_or_else(|| format!("user '{user_id}' not found"))?;
            let mut u = u.clone();
            if let Some(r) = role {
                u.role = r;
            }
            if let Some(e) = enabled {
                u.enabled = e;
            }
            u
        };

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(user_id.to_string()));
        if let Some(r) = role {
            params.insert("role".to_string(), Value::String(r.to_string()));
        }
        if let Some(e) = enabled {
            params.insert("enabled".to_string(), Value::Boolean(e));
        }

        let sets: Vec<&str> = role.iter().map(|_| "u.role = $role").chain(
            enabled.iter().map(|_| "u.enabled = $enabled"),
        ).collect();
        if !sets.is_empty() {
            let q = format!(
                "MATCH (u:{AUTH_USERS_TABLE} {{id: $id}}) SET {}",
                sets.join(", ")
            );
            self.conn
                .execute(&q, Some(params))
                .map_err(|e| format!("failed to update user: {e}"))?;
        }

        {
            let mut users = self.users_cache.write();
            users.insert(user_id.to_string(), updated.clone());
        }
        Ok(updated)
    }

    pub fn delete_user(&self, user_id: &str) -> Result<(), String> {
        let user = {
            let users = self.users_cache.read();
            users.get(user_id).cloned().ok_or_else(|| {
                format!("user '{user_id}' not found")
            })?
        };

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(user_id.to_string()));
        let delete_tokens = format!(
            "MATCH (t:{AUTH_TOKENS_TABLE}) WHERE t.user_id = $id DELETE t"
        );
        let delete_keys = format!(
            "MATCH (k:{AUTH_API_KEYS_TABLE}) WHERE k.user_id = $id DELETE k"
        );
        let delete_user = format!(
            "MATCH (u:{AUTH_USERS_TABLE} {{id: $id}}) DELETE u"
        );
        self.conn
            .execute(&delete_tokens, Some(params.clone()))
            .map_err(|e| format!("failed to delete user tokens: {e}"))?;
        self.conn
            .execute(&delete_keys, Some(params.clone()))
            .map_err(|e| format!("failed to delete user keys: {e}"))?;
        self.conn
            .execute(&delete_user, Some(params))
            .map_err(|e| format!("failed to delete user: {e}"))?;

        {
            let mut tcache = self.tokens_cache.write();
            tcache.retain(|_, t| t.user_id != user_id);
        }
        {
            let mut kcache = self.api_keys_cache.write();
            kcache.retain(|_, k| k.user_id != user_id);
        }
        {
            let mut users = self.users_cache.write();
            users.remove(user_id);
            self.usernames_cache.write().remove(&user.username);
        }

        Ok(())
    }

    pub fn reset_password(&self, user_id: &str, new_password: &str) -> Result<(), String> {
        let password_hash = password::hash_password(new_password)?;

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(user_id.to_string()));
        params.insert("ph".to_string(), Value::String(password_hash.clone()));
        let q = format!(
            "MATCH (u:{AUTH_USERS_TABLE} {{id: $id}}) SET u.password_hash = $ph"
        );
        self.conn
            .execute(&q, Some(params.clone()))
            .map_err(|e| format!("failed to update password: {e}"))?;

        let delete_tokens = format!(
            "MATCH (t:{AUTH_TOKENS_TABLE}) WHERE t.user_id = $id DELETE t"
        );
        self.conn
            .execute(&delete_tokens, Some(params))
            .map_err(|e| format!("failed to revoke user tokens: {e}"))?;

        {
            let mut tcache = self.tokens_cache.write();
            let hashes: Vec<String> = tcache
                .values()
                .filter(|t| t.user_id == user_id)
                .map(|t| t.token_hash.clone())
                .collect();
            for h in &hashes {
                self.token_bloom.mark_revoked(h);
            }
            tcache.retain(|_, t| t.user_id != user_id);
        }

        {
            let mut users = self.users_cache.write();
            if let Some(u) = users.get_mut(user_id) {
                u.password_hash = password_hash;
            }
        }

        Ok(())
    }

    fn db_create_user(conn: &Connection, user: &User) -> Result<(), String> {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(user.id.clone()));
        params.insert("username".to_string(), Value::String(user.username.clone()));
        params.insert("ph".to_string(), Value::String(user.password_hash.clone()));
        params.insert("role".to_string(), Value::String(user.role.to_string()));
        params.insert("enabled".to_string(), Value::Boolean(user.enabled));
        params.insert("created_at".to_string(), Value::Number(user.created_at as f64));
        let q = format!(
            "CREATE (u:{AUTH_USERS_TABLE} {{id: $id, username: $username, \
             password_hash: $ph, role: $role, enabled: $enabled, \
             created_at: $created_at, last_login: 0}}) RETURN u.id"
        );
        conn.execute(&q, Some(params))
            .map_err(|e| format!("failed to create user: {e}"))?;
        Ok(())
    }

    // ------------------------------------------------------------------
    //  Refresh Tokens
    // ------------------------------------------------------------------

    pub fn store_refresh_token(
        &self,
        user_id: &str,
        token_hash: &str,
        ttl_secs: u64,
    ) -> Result<String, String> {
        let now = now_secs();
        let id = generate_id("rt");
        let stored = StoredRefreshToken {
            id: id.clone(),
            user_id: user_id.to_string(),
            token_hash: token_hash.to_string(),
            expires_at: now + ttl_secs as i64,
            revoked: false,
            created_at: now,
        };

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(stored.id.clone()));
        params.insert("user_id".to_string(), Value::String(stored.user_id.clone()));
        params.insert("hash".to_string(), Value::String(stored.token_hash.clone()));
        params.insert("exp".to_string(), Value::Number(stored.expires_at as f64));
        params.insert("now".to_string(), Value::Number(stored.created_at as f64));
        let q = format!(
            "CREATE (t:{AUTH_TOKENS_TABLE} {{id: $id, user_id: $user_id, \
             token_hash: $hash, expires_at: $exp, created_at: $now}}) RETURN t.id"
        );
        self.conn
            .execute(&q, Some(params))
            .map_err(|e| format!("failed to store refresh token: {e}"))?;

        self.token_bloom.insert(&stored.token_hash);
        self.tokens_cache
            .write()
            .insert(stored.token_hash.clone(), stored);
        Ok(id)
    }

    pub fn validate_refresh_token(
        &self,
        raw_token: &str,
    ) -> Result<(User, String), String> {
        let token_hash = jwt::hmac_hash(raw_token, &self.jwt_secret);

        match self.token_bloom.check(&token_hash) {
            CacheResult::DefinitelyNotIssued => {
                return Err("invalid or expired refresh token".to_string());
            }
            CacheResult::Revoked => {
                return Err("refresh token has been revoked".to_string());
            }
            CacheResult::MaybeValid => {}
        }

        let now = now_secs();
        let tcache = self.tokens_cache.read();
        let stored = tcache
            .get(&token_hash)
            .filter(|t| t.expires_at > now)
            .ok_or_else(|| "invalid or expired refresh token".to_string())?;
        let user_id = stored.user_id.clone();
        let token_id = stored.id.clone();
        drop(tcache);

        let users = self.users_cache.read();
        let user = users
            .values()
            .find(|u| u.id == user_id && u.enabled)
            .ok_or_else(|| "user not found or disabled".to_string())?;
        Ok((user.clone(), token_id))
    }

    pub fn revoke_refresh_token(&self, raw_token: &str) -> Result<(), String> {
        let token_hash = jwt::hmac_hash(raw_token, &self.jwt_secret);

        {
            let tcache = self.tokens_cache.read();
            if !tcache.contains_key(&token_hash) {
                return Err("refresh token not found".to_string());
            }
        }

        let mut params = HashMap::new();
        params.insert("hash".to_string(), Value::String(token_hash.clone()));
        let q = format!(
            "MATCH (t:{AUTH_TOKENS_TABLE} {{token_hash: $hash}}) DELETE t"
        );
        self.conn
            .execute(&q, Some(params))
            .map_err(|e| format!("failed to revoke refresh token: {e}"))?;

        self.tokens_cache.write().remove(&token_hash);
        self.token_bloom.mark_revoked(&token_hash);
        Ok(())
    }

    // ------------------------------------------------------------------
    //  API Keys
    // ------------------------------------------------------------------

    pub fn create_api_key(
        &self,
        user_id: &str,
        name: &str,
        role_override: Option<Role>,
        expires_at: Option<i64>,
    ) -> Result<(String, ApiKey), String> {
        {
            let users = self.users_cache.read();
            if !users.values().any(|u| u.id == user_id && u.enabled) {
                return Err("user not found or disabled".to_string());
            }
        }

        let (key, key_hash) = jwt::generate_api_key(&self.jwt_secret);
        let prefix = key.chars().take(12).collect::<String>();
        let now = now_secs();
        let api_key = ApiKey {
            id: generate_id("ak"),
            user_id: user_id.to_string(),
            key_hash,
            name: name.to_string(),
            prefix,
            role_override,
            expires_at,
            created_at: now,
            revoked: false,
        };

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(api_key.id.clone()));
        params.insert("user_id".to_string(), Value::String(api_key.user_id.clone()));
        params.insert("hash".to_string(), Value::String(api_key.key_hash.clone()));
        params.insert("name".to_string(), Value::String(api_key.name.clone()));
        params.insert("prefix".to_string(), Value::String(api_key.prefix.clone()));
        params.insert("ro".to_string(), {
            match &api_key.role_override {
                Some(r) => Value::String(r.to_string()),
                None => Value::String(String::new()),
            }
        });
        params.insert("exp".to_string(), {
            Value::Number(api_key.expires_at.unwrap_or(0) as f64)
        });
        params.insert("now".to_string(), Value::Number(api_key.created_at as f64));
        let q = format!(
            "CREATE (k:{AUTH_API_KEYS_TABLE} {{id: $id, user_id: $user_id, \
             key_hash: $hash, name: $name, prefix: $prefix, \
             role_override: $ro, expires_at: $exp, created_at: $now}}) RETURN k.id"
        );
        self.conn
            .execute(&q, Some(params))
            .map_err(|e| format!("failed to create API key: {e}"))?;

        self.token_bloom.insert(&api_key.key_hash);
        self.api_keys_cache
            .write()
            .insert(api_key.key_hash.clone(), api_key.clone());
        Ok((key, api_key))
    }

    pub fn list_api_keys(&self, user_id: &str) -> Vec<ApiKey> {
        let kcache = self.api_keys_cache.read();
        kcache
            .values()
            .filter(|k| k.user_id == user_id)
            .cloned()
            .collect()
    }

    pub fn revoke_api_key(&self, key_id: &str) -> Result<(), String> {
        let key_hash = {
            let kcache = self.api_keys_cache.read();
            kcache
                .iter()
                .find(|(_, k)| k.id == key_id)
                .map(|(h, _)| h.clone())
                .ok_or_else(|| format!("API key '{key_id}' not found"))
        }?;

        let mut params = HashMap::new();
        params.insert("hash".to_string(), Value::String(key_hash.clone()));
        let q = format!(
            "MATCH (k:{AUTH_API_KEYS_TABLE} {{key_hash: $hash}}) DELETE k"
        );
        self.conn
            .execute(&q, Some(params))
            .map_err(|e| format!("failed to revoke API key: {e}"))?;

        self.api_keys_cache.write().remove(&key_hash);
        Ok(())
    }

    pub fn authenticate_api_key(&self, key: &str) -> Result<AuthenticatedUser, String> {
        let key_hash = jwt::api_key_hash(key, &self.jwt_secret);

        match self.token_bloom.check(&key_hash) {
            CacheResult::DefinitelyNotIssued => {
                return Err("invalid API key".to_string());
            }
            CacheResult::Revoked => {
                return Err("API key has been revoked".to_string());
            }
            CacheResult::MaybeValid => {}
        }

        let now = now_secs();
        let kcache = self.api_keys_cache.read();
        let stored = kcache
            .get(&key_hash)
            .ok_or_else(|| "invalid API key".to_string())?;
        if let Some(expires) = stored.expires_at {
            if now > expires {
                return Err("API key expired".to_string());
            }
        }
        let user_id = stored.user_id.clone();
        let role_override = stored.role_override;
        drop(kcache);

        let users = self.users_cache.read();
        let user = users
            .values()
            .find(|u| u.id == user_id && u.enabled)
            .ok_or_else(|| "user not found or disabled".to_string())?;
        let role = role_override.unwrap_or(user.role);
        Ok(AuthenticatedUser {
            user_id: user.id.clone(),
            username: user.username.clone(),
            role,
            auth_method: AuthMethod::ApiKey,
        })
    }

    // ------------------------------------------------------------------
    //  Garbage Collection
    // ------------------------------------------------------------------

    pub fn purge_expired(&self) -> Result<(), String> {
        let now = now_secs();

        let mut params = HashMap::new();
        params.insert("now".to_string(), Value::Number(now as f64));
        let del_tokens = format!(
            "MATCH (t:{AUTH_TOKENS_TABLE}) WHERE t.expires_at <= $now DELETE t"
        );
        let del_keys = format!(
            "MATCH (k:{AUTH_API_KEYS_TABLE}) WHERE k.expires_at > 0 AND k.expires_at <= $now DELETE k"
        );
        self.conn
            .execute(&del_tokens, Some(params.clone()))
            .map_err(|e| format!("failed to purge expired tokens: {e}"))?;
        self.conn
            .execute(&del_keys, Some(params))
            .map_err(|e| format!("failed to purge expired API keys: {e}"))?;

        {
            let mut tcache = self.tokens_cache.write();
            tcache.retain(|_, t| t.expires_at > now);
        }
        {
            let mut kcache = self.api_keys_cache.write();
            kcache.retain(|_, k| {
                k.expires_at.map_or(true, |exp| exp > now)
            });
        }

        self.rebuild_cache();
        Ok(())
    }

    fn rebuild_cache(&self) {
        let tcache = self.tokens_cache.read();
        let all_hashes: Vec<String> = tcache.values().map(|t| t.token_hash.clone()).collect();
        drop(tcache);

        let kcache = self.api_keys_cache.read();
        let all_key_hashes: Vec<String> = kcache.values().map(|k| k.key_hash.clone()).collect();
        drop(kcache);

        let mut combined = all_hashes;
        combined.extend(all_key_hashes);
        self.token_bloom.rebuild(&combined, &[]);
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

fn as_string_array(col: &Arc<dyn Array>) -> Result<&StringArray, String> {
    col.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "expected string array column".to_string())
}

fn as_int_array(col: &Arc<dyn Array>) -> Result<&Int64Array, String> {
    col.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "expected int64 array column".to_string())
}

fn as_bool_array(col: &Arc<dyn Array>) -> Result<&BooleanArray, String> {
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "expected boolean array column".to_string())
}
