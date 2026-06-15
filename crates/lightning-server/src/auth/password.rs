use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use zeroize::Zeroize;

pub fn hash_password(password: &str) -> Result<String, String> {
    let mut pwd_bytes = password.as_bytes().to_vec();
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(&pwd_bytes, &salt)
        .map_err(|e| format!("password hashing failed: {e}"))?;
    pwd_bytes.zeroize();
    Ok(hash.serialize().to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    let mut pwd_bytes = password.as_bytes().to_vec();
    let parsed_hash = PasswordHash::new(hash).map_err(|e| format!("invalid password hash: {e}"))?;
    let argon2 = Argon2::default();
    let result = argon2.verify_password(&pwd_bytes, &parsed_hash);
    pwd_bytes.zeroize();
    match result {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::errors::Error::Password) => Ok(false),
        Err(e) => Err(format!("password verification error: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_verify() {
        let password = "super-secure-password!@#$";
        let hash = hash_password(password).unwrap();
        assert!(verify_password(password, &hash).unwrap());
        assert!(!verify_password("wrong-password", &hash).unwrap());
    }
}
