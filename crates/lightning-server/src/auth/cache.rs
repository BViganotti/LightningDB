use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;
use sha2::{Digest, Sha256};

const BLOOM_NUM_BITS: u64 = 1 << 20;

pub enum CacheResult {
    DefinitelyNotIssued,
    Revoked,
    MaybeValid,
}

struct BloomFilter {
    bits: Vec<u64>,
    mask: u64,
}

impl BloomFilter {
    fn new() -> Self {
        let num_u64s = ((BLOOM_NUM_BITS + 63) / 64) as usize;
        Self {
            bits: vec![0; num_u64s],
            mask: BLOOM_NUM_BITS - 1,
        }
    }

    fn insert(&mut self, item: &[u8]) {
        for h in &Self::hash_item(item) {
            let idx = (h & self.mask) as usize;
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    fn contains(&self, item: &[u8]) -> bool {
        for h in &Self::hash_item(item) {
            let idx = (h & self.mask) as usize;
            if self.bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn clear(&mut self) {
        for w in &mut self.bits {
            *w = 0;
        }
    }

    fn hash_item(item: &[u8]) -> [u64; 4] {
        let hash = Sha256::digest(item);
        let mut result = [0u64; 4];
        for i in 0..4 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&hash[i * 8..(i + 1) * 8]);
            result[i] = u64::from_be_bytes(bytes);
        }
        result
    }
}

pub struct TokenCache {
    bloom: RwLock<BloomFilter>,
    revoked: RwLock<HashMap<String, Arc<AtomicBool>>>,
    generation: AtomicU64,
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            bloom: RwLock::new(BloomFilter::new()),
            revoked: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, token_hash: &str) {
        self.bloom.write().insert(token_hash.as_bytes());
    }

    pub fn mark_revoked(&self, token_hash: &str) {
        self.revoked
            .write()
            .insert(token_hash.to_string(), Arc::new(AtomicBool::new(true)));
    }

    pub fn check(&self, token_hash: &str) -> CacheResult {
        let bloom = self.bloom.read();
        if !bloom.contains(token_hash.as_bytes()) {
            return CacheResult::DefinitelyNotIssued;
        }
        drop(bloom);
        let revoked = self.revoked.read();
        if let Some(flag) = revoked.get(token_hash) {
            if flag.load(Ordering::Acquire) {
                return CacheResult::Revoked;
            }
        }
        CacheResult::MaybeValid
    }

    pub fn rebuild(&self, all_hashes: &[String], revoked_hashes: &[String]) {
        {
            let mut bloom = self.bloom.write();
            bloom.clear();
            for h in all_hashes {
                bloom.insert(h.as_bytes());
            }
        }
        {
            let mut revoked = self.revoked.write();
            revoked.clear();
            for h in revoked_hashes {
                revoked.insert(h.clone(), Arc::new(AtomicBool::new(true)));
            }
        }
        self.generation.fetch_add(1, Ordering::Release);
    }
}
