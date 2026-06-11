use std::sync::atomic::{AtomicU64, Ordering};

pub struct PageState {
    // Highest 1 bit: dirty bit
    // Next 7 bits: page state (UNLOCKED, LOCKED, MARKED, EVICTED)
    // Lowest 56 bits: version
    state_and_version: AtomicU64,
}

impl Default for PageState {
    fn default() -> Self {
        Self::new()
    }
}

impl PageState {
    pub const UNLOCKED: u64 = 0;
    pub const LOCKED: u64 = 1;
    pub const MARKED: u64 = 2;
    pub const EVICTED: u64 = 3;

    const DIRTY_MASK: u64 = 0x8000_0000_0000_0000;
    const REFERENCED_MASK: u64 = 0x4000_0000_0000_0000;
    const STATE_MASK: u64 = 0x3F00_0000_0000_0000;
    const VERSION_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;
    const STATE_SHIFT: u32 = 56;

    pub fn new() -> Self {
        Self {
            state_and_version: AtomicU64::new(Self::EVICTED << Self::STATE_SHIFT),
        }
    }

    pub fn get_state(&self) -> u64 {
        let val = self.state_and_version.load(Ordering::Acquire);
        (val & Self::STATE_MASK) >> Self::STATE_SHIFT
    }

    pub fn get_version(&self) -> u64 {
        let val = self.state_and_version.load(Ordering::Acquire);
        val & Self::VERSION_MASK
    }

    pub fn is_dirty(&self) -> bool {
        let val = self.state_and_version.load(Ordering::Acquire);
        (val & Self::DIRTY_MASK) != 0
    }

    pub fn is_referenced(&self) -> bool {
        let val = self.state_and_version.load(Ordering::Acquire);
        (val & Self::REFERENCED_MASK) != 0
    }

    pub fn try_lock(&self, old_state_and_version: u64) -> bool {
        // Reject if already locked — prevents idempotent re-locking
        if old_state_and_version & Self::STATE_MASK == Self::LOCKED << Self::STATE_SHIFT {
            return false;
        }
        let new_val = self.update_state_with_same_version(old_state_and_version, Self::LOCKED);
        self.state_and_version
            .compare_exchange(
                old_state_and_version,
                new_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn unlock(&self) {
        let old_val = self.state_and_version.load(Ordering::Acquire);
        let new_val = self.update_state_and_increment_version(old_val, Self::UNLOCKED);
        self.state_and_version.store(new_val, Ordering::Release);
    }

    pub fn set_dirty(&self) {
        self.state_and_version
            .fetch_or(Self::DIRTY_MASK, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        self.state_and_version
            .fetch_and(!Self::DIRTY_MASK, Ordering::Release);
    }

    pub fn set_referenced(&self) {
        self.state_and_version
            .fetch_or(Self::REFERENCED_MASK, Ordering::Release);
    }

    pub fn clear_referenced(&self) {
        self.state_and_version
            .fetch_and(!Self::REFERENCED_MASK, Ordering::Release);
    }

    pub fn get_state_and_version(&self) -> u64 {
        self.state_and_version.load(Ordering::Acquire)
    }

    fn update_state_with_same_version(&self, old_val: u64, new_state: u64) -> u64 {
        (old_val & !Self::STATE_MASK) | (new_state << Self::STATE_SHIFT)
    }

    fn update_state_and_increment_version(&self, old_val: u64, new_state: u64) -> u64 {
        let new_version = (old_val & Self::VERSION_MASK).wrapping_add(1) & Self::VERSION_MASK;
        (old_val & !Self::STATE_MASK & !Self::VERSION_MASK)
            | (new_state << Self::STATE_SHIFT)
            | new_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_evicted_with_zero_version() {
        let ps = PageState::new();
        assert_eq!(ps.get_state(), PageState::EVICTED);
        assert_eq!(ps.get_version(), 0);
        assert!(!ps.is_dirty());
        assert!(!ps.is_referenced());
        let raw = ps.get_state_and_version();
        assert_eq!(raw, PageState::EVICTED << PageState::STATE_SHIFT);
    }

    #[test]
    fn test_default_trait() {
        let ps: PageState = Default::default();
        assert_eq!(ps.get_state(), PageState::EVICTED);
        assert_eq!(ps.get_version(), 0);
    }

    #[test]
    fn test_lock_unlock_roundtrip() {
        let ps = PageState::new();
        let sv = ps.get_state_and_version();
        assert!(ps.try_lock(sv));
        assert_eq!(ps.get_state(), PageState::LOCKED);
        assert_eq!(ps.get_version(), 0);

        ps.unlock();
        assert_eq!(ps.get_state(), PageState::UNLOCKED);
        assert_eq!(ps.get_version(), 1);
    }

    #[test]
    fn test_try_lock_fails_with_stale_version() {
        let ps = PageState::new();
        let sv = ps.get_state_and_version();
        // Lock with an outdated/synthetic old_val
        assert!(!ps.try_lock(sv + 1));
        assert_eq!(ps.get_state(), PageState::EVICTED);
    }

    #[test]
    fn test_try_lock_fails_when_already_locked() {
        let ps = PageState::new();
        let sv = ps.get_state_and_version();
        assert!(ps.try_lock(sv));
        // Second lock attempt should fail because state is now LOCKED
        let sv_locked = ps.get_state_and_version();
        assert!(!ps.try_lock(sv_locked));
        assert_eq!(ps.get_state(), PageState::LOCKED);
    }

    #[test]
    fn test_consecutive_lock_unlock_cycles_increment_version() {
        let ps = PageState::new();
        for expected_version in 1..=10 {
            let sv = ps.get_state_and_version();
            assert!(ps.try_lock(sv));
            assert_eq!(ps.get_state(), PageState::LOCKED);
            ps.unlock();
            assert_eq!(ps.get_state(), PageState::UNLOCKED);
            assert_eq!(ps.get_version(), expected_version);
        }
    }

    #[test]
    fn test_dirty_bit_set_and_clear() {
        let ps = PageState::new();
        assert!(!ps.is_dirty());

        ps.set_dirty();
        assert!(ps.is_dirty());

        ps.clear_dirty();
        assert!(!ps.is_dirty());
    }

    #[test]
    fn test_referenced_bit_set_and_clear() {
        let ps = PageState::new();
        assert!(!ps.is_referenced());

        ps.set_referenced();
        assert!(ps.is_referenced());

        ps.clear_referenced();
        assert!(!ps.is_referenced());
    }

    #[test]
    fn test_dirty_and_referenced_coexist() {
        let ps = PageState::new();
        ps.set_dirty();
        ps.set_referenced();
        assert!(ps.is_dirty());
        assert!(ps.is_referenced());

        ps.clear_dirty();
        assert!(!ps.is_dirty());
        assert!(ps.is_referenced());

        ps.clear_referenced();
        assert!(!ps.is_dirty());
        assert!(!ps.is_referenced());
    }

    #[test]
    fn test_dirty_persists_across_lock_unlock() {
        let ps = PageState::new();
        ps.set_dirty();
        let sv = ps.get_state_and_version();
        assert!(ps.try_lock(sv));
        assert!(ps.is_dirty());
        ps.unlock();
        assert!(ps.is_dirty());
        // unlocking increments version but the dirty flag survives
        assert_eq!(ps.get_version(), 1);
    }

    #[test]
    fn test_referenced_persists_across_lock_unlock() {
        let ps = PageState::new();
        ps.set_referenced();
        let sv = ps.get_state_and_version();
        assert!(ps.try_lock(sv));
        assert!(ps.is_referenced());
        ps.unlock();
        assert!(ps.is_referenced());
    }

    #[test]
    fn test_try_lock_with_stale_version_fails_after_unlock() {
        let ps = PageState::new();
        let sv0 = ps.get_state_and_version();
        assert!(ps.try_lock(sv0));
        ps.unlock();
        // sv0 is stale — version is now 1, so try_lock(sv0) must fail
        assert!(!ps.try_lock(sv0));
        // But try_lock with the current value succeeds
        let sv1 = ps.get_state_and_version();
        assert!(ps.try_lock(sv1));
    }

    #[test]
    fn test_unlock_with_interleaved_dirty_bits() {
        let ps = PageState::new();
        let sv = ps.get_state_and_version();
        assert!(ps.try_lock(sv));
        ps.set_dirty();
        ps.set_referenced();
        ps.unlock();
        assert_eq!(ps.get_state(), PageState::UNLOCKED);
        assert_eq!(ps.get_version(), 1);
        assert!(ps.is_dirty());
        assert!(ps.is_referenced());
    }

    #[test]
    fn test_zero_raw_value_not_allowed() {
        // A raw value of 0 would have state=UNLOCKED, version=0, not dirty, not referenced.
        // This is a valid concrete state but new() always starts as EVICTED.
        let ps = PageState::new();
        let raw = ps.get_state_and_version();
        assert_ne!(raw, 0, "new() must not produce zero (zero means UNLOCKED|v0)");
    }

    #[test]
    fn test_get_state_and_version_roundtrip() {
        let ps = PageState::new();
        let raw = ps.get_state_and_version();
        let state = (raw & PageState::STATE_MASK) >> PageState::STATE_SHIFT;
        let version = raw & PageState::VERSION_MASK;
        let dirty = (raw & PageState::DIRTY_MASK) != 0;
        let referenced = (raw & PageState::REFERENCED_MASK) != 0;
        assert_eq!(state, PageState::EVICTED);
        assert_eq!(version, 0);
        assert!(!dirty);
        assert!(!referenced);
    }

    #[test]
    fn test_state_isolation_in_packed_representation() {
        // Verify that state bits don't leak into version bits and vice versa
        let ps = PageState::new();
        let raw = ps.get_state_and_version();

        // State bits should be in the STATE_MASK region only
        let state_only = raw & PageState::STATE_MASK;
        let version_only = raw & PageState::VERSION_MASK;
        let dirty_only = raw & PageState::DIRTY_MASK;
        let referenced_only = raw & PageState::REFERENCED_MASK;

        // These should be disjoint
        let combined = state_only | version_only | dirty_only | referenced_only;
        assert_eq!(combined, raw, "fields must not overlap in bit layout");

        // Toggle each flag and verify isolation
        ps.set_dirty();
        assert!((ps.get_state_and_version() & PageState::DIRTY_MASK) != 0);
        assert!(ps.get_version() == 0);
        assert_eq!(ps.get_state(), PageState::EVICTED);

        ps.set_referenced();
        assert!((ps.get_state_and_version() & PageState::REFERENCED_MASK) != 0);
        assert!(ps.get_version() == 0);
        assert_eq!(ps.get_state(), PageState::EVICTED);
    }
}
