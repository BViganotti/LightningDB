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
