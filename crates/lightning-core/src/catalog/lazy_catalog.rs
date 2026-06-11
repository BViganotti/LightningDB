use crate::catalog::Catalog;
use crate::Result;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

pub const CATALOG_SAVE_TX_INTERVAL: u64 = 1000;

pub struct LazyCatalog {
    inner: Arc<RwLock<Catalog>>,
    dirty: AtomicBool,
    last_saved_tx_count: AtomicU64,
    path: parking_lot::RwLock<Option<std::path::PathBuf>>,
}

impl LazyCatalog {
    pub fn new(catalog: Catalog, path: Option<std::path::PathBuf>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(catalog)),
            dirty: AtomicBool::new(false),
            last_saved_tx_count: AtomicU64::new(0),
            path: parking_lot::RwLock::new(path),
        }
    }

    pub fn from_disk(path: &std::path::Path) -> std::io::Result<Self> {
        let catalog = Catalog::load_from_disk(path)
            .map_err(std::io::Error::other)?;
        Ok(Self::new(catalog, Some(path.to_path_buf())))
    }

    pub fn set_path(&self, path: std::path::PathBuf) {
        let mut p = self.path.write();
        *p = Some(path);
    }

    pub fn get_path(&self) -> Option<std::path::PathBuf> {
        self.path.read().clone()
    }

    #[inline]
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Catalog> {
        self.inner.read()
    }

    #[inline]
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Catalog> {
        self.inner.write()
    }

    #[inline]
    pub fn inner_catalog(&self) -> Arc<RwLock<Catalog>> {
        Arc::clone(&self.inner)
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
        debug!("Catalog marked dirty");
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub fn transactions_since_last_save(&self) -> u64 {
        self.last_saved_tx_count.load(Ordering::Acquire)
    }

    pub fn save_if_needed(&self, current_tx_count: u64) -> Result<()> {
        let dirty = self.dirty.load(Ordering::Acquire);
        let last_saved = self.last_saved_tx_count.load(Ordering::Acquire);
        let tx_since_last_save = current_tx_count.saturating_sub(last_saved);

        if !dirty && tx_since_last_save < CATALOG_SAVE_TX_INTERVAL {
            debug!(
                "Catalog save skipped: dirty={}, tx_since_last_save={}",
                dirty, tx_since_last_save,
            );
            return Ok(());
        }

        self.save_internal(current_tx_count)
    }

    pub fn force_save(&self) -> Result<()> {
        let current = self.last_saved_tx_count.load(Ordering::Acquire);
        self.save_internal(current + 1)
    }

    /// Save the catalog to disk using an already-acquired lock guard reference.
    /// The caller passes a reference to the locked Catalog, and this method uses
    /// it directly instead of acquiring its own lock.
    /// Returns an error if the provided catalog reference does not match
    /// this LazyCatalog's inner catalog (prevents saving a stale/foreign catalog).
    pub fn force_save_with_catalog(&self, catalog: &Catalog) -> Result<()> {
        // Verify the catalog matches — we compare the pointer to the inner lock's data.
        // This ensures we're not saving a stale/foreign catalog that bypasses dirty tracking.
        let inner_guard = self.inner.read();
        if !std::ptr::eq(catalog as *const Catalog, &*inner_guard as *const Catalog) {
            return Err(crate::LightningError::Database(
                "force_save_with_catalog: catalog reference does not match inner catalog".into()
            ));
        }
        drop(inner_guard);

        let current = self.last_saved_tx_count.load(Ordering::Acquire);
        let path = match self.get_path() {
            Some(p) => p,
            None => {
                return Ok(());
            }
        };
        catalog.save_to_disk(&path)?;
        self.dirty.store(false, Ordering::Release);
        self.last_saved_tx_count.store(current + 1, Ordering::Release);
        Ok(())
    }

    fn save_internal(&self, current_tx_count: u64) -> Result<()> {
        let path = match self.get_path() {
            Some(p) => p,
            None => {
                debug!("Catalog save skipped: no path configured");
                return Ok(());
            }
        };

        let catalog_guard = self.inner.read();

        info!("Saving catalog to disk: {}", path.display());
        catalog_guard.save_to_disk(&path)?;

        self.dirty.store(false, Ordering::Release);
        self.last_saved_tx_count.store(current_tx_count, Ordering::Release);

        debug!(
            "Catalog saved successfully, dirty=false, last_saved_tx_count={}",
            current_tx_count
        );
        Ok(())
    }

    pub fn clone_inner(&self) -> Arc<RwLock<Catalog>> {
        Arc::clone(&self.inner)
    }
}

impl Clone for LazyCatalog {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            dirty: AtomicBool::new(self.dirty.load(Ordering::Acquire)),
            last_saved_tx_count: AtomicU64::new(self.last_saved_tx_count.load(Ordering::Acquire)),
            path: parking_lot::RwLock::new(self.path.read().clone()),
        }
    }
}
