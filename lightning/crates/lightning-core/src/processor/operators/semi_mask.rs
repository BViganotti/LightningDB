use roaring::RoaringTreemap;

#[derive(Debug, Default)]
pub struct SemiMask {
    bitmap: RoaringTreemap,
}

impl SemiMask {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    pub fn insert(&mut self, offset: u64) {
        self.bitmap.insert(offset);
    }

    pub fn contains(&self, offset: u64) -> bool {
        self.bitmap.contains(offset)
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }
}
