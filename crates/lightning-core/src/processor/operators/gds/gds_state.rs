use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub struct GDSFrontier {
    pub(crate) nodes: Vec<AtomicU32>,
    active_nodes: std::sync::Mutex<Vec<u32>>,
    num_nodes: usize,
}

impl GDSFrontier {
    pub fn new(num_nodes: usize) -> Self {
        let mut nodes = Vec::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            nodes.push(AtomicU32::new(u32::MAX)); // Unvisited
        }
        Self {
            nodes,
            active_nodes: std::sync::Mutex::new(Vec::new()),
            num_nodes,
        }
    }

    pub fn is_visited(&self, node_id: usize) -> bool {
        // Use Acquire to synchronize with the SeqCst store in visit()
        self.nodes[node_id].load(Ordering::Acquire) != u32::MAX
    }

    pub fn visit(&self, node_id: usize, distance: u32) -> bool {
        // Returns true if we successfully visited for the first time
        // AcqRel is sufficient: Release makes our distance visible to other
        // threads, Acquire sees their prior writes.
        let first_visit = self.nodes[node_id]
            .compare_exchange(u32::MAX, distance, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if first_visit {
            self.active_nodes.lock().unwrap().push(node_id as u32);
        }
        first_visit
    }

    pub fn get_distance(&self, node_id: usize) -> u32 {
        self.nodes[node_id].load(Ordering::Acquire)
    }

    pub fn clear(&mut self) {
        let active = std::mem::take(&mut *self.active_nodes.lock().unwrap());
        for &node_id in &active {
            // Use Release to ensure prior reads complete before clear is visible
            self.nodes[node_id as usize].store(u32::MAX, Ordering::Release);
        }
    }
}

pub struct GDSState {
    pub current_frontier: Arc<GDSFrontier>,
    pub next_frontier: Arc<GDSFrontier>,
    pub iteration: u32,
}

impl GDSState {
    pub fn new(num_nodes: usize) -> Self {
        Self {
            current_frontier: Arc::new(GDSFrontier::new(num_nodes)),
            next_frontier: Arc::new(GDSFrontier::new(num_nodes)),
            iteration: 0,
        }
    }

    pub fn swap_frontiers(&mut self) {
        std::mem::swap(&mut self.current_frontier, &mut self.next_frontier);
        // If Arc::get_mut fails (refcount > 1 due to clones),
        // create a fresh frontier instead of silently skipping clear()
        if Arc::get_mut(&mut self.next_frontier).is_none() {
            let num_nodes = self.current_frontier.num_nodes;
            self.next_frontier = Arc::new(GDSFrontier::new(num_nodes));
        } else {
            // SAFETY: just verified get_mut returns Some
            Arc::get_mut(&mut self.next_frontier).unwrap().clear();
        }
        self.iteration += 1;
    }
}
