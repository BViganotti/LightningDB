use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::Result;
use std::sync::Arc;

pub struct CSRIndex {
    pub(crate) offset_fh: Arc<FileHandle>,
    pub(crate) adj_node_fh: Arc<FileHandle>,
}

impl CSRIndex {
    pub fn new(offset_fh: Arc<FileHandle>, adj_node_fh: Arc<FileHandle>) -> Self {
        Self {
            offset_fh,
            adj_node_fh,
        }
    }

    /// Allocation-free neighbor iteration
    pub fn for_each_neighbor<F>(
        &self,
        bm: &BufferManager,
        node_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        let start_page = (node_id * 8) / PAGE_SIZE as u64;
        let start_offset_in_page = (node_id * 8) % PAGE_SIZE as u64;
        if start_page >= self.offset_fh.get_num_pages() {
            return Ok(());
        }

        let end_node_id = node_id + 1;
        let end_page = (end_node_id * 8) / PAGE_SIZE as u64;
        let end_offset_in_page = (end_node_id * 8) % PAGE_SIZE as u64;

        let (start, end) = {
            let start_frame = bm.pin_page(self.offset_fh.clone(), start_page, tx)?;
            let start = u64::from_le_bytes(
                start_frame.as_slice()[start_offset_in_page as usize..start_offset_in_page as usize + 8]
                    .try_into()
                    .unwrap(),
            );

            let end = if start_page == end_page {
                u64::from_le_bytes(
                    start_frame.as_slice()[end_offset_in_page as usize..end_offset_in_page as usize + 8]
                        .try_into()
                        .unwrap(),
                )
            } else {
                if end_page >= self.offset_fh.get_num_pages() {
                    return Ok(());
                }
                let end_frame = bm.pin_page(self.offset_fh.clone(), end_page, tx)?;
                u64::from_le_bytes(
                    end_frame.as_slice()[end_offset_in_page as usize..end_offset_in_page as usize + 8]
                        .try_into()
                        .unwrap(),
                )
            };
            (start, end)
        };

        if end <= start {
            return Ok(());
        }

        let mut i = start;
        while i < end {
            let adj_page = (i * 8) / PAGE_SIZE as u64;
            let adj_offset_in_page = (i * 8) % PAGE_SIZE as u64;
            let adj_frame = bm.pin_page(self.adj_node_fh.clone(), adj_page, tx)?;

            let remaining_in_page = (PAGE_SIZE as u64 - adj_offset_in_page) / 8;
            let to_read = std::cmp::min((end - i) as u64, remaining_in_page) as usize;

            for j in 0..to_read {
                let offset = (adj_offset_in_page as usize) + (j * 8);
                let neighbor =
                    u64::from_le_bytes(adj_frame.as_slice()[offset..offset + 8].try_into().unwrap());
                f(neighbor);
            }
            i += to_read as u64;
        }

        Ok(())
    }

    pub fn get_neighbors(
        &self,
        bm: &BufferManager,
        node_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<u64>> {
        let mut neighbors = Vec::new();
        self.for_each_neighbor(bm, node_id, tx, |n| neighbors.push(n))?;
        Ok(neighbors)
    }

    pub fn build(
        bm: &BufferManager,
        offset_fh: Arc<FileHandle>,
        adj_node_fh: Arc<FileHandle>,
        edges: &[(u64, u64)],
        num_nodes: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let _tx_id = tx.tx_id;
        let mut sorted_edges = edges.to_vec();
        sorted_edges.sort_by_key(|e| e.0);

        let mut offsets = vec![0u64; (num_nodes + 2) as usize];
        for &(src, _) in &sorted_edges {
            if src <= num_nodes {
                offsets[(src + 1) as usize] += 1;
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }

        for (i, &val) in offsets.iter().enumerate() {
            let page_idx = (i as u64 * 8) / PAGE_SIZE as u64;
            let offset_in_page = (i as u64 * 8) % PAGE_SIZE as u64;
            while offset_fh.get_num_pages() <= page_idx {
                offset_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(offset_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                std::ptr::copy_nonoverlapping(
                    val.to_le_bytes().as_ptr(),
                    ptr.add(offset_in_page as usize),
                    8,
                );
            }
        }

        for (i, &(_, dst)) in sorted_edges.iter().enumerate() {
            let page_idx = (i as u64 * 8) / PAGE_SIZE as u64;
            let offset_in_page = (i as u64 * 8) % PAGE_SIZE as u64;
            while adj_node_fh.get_num_pages() <= page_idx {
                adj_node_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(adj_node_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                std::ptr::copy_nonoverlapping(
                    dst.to_le_bytes().as_ptr(),
                    ptr.add(offset_in_page as usize),
                    8,
                );
            }
        }

        Ok(())
    }
}
