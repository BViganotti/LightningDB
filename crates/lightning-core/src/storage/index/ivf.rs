use crate::Result;

/// IVF (Inverted File) index for approximate nearest neighbor search.
/// Clusters vectors into K centroids via K-means. At search time, only
/// the NProbe closest clusters are searched, reducing O(n) to O(n/K * NProbe).
///
/// Simpler alternative to HNSW. Good for high-dimensional data where
/// HNSW's graph construction cost is prohibitive.
pub struct IvfIndex {
    /// Number of centroids (clusters).
    num_clusters: usize,
    /// Number of closest clusters to probe during search.
    nprobe: usize,
    /// Dimension of vectors.
    dimension: usize,
    /// Cluster centroids: centroids[i] is the i-th centroid vector.
    centroids: Vec<Vec<f32>>,
    /// Inverted lists: lists[i] contains (node_id, vector) for cluster i.
    lists: Vec<Vec<(u64, Vec<f32>)>>,
}

impl IvfIndex {
    pub fn new(dimension: usize, num_clusters: usize, nprobe: usize) -> Self {
        Self {
            num_clusters,
            nprobe,
            dimension,
            centroids: Vec::new(),
            lists: Vec::new(),
        }
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn len(&self) -> usize {
        self.lists.iter().map(|l| l.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|v| v * v).sum::<f32>().sqrt();
        1.0 - (dot / (norm_a * norm_b.max(f32::EPSILON)))
    }

    /// Find the closest centroid for a vector.
    fn nearest_centroid(&self, vec: &[f32]) -> usize {
        let mut best = 0usize;
        let mut best_dist = f32::MAX;
        for (i, c) in self.centroids.iter().enumerate() {
            let d = Self::cosine_distance(vec, c);
            if d < best_dist {
                best_dist = d;
                best = i;
            }
        }
        best
    }

    /// Build the index from a set of vectors using K-means clustering.
    pub fn build(&mut self, data: &[(u64, Vec<f32>)]) {
        if data.is_empty() || self.dimension == 0 {
            return;
        }

        let k = std::cmp::min(self.num_clusters, data.len());
        self.centroids.clear();
        self.lists = vec![Vec::new(); k];

        // Initialize centroids: pick k random points (using first k for determinism)
        for i in 0..k {
            self.centroids.push(data[i].1.clone());
        }

        // Run K-means for a fixed number of iterations
        let max_iter = 10;
        for _iter in 0..max_iter {
            // Assign each point to nearest centroid
            let mut new_lists: Vec<Vec<(u64, Vec<f32>)>> = vec![Vec::new(); k];
            for (id, vec) in data {
                let c = self.nearest_centroid(vec);
                new_lists[c].push((*id, vec.clone()));
            }

            // Recompute centroids
            let mut new_centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
            for i in 0..k {
                if new_lists[i].is_empty() {
                    new_centroids.push(self.centroids[i].clone());
                    continue;
                }
                let mut sum = vec![0.0f32; self.dimension];
                for (_, vec) in &new_lists[i] {
                    for (j, v) in vec.iter().enumerate() {
                        sum[j] += v;
                    }
                }
                let n = new_lists[i].len() as f32;
                for s in &mut sum {
                    *s /= n;
                }
                new_centroids.push(sum);
            }

            self.centroids = new_centroids;
            self.lists = new_lists;
        }
    }

    /// Insert a single vector (finds nearest centroid and appends to its list).
    pub fn insert(&mut self, id: u64, vec: Vec<f32>) {
        if self.centroids.is_empty() {
            self.centroids.push(vec.clone());
            self.lists.push(Vec::new());
            self.lists[0].push((id, vec));
            return;
        }
        let c = self.nearest_centroid(&vec);
        // Extend lists if needed
        while self.lists.len() <= c {
            self.lists.push(Vec::new());
        }
        self.lists[c].push((id, vec));
    }

    /// Search for k approximate nearest neighbors.
    /// Probes the NProbe closest clusters and returns the top k results.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        if self.centroids.is_empty() {
            return Vec::new();
        }

        // Find NProbe closest centroids
        let mut centroid_dists: Vec<(usize, f32)> = self.centroids.iter().enumerate()
            .map(|(i, c)| (i, Self::cosine_distance(query, c)))
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let nprobe = std::cmp::min(self.nprobe, centroid_dists.len());
        let mut candidates: Vec<(u64, f32)> = Vec::new();

        for i in 0..nprobe {
            let cluster_idx = centroid_dists[i].0;
            if cluster_idx < self.lists.len() {
                for (id, vec) in &self.lists[cluster_idx] {
                    let d = Self::cosine_distance(query, vec);
                    candidates.push((*id, d));
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        candidates
    }
}
