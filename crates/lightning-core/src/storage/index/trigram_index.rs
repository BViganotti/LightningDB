use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum candidate ratio - if more than this fraction of total rows match a trigram,
/// we skip the trigram index and fall back to full scan.
/// 0.5 = if >50% of rows match a single trigram, skip index.
const DEFAULT_CANDIDATE_THRESHOLD_RATIO: f64 = 0.5;

/// Minimum candidate count to use trigram index.
/// Even if ratio is low, if absolute number is tiny, full scan may be faster.
const MIN_CANDIDATES_TO_USE_INDEX: u64 = 100;

/// Small table threshold - full scan always faster for tables under this size.
const SMALL_TABLE_ROW_COUNT: u64 = 1000;

/// Short pattern penalty threshold - for 1-2 char patterns in tables over this size,
/// trigram intersection overhead exceeds full scan cost.
const SHORT_PATTERN_TABLE_SIZE: u64 = 10000;

/// Large table threshold - above this, use stricter ratio (30%) to avoid
/// memory pressure from large candidate sets.
const LARGE_TABLE_ROW_COUNT: u64 = 100_000;

/// Stricter threshold for large tables to avoid memory pressure.
const LARGE_TABLE_THRESHOLD_RATIO: f64 = 0.3;

/// Statistics about the trigram index for adaptive query planning.
#[derive(Debug, Clone, Default)]
pub struct TrigramStats {
    pub total_rows: u64,
    pub total_trigrams: u64,
    pub avg_posting_list_size: f64,
    pub max_posting_list_size: u64,
}

impl Default for TrigramIndex {
    fn default() -> Self {
        Self::new(String::new())
    }
}

/// A trigram-based inverted index for O(1) substring search.
///
/// Splits every indexed string into 3-character substrings (trigrams) and maintains
/// a posting list mapping each trigram to the set of row IDs containing it.
///
/// Query algorithm:
/// 1. Extract trigrams from the search pattern
/// 2. Intersect posting lists → candidate row IDs
/// 3. Caller verifies candidates with actual `str::contains()` (eliminates false positives)
///
/// For patterns < 3 chars, falls back to bigram/unigram posting lists.
///
/// Performance optimization:
/// - Tracks statistics to determine when trigram index would be slower than full scan
/// - Falls back to full scan when candidate set is too large (common trigrams)
pub struct TrigramIndex {
    /// trigram → sorted list of row IDs
    trigrams: RwLock<HashMap<[u8; 3], Vec<u64>>>,
    /// bigram → sorted list of row IDs (for 2-char patterns)
    bigrams: RwLock<HashMap<[u8; 2], Vec<u64>>>,
    /// unigram → sorted list of row IDs (for 1-char patterns)
    unigrams: RwLock<HashMap<u8, Vec<u64>>>,
    /// Column name this index covers
    pub column_name: String,
    /// Total number of rows indexed
    total_rows: AtomicU64,
    /// Total number of trigrams across all rows
    total_trigrams: AtomicU64,
    /// Running sum for average calculation (use (sum / count) for actual avg)
    posting_size_sum: AtomicU64,
    /// Maximum posting list size seen
    max_posting_size: AtomicU64,
}

/// Result of a trigram index query with adaptive threshold information.
#[derive(Debug, Clone, Default)]
pub struct TrigramQueryResult {
    pub candidates: Vec<u64>,
    pub use_index: bool,
    pub estimated_ratio: f64,
}

impl TrigramIndex {
    pub fn new(column_name: String) -> Self {
        Self {
            trigrams: RwLock::new(HashMap::new()),
            bigrams: RwLock::new(HashMap::new()),
            unigrams: RwLock::new(HashMap::new()),
            column_name,
            total_rows: AtomicU64::new(0),
            total_trigrams: AtomicU64::new(0),
            posting_size_sum: AtomicU64::new(0),
            max_posting_size: AtomicU64::new(0),
        }
    }

    /// Returns statistics about this index for query planning.
    pub fn stats(&self) -> TrigramStats {
        let total_trigrams = self.total_trigrams.load(Ordering::Relaxed);
        let posting_count = self.trigrams.read().len() as u64;
        TrigramStats {
            total_rows: self.total_rows.load(Ordering::Relaxed),
            total_trigrams,
            avg_posting_list_size: if posting_count > 0 {
                self.posting_size_sum.load(Ordering::Relaxed) as f64 / posting_count as f64
            } else {
                0.0
            },
            max_posting_list_size: self.max_posting_size.load(Ordering::Relaxed),
        }
    }

    /// Returns true if using the trigram index would be beneficial for the given candidate count.
    /// Falls back to full scan if candidates are too many (common patterns like "ing", "tion", etc.)
    pub fn should_use_index(&self, candidate_count: u64) -> bool {
        if candidate_count < MIN_CANDIDATES_TO_USE_INDEX {
            return false;
        }
        let total = self.total_rows.load(Ordering::Relaxed);
        if total == 0 {
            return candidate_count > 0;
        }
        let ratio = candidate_count as f64 / total as f64;
        ratio < DEFAULT_CANDIDATE_THRESHOLD_RATIO
    }

    /// Estimate how many candidates a pattern would produce.
    /// Returns (min_candidates, max_candidates, is_common) based on trigram frequency.
    pub fn estimate_candidates(&self, pattern: &str) -> (u64, u64, bool) {
        let pattern_bytes = pattern.as_bytes();
        if pattern_bytes.len() >= 3 {
            let trigrams = Self::extract_trigrams(pattern);
            if trigrams.is_empty() {
                return (0, 0, false);
            }
            let map = self.trigrams.read();
            let mut min_candidates = u64::MAX;
            let mut max_candidates = 0u64;
            let mut is_common = false;

            for tri in &trigrams {
                if let Some(list) = map.get(tri) {
                    let len = list.len() as u64;
                    min_candidates = min_candidates.min(len);
                    max_candidates = max_candidates.max(len);
                    if len > self.total_rows.load(Ordering::Relaxed) / 2 {
                        is_common = true;
                    }
                } else {
                    return (0, 0, false);
                }
            }
            (min_candidates, max_candidates, is_common)
        } else if pattern_bytes.len() == 2 {
            let key = [pattern_bytes[0], pattern_bytes[1]];
            let map = self.bigrams.read();
            if let Some(list) = map.get(&key) {
                let len = list.len() as u64;
                (len, len, len > self.total_rows.load(Ordering::Relaxed) / 2)
            } else {
                (0, 0, false)
            }
        } else {
            let key = pattern_bytes[0];
            let map = self.unigrams.read();
            if let Some(list) = map.get(&key) {
                let len = list.len() as u64;
                (len, len, len > self.total_rows.load(Ordering::Relaxed) / 2)
            } else {
                (0, 0, false)
            }
        }
    }

    /// Extract trigrams from a string (lowercased for case-insensitive indexing is NOT done;
    /// we index the raw bytes to preserve case-sensitive CONTAINS semantics).
    fn extract_trigrams(s: &str) -> Vec<[u8; 3]> {
        let bytes = s.as_bytes();
        if bytes.len() < 3 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(bytes.len() - 2);
        for window in bytes.windows(3) {
            result.push([window[0], window[1], window[2]]);
        }
        result
    }

    fn extract_bigrams(s: &str) -> Vec<[u8; 2]> {
        let bytes = s.as_bytes();
        if bytes.len() < 2 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(bytes.len() - 1);
        for window in bytes.windows(2) {
            result.push([window[0], window[1]]);
        }
        result
    }

    fn extract_unigrams(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    /// Insert a single value into the index.
    /// Posting lists are maintained in sorted order for binary_search correctness
    /// in `intersect_sorted_lists`. The list.last() fast-path catches the common
    /// case of monotonically increasing row_ids, with binary_search as the
    /// authoritative dedup check for out-of-order inserts.
    pub fn insert(&self, row_id: u64, value: &str) {
        {
            let trigrams = Self::extract_trigrams(value);
            let mut map = self.trigrams.write();
            for tri in trigrams {
                let list = map.entry(tri).or_insert_with(Vec::new);
                if list.last() != Some(&row_id) {
                    if let Err(pos) = list.binary_search(&row_id) {
                        list.insert(pos, row_id);
                    }
                }
            }
        }
        {
            let bigrams = Self::extract_bigrams(value);
            let mut map = self.bigrams.write();
            for bi in bigrams {
                let list = map.entry(bi).or_insert_with(Vec::new);
                if list.last() != Some(&row_id) {
                    if let Err(pos) = list.binary_search(&row_id) {
                        list.insert(pos, row_id);
                    }
                }
            }
        }
        {
            let unigrams = Self::extract_unigrams(value);
            let mut map = self.unigrams.write();
            for u in unigrams {
                let list = map.entry(u).or_insert_with(Vec::new);
                if list.last() != Some(&row_id) {
                    if let Err(pos) = list.binary_search(&row_id) {
                        list.insert(pos, row_id);
                    }
                }
            }
        }
    }

    /// Batch insert multiple (row_id, value) pairs.
    /// Ensures all posting lists remain sorted for binary search operations.
    /// Updates index statistics for adaptive query planning.
    /// Uses binary_search for authoritative dedup since lists are sorted at
    /// the start of each batch (from prior insert/insert_batch calls).
    pub fn insert_batch(&self, entries: &[(u64, &str)]) {
        let entry_count = entries.len() as u64;
        let mut trigram_count = 0u64;
        let mut bigram_count = 0u64;
        let mut unigram_count = 0u64;

        let mut tri_map = self.trigrams.write();
        let mut bi_map = self.bigrams.write();
        let mut uni_map = self.unigrams.write();

        for &(row_id, value) in entries {
            let tris = Self::extract_trigrams(value);
            trigram_count += tris.len() as u64;
            for tri in tris {
                let list = tri_map.entry(tri).or_insert_with(Vec::new);
                if !list.contains(&row_id) {
                    list.push(row_id);
                }
            }

            let bis = Self::extract_bigrams(value);
            bigram_count += bis.len() as u64;
            for bi in bis {
                let list = bi_map.entry(bi).or_insert_with(Vec::new);
                if !list.contains(&row_id) {
                    list.push(row_id);
                }
            }

            let unis = Self::extract_unigrams(value);
            unigram_count += unis.len() as u64;
            for u in unis {
                let list = uni_map.entry(u).or_insert_with(Vec::new);
                if !list.contains(&row_id) {
                    list.push(row_id);
                }
            }
        }

        for list in tri_map.values_mut() {
            list.sort_unstable();
            let size = list.len() as u64;
            self.posting_size_sum.fetch_add(size, Ordering::Relaxed);
            self.max_posting_size.fetch_max(size, Ordering::Relaxed);
        }
        drop(tri_map);

        for list in bi_map.values_mut() {
            list.sort_unstable();
        }
        drop(bi_map);

        for list in uni_map.values_mut() {
            list.sort_unstable();
        }

        self.total_rows.fetch_add(entry_count, Ordering::Relaxed);
        self.total_trigrams
            .fetch_add(trigram_count, Ordering::Relaxed);
    }

    /// Query the index with adaptive threshold check.
    ///
    /// Returns a result indicating:
    /// - `use_index = true`: Use candidates from trigram index for filtering
    /// - `use_index = false`: Full table scan would be faster (too many candidates)
    ///
    /// This method automatically determines whether the trigram index would be
    /// beneficial based on:
    /// 1. Small table heuristic: tables < 1K rows always use full scan
    /// 2. Common pattern check: patterns matching >50% of rows skip index
    /// 3. Short pattern penalty: 1-2 char patterns in tables > 10K rows skip index
    /// 4. Adaptive threshold: stricter ratio (30%) for large tables (> 100K rows)
    pub fn query_with_adaptive_threshold(&self, pattern: &str) -> Option<TrigramQueryResult> {
        if pattern.is_empty() {
            return None;
        }

        let pattern_bytes = pattern.as_bytes();
        let total_rows = self.total_rows.load(Ordering::Relaxed);

        // Early exit: small table - full scan always faster than trigram intersection
        if total_rows < SMALL_TABLE_ROW_COUNT {
            return Some(TrigramQueryResult {
                candidates: vec![],
                use_index: false,
                estimated_ratio: 1.0,
            });
        }

        // Check if pattern is common (any trigram/bigram/unigram matches >50% of rows)
        let (_, _, is_common) = self.estimate_candidates(pattern);
        if is_common {
            return Some(TrigramQueryResult {
                candidates: vec![],
                use_index: false,
                estimated_ratio: 1.0,
            });
        }

        // Short pattern penalty: 1-2 char patterns in larger tables
        // Trigram intersection overhead exceeds full scan cost
        if pattern_bytes.len() <= 2 && total_rows > SHORT_PATTERN_TABLE_SIZE {
            return Some(TrigramQueryResult {
                candidates: vec![],
                use_index: false,
                estimated_ratio: 1.0,
            });
        }

        // Determine adaptive threshold based on table size
        let threshold = if total_rows > LARGE_TABLE_ROW_COUNT {
            LARGE_TABLE_THRESHOLD_RATIO
        } else {
            DEFAULT_CANDIDATE_THRESHOLD_RATIO
        };

        if pattern_bytes.len() >= 3 {
            let trigrams = Self::extract_trigrams(pattern);
            if trigrams.is_empty() {
                return None;
            }

            let map = self.trigrams.read();
            let mut posting_lists: Vec<&Vec<u64>> = Vec::new();

            for tri in &trigrams {
                match map.get(tri) {
                    Some(list) => {
                        posting_lists.push(list);
                    }
                    None => return Some(TrigramQueryResult::default()),
                }
            }

            let candidates = Self::intersect_sorted_lists(&posting_lists);
            let candidate_count = candidates.len() as u64;
            let ratio = if total_rows > 0 {
                candidate_count as f64 / total_rows as f64
            } else {
                0.0
            };

            let use_index = candidate_count >= MIN_CANDIDATES_TO_USE_INDEX && ratio < threshold;

            Some(TrigramQueryResult {
                candidates,
                use_index,
                estimated_ratio: ratio,
            })
        } else if pattern_bytes.len() == 2 {
            let key = [pattern_bytes[0], pattern_bytes[1]];
            let map = self.bigrams.read();
            match map.get(&key) {
                Some(list) => {
                    let candidate_count = list.len() as u64;
                    let ratio = if total_rows > 0 {
                        candidate_count as f64 / total_rows as f64
                    } else {
                        0.0
                    };
                    let use_index =
                        candidate_count >= MIN_CANDIDATES_TO_USE_INDEX && ratio < threshold;
                    Some(TrigramQueryResult {
                        candidates: list.clone(),
                        use_index,
                        estimated_ratio: ratio,
                    })
                }
                None => Some(TrigramQueryResult::default()),
            }
        } else {
            let key = pattern_bytes[0];
            let map = self.unigrams.read();
            match map.get(&key) {
                Some(list) => {
                    let candidate_count = list.len() as u64;
                    let ratio = if total_rows > 0 {
                        candidate_count as f64 / total_rows as f64
                    } else {
                        0.0
                    };
                    let use_index =
                        candidate_count >= MIN_CANDIDATES_TO_USE_INDEX && ratio < threshold;
                    Some(TrigramQueryResult {
                        candidates: list.clone(),
                        use_index,
                        estimated_ratio: ratio,
                    })
                }
                None => Some(TrigramQueryResult::default()),
            }
        }
    }

    /// Query the index for candidate row IDs that *might* contain the pattern.
    /// Returns None if the index cannot help (e.g. empty pattern) — caller should fall back to scan.
    /// Returns Some(vec) with candidate row IDs that need verification via str::contains().
    pub fn query(&self, pattern: &str) -> Option<Vec<u64>> {
        if pattern.is_empty() {
            return None; // Empty pattern matches everything — can't optimize
        }

        let pattern_bytes = pattern.as_bytes();

        if pattern_bytes.len() >= 3 {
            // Use trigram intersection
            let trigrams = Self::extract_trigrams(pattern);
            if trigrams.is_empty() {
                return None;
            }
            let map = self.trigrams.read();
            let mut posting_lists: Vec<&Vec<u64>> = Vec::new();
            for tri in &trigrams {
                match map.get(tri) {
                    Some(list) => posting_lists.push(list),
                    None => return Some(Vec::new()), // Trigram not in index → no matches
                }
            }
            // Intersect all posting lists (they are sorted)
            Some(Self::intersect_sorted_lists(&posting_lists))
        } else if pattern_bytes.len() == 2 {
            // Use bigram
            let key = [pattern_bytes[0], pattern_bytes[1]];
            let map = self.bigrams.read();
            match map.get(&key) {
                Some(list) => Some(list.clone()),
                None => Some(Vec::new()),
            }
        } else {
            // Single char — use unigram
            let key = pattern_bytes[0];
            let map = self.unigrams.read();
            match map.get(&key) {
                Some(list) => Some(list.clone()),
                None => Some(Vec::new()),
            }
        }
    }

    /// Intersect multiple sorted posting lists.
    /// Uses the smallest list as the driver and binary-searches in the others.
    fn intersect_sorted_lists(lists: &[&Vec<u64>]) -> Vec<u64> {
        if lists.is_empty() {
            return Vec::new();
        }
        if lists.len() == 1 {
            return lists[0].clone();
        }

        // Find shortest list to drive the intersection
        let mut shortest_idx = 0;
        let mut shortest_len = lists[0].len();
        for (i, list) in lists.iter().enumerate().skip(1) {
            if list.len() < shortest_len {
                shortest_len = list.len();
                shortest_idx = i;
            }
        }

        let driver = lists[shortest_idx];
        let mut result = Vec::with_capacity(shortest_len);

        'outer: for &id in driver {
            for (i, list) in lists.iter().enumerate() {
                if i == shortest_idx {
                    continue;
                }
                if list.binary_search(&id).is_err() {
                    continue 'outer;
                }
            }
            result.push(id);
        }

        result
    }

    /// Returns the number of unique trigrams indexed.
    pub fn trigram_count(&self) -> usize {
        self.trigrams.read().len()
    }

    /// Returns the total number of postings across all trigrams.
    pub fn total_postings(&self) -> usize {
        self.trigrams
            .read()
            .values()
            .map(|v| v.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigram_extraction() {
        let tris = TrigramIndex::extract_trigrams("hello");
        assert_eq!(tris.len(), 3); // "hel", "ell", "llo"
        assert_eq!(&tris[0], b"hel");
        assert_eq!(&tris[1], b"ell");
        assert_eq!(&tris[2], b"llo");
    }

    #[test]
    fn test_short_string() {
        let tris = TrigramIndex::extract_trigrams("hi");
        assert!(tris.is_empty());
    }

    #[test]
    fn test_insert_and_query() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "compression_utils");
        idx.insert(1, "parser_module");
        idx.insert(2, "decompressor");

        let candidates = idx.query("compress").expect("internal invariant violated");
        assert_eq!(candidates, vec![0, 2]);
    }

    #[test]
    fn test_query_no_match() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "hello");
        idx.insert(1, "world");

        let candidates = idx.query("xyz").expect("internal invariant violated");
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_single_char_query() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "hello");
        idx.insert(1, "world");

        let candidates = idx.query("o").expect("internal invariant violated");
        assert_eq!(candidates, vec![0, 1]); // both contain 'o'
    }

    #[test]
    fn test_two_char_query() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "hello");
        idx.insert(1, "world");
        idx.insert(2, "help");

        let candidates = idx.query("he").expect("internal invariant violated");
        assert_eq!(candidates, vec![0, 2]); // "hello" and "help"
    }

    #[test]
    fn test_empty_pattern() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "hello");

        let result = idx.query("");
        assert!(result.is_none()); // Cannot optimize empty pattern
    }

    #[test]
    fn test_batch_insert() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert_batch(&[
            (0, "compression_utils"),
            (1, "parser_module"),
            (2, "decompressor"),
            (3, "compression_engine"),
        ]);

        let candidates = idx.query("compress").expect("internal invariant violated");
        assert_eq!(candidates, vec![0, 2, 3]);
    }

    #[test]
    fn test_special_chars() {
        let idx = TrigramIndex::new("email".to_string());
        idx.insert(0, "user@example.com");
        idx.insert(1, "noemail");

        // Single char query for '@'
        let candidates = idx.query("@").expect("internal invariant violated");
        assert_eq!(candidates, vec![0]);

        // Trigram query
        let candidates = idx.query("@ex").expect("internal invariant violated");
        assert_eq!(candidates, vec![0]);
    }

    #[test]
    fn test_case_sensitive() {
        let idx = TrigramIndex::new("name".to_string());
        idx.insert(0, "HelloWorld");
        idx.insert(1, "helloworld");

        let candidates = idx.query("Hello").expect("internal invariant violated");
        assert_eq!(candidates, vec![0]); // Only uppercase match
    }

    #[test]
    fn test_large_scale() {
        let idx = TrigramIndex::new("name".to_string());
        for i in 0..10_000u64 {
            idx.insert(i, &format!("function_{}", i));
        }
        // All 10K contain "function"
        let candidates = idx.query("function").expect("internal invariant violated");
        assert_eq!(candidates.len(), 10_000);

        // Query for a unique suffix - "xyz_99999" doesn't exist, so should return empty
        let candidates = idx.query("xyz_99999").expect("internal invariant violated");
        assert!(
            candidates.is_empty(),
            "Non-existent pattern should return empty"
        );

        // Query for "abc" - not a trigram that exists in "function_N" patterns
        // since "abc" doesn't appear in any indexed strings
        let candidates = idx.query("abc").expect("internal invariant violated");
        assert!(
            candidates.is_empty(),
            "Pattern not in any string should return empty"
        );

        // The trigram index returns candidates based on trigram matching,
        // verification (str::contains) happens at query execution time
    }
}
