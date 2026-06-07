use lightning_core::fusion::FusionApp as CoreFusion;

use crate::connection::Connection;
use crate::types::Result;

pub use lightning_core::fusion::ConnectedDirection;
pub use lightning_core::fusion::ModuleCohesion;

/// Fusion integration layer for LightningDB.
///
/// Provides graph-based code analysis tools: node search, path finding,
/// architecture cohesion metrics, PageRank computation, D3 graph export,
/// and observation storage/retrieval.
///
/// Uses the `CodeNode` and `Observation` node tables, which are created
/// by the Fusion indexer pipeline.
///
/// # Example
///
/// ```no_run
/// use lightning::prelude::*;
///
/// let db = Database::open("path/to/db").unwrap();
/// let conn = db.connect();
///
/// let nodes = Fusion::find_node_by_name(&conn, "main").unwrap();
/// let paths = Fusion::find_paths(&conn, &nodes[0], "target_id", &[]).unwrap();
/// ```
pub struct Fusion;

impl Fusion {
    /// Initialize Fusion-specific schema.
    pub fn init_schema(conn: &Connection) -> Result<()> {
        CoreFusion::init_fusion_schema(conn.inner())
    }

    // ── Node Queries ─────────────────────────────────────────────────

    /// Find CodeNode IDs by exact name match.
    pub fn find_node_by_name(conn: &Connection, name: &str) -> Result<Vec<String>> {
        CoreFusion::find_node_by_name(conn.inner(), name)
    }

    /// Find paths between two nodes (direct connections).
    pub fn find_paths(
        conn: &Connection,
        source_id: &str,
        target_id: &str,
        edge_types: &[&str],
    ) -> Result<Vec<String>> {
        CoreFusion::find_paths(conn.inner(), source_id, target_id, edge_types)
    }

    /// Find connected node IDs by edge traversal.
    ///
    /// Traverse edges from `node_id`. Filter by `edge_types` (if non-empty)
    /// and restrict to the given `direction` (incoming or outgoing).
    pub fn find_connected_nodes(
        conn: &Connection,
        node_id: &str,
        edge_types: &[&str],
        direction: ConnectedDirection,
    ) -> Result<Vec<String>> {
        CoreFusion::find_connected_nodes(conn.inner(), node_id, edge_types, direction)
    }

    /// Look up (id, name, node_type) tuples for a list of node IDs.
    pub fn lookup_node_names(
        conn: &Connection,
        ids: &[String],
    ) -> Result<Vec<(String, String, String)>> {
        CoreFusion::lookup_node_names(conn.inner(), ids)
    }

    // ── Observations ─────────────────────────────────────────────────

    /// Store an observation about the codebase.
    ///
    /// Observations persist findings from analysis agents (e.g., bugs,
    /// architectural smells, documentation gaps).
    pub fn add_observation(
        conn: &Connection,
        id: &str,
        content: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        CoreFusion::add_observation(conn.inner(), id, content, parent_id)
    }

    /// Get recent observation content strings, ordered by creation time.
    pub fn get_recent_observations(conn: &Connection, limit: usize) -> Result<Vec<String>> {
        CoreFusion::get_recent_observations(conn.inner(), limit)
    }

    // ── Architecture Analysis ────────────────────────────────────────

    /// Compute architecture cohesion metrics from the module graph.
    ///
    /// Each module gets internal/external edge counts and a cohesion score
    /// (higher = more self-contained).
    pub fn compute_architecture_cohesion(
        conn: &Connection,
    ) -> Result<Vec<ModuleCohesion>> {
        CoreFusion::compute_architecture_cohesion(conn.inner())
    }

    /// Recompute PageRank scores for all CodeNodes.
    ///
    /// Iterative PageRank with damping factor 0.85, convergence threshold 0.0001,
    /// max 100 iterations. Results are stored on `n.page_rank` for each node.
    pub fn materialize_pagerank(conn: &Connection) -> Result<()> {
        CoreFusion::materialize_pagerank(conn.inner())
    }

    /// Export the entire graph as D3-compatible JSON.
    ///
    /// Returns a JSON string with `nodes` and `links` arrays suitable for
    /// visualization with D3.js force-directed graph layouts.
    pub fn export_to_d3_json(conn: &Connection) -> Result<String> {
        CoreFusion::export_to_d3_json(conn.inner())
    }
}


