/// MobyDB Server — HTTP API
///
/// REST API for MobyDB. Wire protocol v1.
///
/// POST /write              — write a signed record
/// POST /write/batch        — write multiple records atomically  
/// GET  /near/:cell         — proximity query (?rings=2&epoch=9)
/// GET  /record/:cell/:epoch/:pubkey — read single record
/// POST /epoch/:epoch/seal  — seal an epoch, compute Merkle root
/// GET  /epoch/:epoch/root  — read a sealed epoch root
/// GET  /proof/:cell/:epoch/:pubkey  — generate Merkle proof
/// GET  /stats              — database statistics

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use mobydb_core::{MobyRecord, SpacetimeAddress, TrustTier};
use mobydb_merkle::EpochEngine;
use mobydb_query::{MobyQuery};
use mobydb_storage::MobyStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

// ── Server State ──────────────────────────────────────────────────────────────

pub struct ServerState {
    pub store: MobyStore,
}

pub type AppState = Arc<ServerState>;

// ── Request / Response Types ──────────────────────────────────────────────────

#[derive(Serialize)]
struct ApiResponse<T: Serialize> {
    success: bool,
    data:    Option<T>,
    error:   Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    fn ok(data: T) -> Json<Self> {
        Json(Self { success: true, data: Some(data), error: None })
    }
    fn err(msg: &str) -> Json<ApiResponse<()>> {
        Json(ApiResponse { success: false, data: None, error: Some(msg.to_string()) })
    }
}

#[derive(Deserialize)]
struct NearParams {
    rings:       Option<u32>,
    epoch:       Option<u64>,
    epoch_start: Option<u64>,
    epoch_end:   Option<u64>,
    tier:        Option<String>,
    limit:       Option<usize>,
}

#[derive(Serialize)]
struct WriteResponse {
    key_hex:  String,
    h3_cell:  u64,
    epoch:    u64,
}

#[derive(Serialize)]
struct NearResponse {
    records:       Vec<MobyRecord>,
    count:         usize,
    cells_scanned: usize,
}

#[derive(Serialize)]
struct StatsResponse {
    approx_record_count: u64,
    sealed_epochs:       Vec<u64>,
    genesis:             String,
}

// ── Routes ────────────────────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    Router::new()
        // Write
        .route("/write",             post(write_record))
        .route("/write/batch",       post(write_batch))
        // Query
        .route("/near/:cell",        get(near_query))
        .route("/record/:cell/:epoch/:pubkey", get(read_record))
        // Epoch
        .route("/epoch/:epoch/seal", post(seal_epoch))
        .route("/epoch/:epoch/root", get(epoch_root))
        // Proof
        .route("/proof/:cell/:epoch/:pubkey", get(generate_proof))
        // Meta
        .route("/stats",             get(stats))
        .route("/health",            get(health))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn write_record(
    State(s): State<AppState>,
    Json(record): Json<MobyRecord>,
) -> Result<Json<ApiResponse<WriteResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    match s.store.write(&record) {
        Ok(key) => {
            Ok(ApiResponse::ok(WriteResponse {
                key_hex:  hex::encode(key.as_bytes()),
                h3_cell:  record.address.h3_cell,
                epoch:    record.address.epoch,
            }))
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            ApiResponse::<()>::err(&e.to_string()),
        )),
    }
}

async fn write_batch(
    State(s): State<AppState>,
    Json(records): Json<Vec<MobyRecord>>,
) -> Result<Json<ApiResponse<serde_json::Value>>, (StatusCode, Json<ApiResponse<()>>)> {
    let count = records.len();
    match s.store.write_batch(&records) {
        Ok(_) => Ok(ApiResponse::ok(serde_json::json!({ "written": count }))),
        Err(e) => Err((StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn near_query(
    Path(cell_hex): Path<String>,
    Query(params): Query<NearParams>,
    State(s): State<AppState>,
) -> Result<Json<ApiResponse<NearResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    // Parse cell (accepts hex string or u64)
    let cell = parse_cell(&cell_hex)
        .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;

    let rings       = params.rings.unwrap_or(0).min(10);
    let epoch_start = params.epoch_start.or(params.epoch).unwrap_or(0);
    let epoch_end   = params.epoch_end.or(params.epoch).unwrap_or(u64::MAX);

    let mut query = MobyQuery::near(cell, rings).during(epoch_start, epoch_end);

    if let Some(ref tier_str) = params.tier {
        let tier = parse_tier(tier_str)
            .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;
        query = query.with_tier(tier);
    }

    if let Some(limit) = params.limit {
        query = query.limit(limit);
    }

    match query.execute(&s.store) {
        Ok(result) => Ok(ApiResponse::ok(NearResponse {
            count:         result.count,
            cells_scanned: result.cells_scanned,
            records:       result.records,
        })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn read_record(
    Path((cell_hex, epoch, pubkey_hex)): Path<(String, u64, String)>,
    State(s): State<AppState>,
) -> Result<Json<ApiResponse<MobyRecord>>, (StatusCode, Json<ApiResponse<()>>)> {
    let cell = parse_cell(&cell_hex)
        .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;
    let pubkey = parse_pubkey(&pubkey_hex)
        .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;

    let addr = SpacetimeAddress::new(cell, epoch, pubkey);

    match s.store.read(&addr) {
        Ok(record) => Ok(ApiResponse::ok(record)),
        Err(mobydb_core::MobyError::NotFound) => Err((
            StatusCode::NOT_FOUND,
            ApiResponse::<()>::err("record not found"),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn seal_epoch(
    Path(epoch): Path<u64>,
    State(s): State<AppState>,
) -> Result<Json<ApiResponse<mobydb_core::EpochRoot>>, (StatusCode, Json<ApiResponse<()>>)> {
    let engine = EpochEngine::new(&s.store);
    match engine.seal_epoch(epoch) {
        Ok(root) => {
            info!("Epoch {} sealed via API: {}", epoch, root.root_hex());
            Ok(ApiResponse::ok(root))
        }
        Err(e) => Err((StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn epoch_root(
    Path(epoch): Path<u64>,
    State(s): State<AppState>,
) -> Result<Json<ApiResponse<mobydb_core::EpochRoot>>, (StatusCode, Json<ApiResponse<()>>)> {
    match s.store.read_epoch_root(epoch) {
        Ok(root) => Ok(ApiResponse::ok(root)),
        Err(mobydb_core::MobyError::NotFound) => Err((
            StatusCode::NOT_FOUND,
            ApiResponse::<()>::err(&format!("epoch {} not yet sealed", epoch)),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn generate_proof(
    Path((cell_hex, epoch, pubkey_hex)): Path<(String, u64, String)>,
    State(s): State<AppState>,
) -> Result<Json<ApiResponse<mobydb_core::MerkleProof>>, (StatusCode, Json<ApiResponse<()>>)> {
    let cell = parse_cell(&cell_hex)
        .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;
    let pubkey = parse_pubkey(&pubkey_hex)
        .map_err(|e| (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)))?;

    let addr   = SpacetimeAddress::new(cell, epoch, pubkey);
    let engine = EpochEngine::new(&s.store);

    match engine.generate_proof(&addr) {
        Ok(proof) => Ok(ApiResponse::ok(proof)),
        Err(e) => Err((StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e.to_string()))),
    }
}

async fn stats(
    State(s): State<AppState>,
) -> Json<ApiResponse<StatsResponse>> {
    let sealed = s.store.sealed_epochs().unwrap_or_default();
    ApiResponse::ok(StatsResponse {
        approx_record_count: s.store.approx_record_count(),
        sealed_epochs: sealed,
        genesis: mobydb_core::GEP_GENESIS_HASH.to_string(),
    })
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "engine": "MobyDB",
        "protocol": "GEP"
    }))
}

// ── Parse Helpers ─────────────────────────────────────────────────────────────

fn parse_cell(s: &str) -> Result<u64, String> {
    if s.starts_with("0x") {
        u64::from_str_radix(s.trim_start_matches("0x"), 16)
            .map_err(|e| format!("invalid cell: {}", e))
    } else {
        // Try parsing as decimal first, fallback to hex only if it contains letters
        s.parse::<u64>().or_else(|_| {
            u64::from_str_radix(s, 16)
        }).map_err(|e| format!("invalid cell: {}", e))
    }
}

fn parse_pubkey(s: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(s).map_err(|e| format!("invalid pubkey hex: {}", e))?;
    bytes.try_into().map_err(|_| "pubkey must be 32 bytes (64 hex chars)".to_string())
}

fn parse_tier(s: &str) -> Result<TrustTier, String> {
    match s.to_lowercase().as_str() {
        "seedling"    => Ok(TrustTier::Seedling),
        "explorer"    => Ok(TrustTier::Explorer),
        "navigator"   => Ok(TrustTier::Navigator),
        "trailblazer" => Ok(TrustTier::Trailblazer),
        "sovereign"   => Ok(TrustTier::Sovereign),
        "certified"   => Ok(TrustTier::Certified),
        _ => Err(format!("unknown trust tier: {}", s)),
    }
}
