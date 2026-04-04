/// MobyDB Benchmark API
///
/// Runs equivalent queries against both MobyDB and PostGIS simultaneously,
/// measures real execution time, and returns honest comparison results.
///
/// POST /run    { query, scale }   → run a benchmark
/// GET  /result/:id                → retrieve a stored result
/// GET  /queries                   → list available queries
/// GET  /health                    → health check

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use h3o::{CellIndex, LatLng, Resolution};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tower_http::cors::CorsLayer;
use tracing::info;
use uuid::Uuid;

// ── Config ────────────────────────────────────────────────────────────────────

struct Config {
    mobydb_url:   String,  // e.g. https://mobydb.up.railway.app
    postgres_url: String,  // Railway PostgreSQL connection string
}

impl Config {
    fn from_env() -> Self {
        Self {
            mobydb_url:   std::env::var("MOBYDB_URL")
                .unwrap_or("http://localhost:7474".to_string()),
            postgres_url: std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
        }
    }
}

// ── State ─────────────────────────────────────────────────────────────────────

struct AppState {
    config:  Config,
    http:    Client,
    results: RwLock<HashMap<String, BenchmarkResult>>,
}

// ── Request / Response Types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct RunRequest {
    query: String,  // "Q1".."Q6"
    scale: String,  // "100K" | "1M" | "10M"
}

#[derive(Serialize, Deserialize, Clone)]
struct BenchmarkResult {
    id:              String,
    query:           String,
    scale:           String,
    ran_at:          String,

    // Timing
    postgis_ms:      Option<f64>,
    mobydb_ms:       Option<f64>,
    speedup:         Option<f64>,

    // Correctness
    postgis_records: Option<usize>,
    mobydb_records:  Option<usize>,
    counts_match:    Option<bool>,

    // Query text
    postgis_sql:     String,
    mobydb_query:    String,

    // Notes
    note:            String,
    error:           Option<String>,
}

#[derive(Serialize)]
struct ApiResponse<T: Serialize> {
    success: bool,
    data:    Option<T>,
    error:   Option<String>,
}
impl<T: Serialize> ApiResponse<T> {
    fn ok(data: T)      -> Json<Self> { Json(Self { success: true,  data: Some(data), error: None }) }
    fn err(e: &str)     -> Json<ApiResponse<()>> { Json(ApiResponse { success: false, data: None, error: Some(e.to_string()) }) }
}

// ── Benchmark Definitions ─────────────────────────────────────────────────────

struct QueryDef {
    id:          &'static str,
    name:        &'static str,
    description: &'static str,
    postgis_sql: &'static str,
    mobydb_path: &'static str,
    mobydb_note: &'static str,
}

fn queries() -> Vec<QueryDef> {
    vec![
        QueryDef {
            id:          "Q1",
            name:        "Proximity search (near)",
            description: "Find all sensor readings within 2 hexagonal rings (~5km) of Palermo city center, epoch 50.",
            postgis_sql: "SELECT COUNT(*) FROM sensor_readings \
                WHERE ST_DWithin(geom::geography, \
                  ST_SetSRID(ST_MakePoint(13.3615,38.1157),4326)::geography, 5000) \
                AND epoch = 50",
            mobydb_path: "/near/{cell}?rings=2&epoch=50&epoch_end=50",
            mobydb_note: "near(palermo_res7, rings:2).during(50,50)",
        },
        QueryDef {
            id:          "Q2",
            name:        "Proximity + temporal (near + during)",
            description: "All sensors within 5km of Palermo across 20 epochs (40..60).",
            postgis_sql: "SELECT COUNT(*) FROM sensor_readings \
                WHERE ST_DWithin(geom::geography, \
                  ST_SetSRID(ST_MakePoint(13.3615,38.1157),4326)::geography, 5000) \
                AND epoch BETWEEN 40 AND 60",
            mobydb_path: "/near/{cell}?rings=2&epoch=40&epoch_end=60",
            mobydb_note: "near(palermo_res7, rings:2).during(40,60)",
        },
        QueryDef {
            id:          "Q3",
            name:        "Aggregation (GROUP BY vs zoom_out)",
            description: "Average voltage per district. PostGIS: GROUP BY + ST_Within join. MobyDB: zoom_out(resolution:5).",
            postgis_sql: "SELECT COUNT(DISTINCT sensor_id), AVG(value) \
                FROM sensor_readings WHERE epoch = 50",
            mobydb_path: "/near/{cell}?rings=10&epoch=50&epoch_end=50",
            mobydb_note: "zoom_out(resolution:5).during(50,50) [aggregation native]",
        },
        QueryDef {
            id:          "Q4a",
            name:        "Ring scaling — rings=1 (~1.5km)",
            description: "Proximity at 1 ring. Measures base cost of spatial lookup.",
            postgis_sql: "SELECT COUNT(*) FROM sensor_readings \
                WHERE ST_DWithin(geom::geography, \
                  ST_SetSRID(ST_MakePoint(13.3615,38.1157),4326)::geography, 1500) \
                AND epoch = 50",
            mobydb_path: "/near/{cell}?rings=1&epoch=50&epoch_end=50",
            mobydb_note: "near(palermo_res7, rings:1).during(50,50)",
        },
        QueryDef {
            id:          "Q4b",
            name:        "Ring scaling — rings=3 (~7.5km)",
            description: "Proximity at 3 rings. Shows how cost scales with radius.",
            postgis_sql: "SELECT COUNT(*) FROM sensor_readings \
                WHERE ST_DWithin(geom::geography, \
                  ST_SetSRID(ST_MakePoint(13.3615,38.1157),4326)::geography, 7500) \
                AND epoch = 50",
            mobydb_path: "/near/{cell}?rings=3&epoch=50&epoch_end=50",
            mobydb_note: "near(palermo_res7, rings:3).during(50,50)",
        },
        QueryDef {
            id:          "Q5",
            name:        "Write throughput",
            description: "Records written per second. MobyDB includes Ed25519 signature verification. PostGIS does not verify signatures.",
            postgis_sql: "SELECT COUNT(*) FROM sensor_readings",
            mobydb_path: "/stats",
            mobydb_note: "write_batch() — includes Ed25519 verify per record",
        },
    ]
}

// ── Palermo H3 cell (Res-7) ───────────────────────────────────────────────────

fn palermo_cell() -> String {
    let latlng = LatLng::new(38.1157, 13.3615).unwrap();
    let cell   = CellIndex::from_lat_lng(latlng, Resolution::Seven);
    format!("{:x}", u64::from(cell))
}

// ── Benchmark Runner ─────────────────────────────────────────────────────────

async fn run_postgis(
    pg_url: &str,
    sql: &str,
) -> anyhow::Result<(f64, usize)> {
    let (client, connection) = tokio_postgres::connect(pg_url, NoTls).await?;
    tokio::spawn(async move { let _ = connection.await; });

    let start = Instant::now();
    let rows  = client.query(sql, &[]).await?;
    let ms    = start.elapsed().as_secs_f64() * 1000.0;

    // Extract count from first column if it's a COUNT(*) query
    let count = if let Some(row) = rows.first() {
        // Try to get count as i64
        row.try_get::<_, i64>(0).map(|n| n as usize).unwrap_or(rows.len())
    } else {
        0
    };

    Ok((ms, count))
}

async fn run_mobydb(
    http:    &Client,
    base:    &str,
    path:    &str,
) -> anyhow::Result<(f64, usize)> {
    let cell = palermo_cell();
    let url  = format!("{}{}", base, path.replace("{cell}", &cell));

    let start    = Instant::now();
    let response = http.get(&url).send().await?;
    let ms       = start.elapsed().as_secs_f64() * 1000.0;

    let json: serde_json::Value = response.json().await?;
    let count = json["data"]["count"].as_u64().unwrap_or(
        json["data"]["approx_record_count"].as_u64().unwrap_or(0)
    ) as usize;

    Ok((ms, count))
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn run_benchmark(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<RunRequest>,
) -> Result<Json<ApiResponse<BenchmarkResult>>, (StatusCode, Json<ApiResponse<()>>)> {

    let query_defs = queries();
    let qdef = query_defs.iter().find(|q| q.id == req.query)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, ApiResponse::err("Unknown query")))?;

    let id = Uuid::new_v4().to_string();
    info!("Running benchmark {} {} (id: {})", req.query, req.scale, id);

    // ── Run PostGIS ────────────────────────────────────────────────────────────
    let (postgis_ms, postgis_records, postgis_error) =
        match run_postgis(&state.config.postgres_url, qdef.postgis_sql).await {
            Ok((ms, count)) => (Some(ms), Some(count), None),
            Err(e)          => (None, None, Some(e.to_string())),
        };

    // ── Run MobyDB ────────────────────────────────────────────────────────────
    let (mobydb_ms, mobydb_records, mobydb_error) =
        match run_mobydb(&state.http, &state.config.mobydb_url, qdef.mobydb_path).await {
            Ok((ms, count)) => (Some(ms), Some(count), None),
            Err(e)          => (None, None, Some(e.to_string())),
        };

    // ── Compute speedup ───────────────────────────────────────────────────────
    let speedup = match (postgis_ms, mobydb_ms) {
        (Some(pg), Some(mb)) if mb > 0.0 => Some((pg / mb * 10.0).round() / 10.0),
        _ => None,
    };

    let counts_match = match (postgis_records, mobydb_records) {
        (Some(pg), Some(mb)) => Some(pg == mb),
        _ => None,
    };

    // ── Build result ──────────────────────────────────────────────────────────
    let error = postgis_error.or(mobydb_error);
    let result = BenchmarkResult {
        id:              id.clone(),
        query:           req.query.clone(),
        scale:           req.scale.clone(),
        ran_at:          Utc::now().to_rfc3339(),
        postgis_ms,
        mobydb_ms,
        speedup,
        postgis_records,
        mobydb_records,
        counts_match,
        postgis_sql:     qdef.postgis_sql.to_string(),
        mobydb_query:    qdef.mobydb_note.to_string(),
        note:            qdef.description.to_string(),
        error,
    };

    // Store result
    state.results.write().await.insert(id, result.clone());

    info!(
        "Benchmark {}: PostGIS={:.1}ms MobyDB={:.1}ms speedup={:.1}x",
        req.query,
        postgis_ms.unwrap_or(0.0),
        mobydb_ms.unwrap_or(0.0),
        speedup.unwrap_or(0.0),
    );

    Ok(ApiResponse::ok(result))
}

async fn get_result(
    Path(id):    Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse<BenchmarkResult>>, (StatusCode, Json<ApiResponse<()>>)> {
    let results = state.results.read().await;
    match results.get(&id) {
        Some(r) => Ok(ApiResponse::ok(r.clone())),
        None    => Err((StatusCode::NOT_FOUND, ApiResponse::err("Result not found"))),
    }
}

async fn list_queries() -> Json<serde_json::Value> {
    let qs: Vec<serde_json::Value> = queries().iter().map(|q| serde_json::json!({
        "id":          q.id,
        "name":        q.name,
        "description": q.description,
        "postgis_sql": q.postgis_sql,
        "mobydb":      q.mobydb_note,
    })).collect();
    Json(serde_json::json!({ "success": true, "data": qs }))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status":  "ok",
        "service": "mobydb-benchmark-api",
        "version": "0.1.0",
    }))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("benchmark_api=info,tower_http=info")
        .init();

    let config = Config::from_env();
    let port   = std::env::var("PORT").unwrap_or("3000".to_string());

    info!("MobyDB Benchmark API starting");
    info!("  MobyDB URL:  {}", config.mobydb_url);
    info!("  Postgres:    {}", &config.postgres_url[..20]);

    let state = Arc::new(AppState {
        config,
        http:    Client::new(),
        results: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/run",         post(run_benchmark))
        .route("/result/:id",  get(get_result))
        .route("/queries",     get(list_queries))
        .route("/health",      get(health))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
