/// generate_iot — Synthetic IoT telemetry generator for MobyDB benchmark
///
/// Generates sensor readings for the Italian electricity grid scenario.
/// Writes directly to MobyDB (via MobyStore) and exports CSV for PostGIS.
///
/// Usage:
///   cargo run --release --bin generate_iot -- \
///     --sensors 1000 --epochs 100 --mobydb-path ./benchmark_data \
///     --postgis-csv ./data/iot_1m.csv

use anyhow::Result;
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey};
use h3o::{LatLng, Resolution};
use mobydb_core::{CollectionType, MobyPayload, MobyRecord, SpacetimeAddress, TrustTier};
use mobydb_storage::MobyStore;
use rand::Rng;
use std::io::Write;
use std::time::Instant;
use tracing::info;

// Italian grid cities — sensors clustered around these hubs
// Palermo is the benchmark reference point and gets the most sensors
const CITIES: &[(f64, f64, &str, f64)] = &[
    // (lat, lng, name, weight)  — weight determines sensor share
    (38.1157, 13.3615, "Palermo",   0.30),  // 30% — benchmark reference
    (41.9028, 12.4964, "Rome",      0.15),
    (45.4642, 9.1900,  "Milan",     0.10),
    (40.8518, 14.2681, "Naples",    0.10),
    (43.7696, 11.2558, "Florence",  0.05),
    (45.4384, 12.3267, "Venice",    0.05),
    (44.4949, 11.3426, "Bologna",   0.05),
    (40.6401, 15.8056, "Potenza",   0.05),  // Basilicata grid
    (39.3088, 16.3463, "Cosenza",   0.05),  // Calabria grid
    (37.5079, 15.0830, "Catania",   0.05),  // Sicily east
    (47.0667, 12.1333, "Brennero",  0.025), // Alpine border
    (36.9271, 14.7322, "Ragusa",    0.025), // Sicily south
];

#[derive(Parser, Debug)]
#[command(name = "generate_iot", about = "Generate synthetic IoT sensor data")]
struct Args {
    /// Number of sensors (each gets its own Ed25519 keypair)
    #[arg(long, default_value = "1000")]
    sensors: u32,

    /// Number of GEP epochs
    #[arg(long, default_value = "100")]
    epochs: u32,

    /// H3 resolution (7 = ~5km² hexagons)
    #[arg(long, default_value = "7")]
    resolution: u8,

    /// MobyDB data directory (writes directly to RocksDB)
    #[arg(long, default_value = "./benchmark_data")]
    mobydb_path: String,

    /// PostGIS CSV output path
    #[arg(long, default_value = "./data/iot_data.csv")]
    postgis_csv: String,

    /// Write batch size
    #[arg(long, default_value = "1000")]
    batch_size: usize,
}

struct Sensor {
    signing_key: SigningKey,
    public_key:  [u8; 32],
    lat:         f64,
    lng:         f64,
    h3_cell:     u64,
    sensor_id:   uuid::Uuid,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let args = Args::parse();
    let total_records = args.sensors as u64 * args.epochs as u64;
    let res = Resolution::try_from(args.resolution)
        .map_err(|_| anyhow::anyhow!("Invalid H3 resolution: {}", args.resolution))?;

    info!(
        "Generating {} records ({} sensors × {} epochs, H3 res {})",
        total_records, args.sensors, args.epochs, args.resolution
    );

    // ── Generate sensors clustered around Italian grid cities ──
    let mut rng = rand::thread_rng();
    let mut sensors: Vec<Sensor> = Vec::with_capacity(args.sensors as usize);

    for &(city_lat, city_lng, city_name, weight) in CITIES {
        let city_sensors = (args.sensors as f64 * weight).round() as u32;
        for _ in 0..city_sensors {
            let signing_key = SigningKey::generate(&mut rng);
            let public_key = signing_key.verifying_key().to_bytes();
            // Scatter within ~10km of city center
            let lat = city_lat + rng.gen_range(-0.08..0.08);
            let lng = city_lng + rng.gen_range(-0.08..0.08);
            let ll = LatLng::new(lat, lng).unwrap();
            let h3_cell = u64::from(ll.to_cell(res));
            let sensor_id = uuid::Uuid::new_v4();
            sensors.push(Sensor { signing_key, public_key, lat, lng, h3_cell, sensor_id });
        }
        if city_sensors > 0 {
            info!("  {} — {} sensors near ({:.4}, {:.4})", city_name, city_sensors, city_lat, city_lng);
        }
    }

    // Fill any remainder (rounding) with random Palermo sensors
    while sensors.len() < args.sensors as usize {
        let signing_key = SigningKey::generate(&mut rng);
        let public_key = signing_key.verifying_key().to_bytes();
        let lat = 38.1157 + rng.gen_range(-0.08..0.08);
        let lng = 13.3615 + rng.gen_range(-0.08..0.08);
        let ll = LatLng::new(lat, lng).unwrap();
        let h3_cell = u64::from(ll.to_cell(res));
        let sensor_id = uuid::Uuid::new_v4();
        sensors.push(Sensor { signing_key, public_key, lat, lng, h3_cell, sensor_id });
    }

    info!("{} sensors placed across {} Italian grid cities", sensors.len(), CITIES.len());

    // ── Open MobyDB ─────────────────────────────────────────
    std::fs::create_dir_all(&args.mobydb_path)?;
    let store = MobyStore::open(&args.mobydb_path)?;
    info!("MobyDB opened at {}", args.mobydb_path);

    // ── Open CSV for PostGIS ────────────────────────────────
    if let Some(parent) = std::path::Path::new(&args.postgis_csv).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut csv = std::fs::File::create(&args.postgis_csv)?;
    writeln!(csv, "sensor_id,lat,lng,epoch,value,unit,recorded_at")?;
    info!("PostGIS CSV: {}", args.postgis_csv);

    // ── Generate records ────────────────────────────────────
    let start = Instant::now();
    let mut moby_batch: Vec<MobyRecord> = Vec::with_capacity(args.batch_size);
    let mut moby_written = 0u64;
    let mut csv_written = 0u64;

    for epoch in 1..=args.epochs as u64 {
        for sensor in &sensors {
            // Simulated voltage reading: 220kV ± 15%
            let value: f64 = 220.0 + rng.gen_range(-33.0..33.0);
            let written_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // ── MobyDB record ───────────────────────────────
            let address = SpacetimeAddress::new(sensor.h3_cell, epoch, sensor.public_key);
            let payload = MobyPayload {
                collection_type: CollectionType::Telemetry,
                payload_type:    "iot/voltage".to_string(),
                data: serde_json::json!({
                    "sensor_id": sensor.sensor_id.to_string(),
                    "value": value,
                    "unit": "kV",
                    "lat": sensor.lat,
                    "lng": sensor.lng,
                }),
            };

            // Sign the record
            let temp = MobyRecord {
                address: address.clone(),
                payload: payload.clone(),
                signature: [0u8; 64],
                trust_tier: TrustTier::Certified,
                written_at_ms,
            };
            let sig = sensor.signing_key.sign(&temp.canonical_bytes()).to_bytes();

            let record = MobyRecord {
                address, payload, signature: sig,
                trust_tier: TrustTier::Certified, written_at_ms,
            };

            moby_batch.push(record);

            // Flush batch when full
            if moby_batch.len() >= args.batch_size {
                store.write_batch(&moby_batch)?;
                moby_written += moby_batch.len() as u64;
                moby_batch.clear();
            }

            // ── PostGIS CSV row ─────────────────────────────
            let ts = chrono::Utc::now().to_rfc3339();
            writeln!(
                csv, "{},{},{},{},{},{},{}",
                sensor.sensor_id, sensor.lat, sensor.lng, epoch, value, "kV", ts
            )?;
            csv_written += 1;
        }

        // Progress every 10 epochs
        if epoch % 10 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = moby_written as f64 / elapsed;
            info!(
                "Epoch {}/{}: {} MobyDB records ({:.0} rec/s)",
                epoch, args.epochs, moby_written, rate
            );
        }
    }

    // Flush remaining
    if !moby_batch.is_empty() {
        store.write_batch(&moby_batch)?;
        moby_written += moby_batch.len() as u64;
    }

    let elapsed = start.elapsed();
    let rate = moby_written as f64 / elapsed.as_secs_f64();

    info!("──────────────────────────────────────────");
    info!("Generation complete in {:.2}s", elapsed.as_secs_f64());
    info!("MobyDB:  {} records written ({:.0} rec/s)", moby_written, rate);
    info!("PostGIS: {} CSV rows written → {}", csv_written, args.postgis_csv);
    info!(
        "Import CSV to PostGIS:\n  COPY sensor_readings(sensor_id,lat,lng,epoch,value,unit,recorded_at) \
         FROM '{}' CSV HEADER;",
        std::fs::canonicalize(&args.postgis_csv)
            .unwrap_or_default()
            .display()
    );
    info!("Then: VACUUM ANALYZE sensor_readings;");

    Ok(())
}
