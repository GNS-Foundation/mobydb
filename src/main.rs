/// mobydbd — MobyDB Server Binary
///
/// Commands:
///   mobydbd serve          -- start HTTP server
///   mobydbd stats          -- print database statistics
///   mobydbd seal --epoch N -- seal an epoch
///   mobydbd demo           -- write first signed breadcrumb (Rome, Res-7)

use anyhow::Result;
use clap::{Parser, Subcommand};
use mobydb_server::{router, ServerState};
use mobydb_storage::MobyStore;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(
    name    = "mobydbd",
    about   = "MobyDB — The Geospatial-Native Database | Built on GEP Protocol",
    version = env!("CARGO_PKG_VERSION"),
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Serve {
        #[arg(short, long, default_value = "./mobydata")]
        data: String,
        #[arg(short, long, env = "PORT", default_value = "7474")]
        port: u16,
        #[arg(short, long, default_value = "info")]
        log: String,
    },
    Stats {
        #[arg(short, long, default_value = "./mobydata")]
        data: String,
    },
    Seal {
        #[arg(short, long, default_value = "./mobydata")]
        data: String,
        #[arg(short, long)]
        epoch: u64,
    },
    Demo {
        #[arg(short, long, default_value = "http://localhost:7474")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { data, log, .. } => {
            init_tracing(&log);
            let port: u16 = std::env::var("PORT")
                .unwrap_or_else(|_| "7474".to_string())
                .parse()
                .expect("PORT must be a valid u16");
            info!("🐋  MobyDB starting...");
            info!("   Data:    {}", data);
            info!("   Port:    {} (from $PORT)", port);
            info!("   GEP genesis: {}", mobydb_core::GEP_GENESIS_HASH);
            let store = MobyStore::open(&data)?;
            let state = Arc::new(ServerState { store });
            let app   = router(state);
            let addr  = format!("0.0.0.0:{}", port);
            info!("🐋  MobyDB listening on {}", addr);
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            axum::serve(listener, app).await?;
        }

        Commands::Stats { data } => {
            init_tracing("info");
            let store = MobyStore::open(&data)?;
            println!("🐋  MobyDB Statistics");
            println!("   Records (approx): {}", store.approx_record_count());
            let epochs = store.sealed_epochs()?;
            println!("   Sealed epochs:    {:?}", epochs);
            println!("   GEP genesis:      {}", mobydb_core::GEP_GENESIS_HASH);
        }

        Commands::Seal { data, epoch } => {
            init_tracing("info");
            let store  = MobyStore::open(&data)?;
            let engine = mobydb_merkle::EpochEngine::new(&store);
            let root   = engine.seal_epoch(epoch)?;
            println!("🐋  Epoch {} sealed", epoch);
            println!("   Root hash:    {}", root.root_hex());
            println!("   Records:      {}", root.record_count);
            if let Some(prev) = root.prev_hex() {
                println!("   Prev epoch:   {}", prev);
            }
        }

        Commands::Demo { server } => {
            init_tracing("warn");
            run_demo(&server).await?;
        }
    }

    Ok(())
}

async fn run_demo(server: &str) -> Result<()> {
    use ed25519_dalek::{Signer, SigningKey};
    use h3o::{LatLng, Resolution};
    use mobydb_core::{CollectionType, MobyPayload, MobyRecord, SpacetimeAddress, TrustTier};
    use rand::rngs::OsRng;

    println!();
    println!("🐋  MobyDB Demo — First Signed Breadcrumb");
    println!("   Location:  Rome, Italy (41.9028, 12.4964)");
    println!("   Cell:      H3 Res-7  |  Epoch: 1");
    println!();

    let signing_key = SigningKey::generate(&mut OsRng);
    let pk_bytes: [u8; 32] = signing_key.verifying_key().to_bytes();
    let pk_hex = hex::encode(pk_bytes);
    println!("   Public key:  {}", pk_hex);

    let latlng  = LatLng::new(41.9028, 12.4964)?;
    let cell    = latlng.to_cell(Resolution::Seven);
    let cell_id = u64::from(cell);
    println!("   H3 cell:     {:x}", cell_id);

    let written_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let address = SpacetimeAddress::new(cell_id, 1u64, pk_bytes);
    let payload = MobyPayload {
        collection_type: CollectionType::Breadcrumb,
        payload_type: "gns/breadcrumb".to_string(),
        data: serde_json::json!({
            "lat": 41.9028, "lng": 12.4964,
            "handle": "@camiloayerbe",
            "city": "Rome", "country": "IT",
            "note": "MobyDB first signed breadcrumb — April 2, 2026"
        }),
    };

    let temp = MobyRecord {
        address: address.clone(), payload: payload.clone(),
        signature: [0u8; 64], trust_tier: TrustTier::Navigator,
        written_at_ms,
    };
    let sig: [u8; 64] = signing_key.sign(&temp.canonical_bytes()).to_bytes();
    println!("   Signature:   {}...", hex::encode(&sig[..8]));

    let record = MobyRecord {
        address, payload, signature: sig,
        trust_tier: TrustTier::Navigator, written_at_ms,
    };

    println!();
    println!("   Sending to MobyDB...");
    let client   = reqwest::Client::new();
    let response = client.post(format!("{}/write", server))
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&record)?)
        .send().await?;

    let ok = response.status().is_success();
    let txt = response.text().await?;

    if ok {
        println!();
        println!("   ✅  Written to MobyDB");
        println!("   Response:  {}", txt);
        println!();
        println!("   Sealing epoch 1...");
        let seal = client.post(format!("{}/epoch/1/seal", server))
            .send().await?.text().await?;
        println!("   Sealed:    {}", seal);
        println!();
        println!("   Query your record:");
        println!("   curl \"http://localhost:7474/near/{:x}?rings=0&epoch=1\"", cell_id);
        println!();
        println!("   🐋  The whale has left its first trail.");
    } else {
        println!("   ❌  {}", txt);
    }

    Ok(())
}

fn init_tracing(level: &str) {
    let filter = format!(
        "mobydbd={level},mobydb_core={level},mobydb_storage={level},\
         mobydb_merkle={level},mobydb_query={level},mobydb_server={level}"
    );
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(tracing_subscriber::fmt::layer())
        .init();
}
