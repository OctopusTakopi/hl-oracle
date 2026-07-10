use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use hl_oracle::{Asset, Comparison, OracleBuilder, Venue};
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Low-latency local replica of the Hyperliquid spot oracle"
)]
struct Args {
    /// Perp coins to price, for example: BTC ETH SOL.
    #[arg(required = true)]
    coins: Vec<String>,

    /// Hyperliquid spot mapping, repeated as COIN=SPOT_COIN. HYPE=HYPE/USDC is enabled by default.
    #[arg(long = "hl-spot", value_name = "COIN=SPOT_COIN")]
    hyperliquid_spot: Vec<String>,

    /// Enable external sources for a Hyperliquid-primary spot coin after its eligibility condition is met.
    #[arg(long = "include-external-for", value_name = "COIN")]
    include_external_for: Vec<String>,

    /// Maximum accepted age of a source quote before it is excluded from the weighted median.
    #[arg(long, default_value_t = 1_500)]
    max_quote_age_ms: u64,

    /// Maximum accepted age of Hyperliquid's official oracle. It updates every three seconds.
    #[arg(long, default_value_t = 6_000)]
    max_official_age_ms: u64,

    /// Bounded update queue capacity. New updates are dropped under sustained consumer overload.
    #[arg(long, default_value_t = 8_192)]
    queue_capacity: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // stdout carries the JSON Lines feed; logs must go to stderr to keep it clean.
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let mut builder = OracleBuilder::new(args.coins)
        .max_quote_age(Duration::from_millis(args.max_quote_age_ms))
        .max_official_age(Duration::from_millis(args.max_official_age_ms))
        .queue_capacity(args.queue_capacity);
    for mapping in args.hyperliquid_spot {
        builder = builder.hl_spot_mapping(mapping);
    }
    for coin in args.include_external_for {
        builder = builder.include_external_for(coin);
    }
    let mut feed = builder.spawn()?;

    info!(
        coins = feed.assets().len(),
        max_quote_age_ms = args.max_quote_age_ms,
        "oracle feed started"
    );
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            update = feed.recv() => {
                let Some(update) = update else { return Ok(()) };
                emit(
                    &feed.assets()[update.asset],
                    update.comparison,
                    feed.dropped_updates(),
                );
            }
        }
    }
}

fn emit(asset: &Asset, value: Comparison, dropped_updates: u64) {
    let active_sources = Venue::ALL
        .into_iter()
        .filter(|venue| value.active_venues[*venue as usize])
        .map(|venue| format!(r#""{}""#, venue.name()))
        .collect::<Vec<_>>()
        .join(",");
    match value.official {
        Some(official) => println!(
            r#"{{"coin":"{}","local_oracle":{:.10},"hyperliquid_oracle":{:.10},"difference_bps":{:.6},"sources":{},"weight":{},"expected_sources":{},"expected_weight":{},"active_sources":[{}],"dropped_updates":{}}}"#,
            asset.coin,
            value.local,
            official,
            value.difference_bps.unwrap_or_default(),
            value.sources,
            value.total_weight,
            asset.expected_source_count(),
            asset.expected_source_weight(),
            active_sources,
            dropped_updates,
        ),
        None => println!(
            r#"{{"coin":"{}","local_oracle":{:.10},"hyperliquid_oracle":null,"difference_bps":null,"sources":{},"weight":{},"expected_sources":{},"expected_weight":{},"active_sources":[{}],"dropped_updates":{}}}"#,
            asset.coin,
            value.local,
            value.sources,
            value.total_weight,
            asset.expected_source_count(),
            asset.expected_source_weight(),
            active_sources,
            dropped_updates,
        ),
    }
}
