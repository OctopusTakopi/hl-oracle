//! Minimal embedding example: print each coin's local oracle against
//! Hyperliquid's published oracle as they change.
//!
//! ```sh
//! cargo run --example feed -- BTC ETH SOL
//! ```

use hl_oracle::OracleBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let coins: Vec<String> = std::env::args().skip(1).collect();
    let coins = if coins.is_empty() {
        vec!["BTC".to_owned()]
    } else {
        coins
    };

    let mut feed = OracleBuilder::new(coins).spawn()?;
    while let Some(update) = feed.recv().await {
        let coin = feed.coin(update.asset).unwrap_or("?");
        let comparison = update.comparison;
        match (comparison.official, comparison.difference_bps) {
            (Some(official), Some(bps)) => println!(
                "{coin:6} local {:.2}  hl {:.2}  {:+.3} bps  ({} sources)",
                comparison.local, official, bps, comparison.sources,
            ),
            _ => println!(
                "{coin:6} local {:.2}  hl -  ({} sources)",
                comparison.local, comparison.sources,
            ),
        }
    }
    Ok(())
}
