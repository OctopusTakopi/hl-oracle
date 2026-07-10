//! A local reconstruction of Hyperliquid's spot oracle.
//!
//! `hl-oracle` subscribes to top-of-book data from Binance, OKX, Bybit, Kraken,
//! KuCoin, Gate, and MEXC, takes each book's mid, and computes the weighted
//! median Hyperliquid documents for its spot oracle. It also reads Hyperliquid's
//! `activeAssetCtx` so the local value can be compared against the published
//! `oraclePx`.
//!
//! Most callers want the high-level API:
//!
//! ```no_run
//! use hl_oracle::OracleBuilder;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let mut feed = OracleBuilder::new(["BTC", "ETH", "SOL"]).spawn()?;
//! while let Some(update) = feed.recv().await {
//!     println!("{:?}: {:.2}", feed.coin(update.asset), update.comparison.local);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The lower-level pieces ([`sources::run_all`], [`oracle::OracleBook`]) are also
//! public for callers that need the raw update stream, such as the bundled
//! `oracle-monitor`.

pub mod config;
pub mod feed;
pub mod lead_lag;
pub mod model;
pub mod oracle;
pub mod sources;
pub mod ws;

pub use feed::{OracleBuilder, OracleFeed, OracleUpdate};
pub use model::Venue;
pub use oracle::Comparison;
pub use sources::Asset;
