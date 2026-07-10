//! High-level embedding API.
//!
//! [`OracleBuilder`] configures a feed and [`OracleFeed`] runs it: it spawns the
//! exchange WebSocket readers in the background and lets the caller pull oracle
//! values as they change.
//!
//! ```no_run
//! use hl_oracle::OracleBuilder;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let mut feed = OracleBuilder::new(["BTC", "ETH"]).spawn()?;
//! while let Some(update) = feed.recv().await {
//!     let coin = feed.coin(update.asset).unwrap_or("?");
//!     println!(
//!         "{coin}: local {:.2} official {:?} ({} sources)",
//!         update.comparison.local, update.comparison.official, update.comparison.sources,
//!     );
//! }
//! # Ok(())
//! # }
//! ```

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use kanal::{AsyncReceiver, bounded_async};
use tokio::task::JoinHandle;

use crate::{
    config::build_assets,
    model::{FeedStats, Update},
    oracle::{Comparison, OracleBook},
    sources::{self, Asset},
    ws,
};

/// Configuration for an [`OracleFeed`]. Defaults match the `hl-oracle` binary:
/// a 1500 ms quote window, a 6000 ms official window, and a queue of 8192.
#[derive(Clone, Debug)]
pub struct OracleBuilder {
    coins: Vec<String>,
    hl_spot: Vec<String>,
    include_external_for: Vec<String>,
    max_quote_age: Duration,
    max_official_age: Duration,
    queue_capacity: usize,
}

impl OracleBuilder {
    /// Starts a configuration for the given perp coins, e.g. `["BTC", "ETH"]`.
    pub fn new<I, S>(coins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            coins: coins.into_iter().map(Into::into).collect(),
            hl_spot: Vec::new(),
            include_external_for: Vec::new(),
            max_quote_age: Duration::from_millis(1_500),
            max_official_age: Duration::from_millis(6_000),
            queue_capacity: 8_192,
        }
    }

    /// Maps a coin to a Hyperliquid spot market, e.g. `("HYPE", "HYPE/USDC")`.
    pub fn hl_spot(mut self, coin: impl Into<String>, spot: impl Into<String>) -> Self {
        self.hl_spot
            .push(format!("{}={}", coin.into(), spot.into()));
        self
    }

    /// Adds a raw `COIN=SPOT_COIN` mapping, as accepted on the command line.
    pub fn hl_spot_mapping(mut self, mapping: impl Into<String>) -> Self {
        self.hl_spot.push(mapping.into());
        self
    }

    /// Enables external venues for a Hyperliquid-primary spot coin (e.g. `HYPE`).
    pub fn include_external_for(mut self, coin: impl Into<String>) -> Self {
        self.include_external_for.push(coin.into());
        self
    }

    /// Freshness window for exchange and Hyperliquid-spot books.
    pub fn max_quote_age(mut self, age: Duration) -> Self {
        self.max_quote_age = age;
        self
    }

    /// Freshness window for Hyperliquid's `oraclePx`, published every 3 s.
    pub fn max_official_age(mut self, age: Duration) -> Self {
        self.max_official_age = age;
        self
    }

    /// Capacity of the bounded queue between the readers and the consumer.
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Spawns the feed. Must be called from within a Tokio runtime.
    pub fn spawn(self) -> Result<OracleFeed> {
        OracleFeed::spawn(self)
    }
}

/// A change in a coin's oracle comparison, returned by [`OracleFeed::recv`].
#[derive(Clone, Copy, Debug)]
pub struct OracleUpdate {
    /// Index into [`OracleFeed::assets`]; also usable with [`OracleFeed::coin`].
    pub asset: usize,
    /// The recomputed local-versus-official comparison.
    pub comparison: Comparison,
}

/// A running oracle feed.
///
/// The exchange readers run on background Tokio tasks; the caller drives
/// aggregation by awaiting [`recv`](Self::recv). Dropping the feed aborts the
/// readers.
pub struct OracleFeed {
    rx: AsyncReceiver<Update>,
    book: OracleBook,
    assets: Arc<Vec<Asset>>,
    stats: Arc<FeedStats>,
    last_emitted: Vec<Option<Comparison>>,
    source_task: JoinHandle<()>,
}

impl OracleFeed {
    /// Builds the assets, opens the queue, and spawns the reader tasks.
    pub fn spawn(builder: OracleBuilder) -> Result<Self> {
        if builder.queue_capacity == 0 {
            bail!("queue_capacity must be greater than zero");
        }
        let assets = Arc::new(build_assets(
            &builder.coins,
            &builder.hl_spot,
            &builder.include_external_for,
        )?);
        let (tx, rx) = bounded_async(builder.queue_capacity);
        let stats = Arc::new(FeedStats::default());
        let tls = ws::tls_config();
        let source_assets = Arc::clone(&assets);
        let source_stats = Arc::clone(&stats);
        let source_task =
            tokio::spawn(
                async move { sources::run_all(source_assets, tx, source_stats, tls).await },
            );
        let count = assets.len();
        Ok(Self {
            rx,
            book: OracleBook::new(count, builder.max_quote_age, builder.max_official_age),
            assets,
            stats,
            last_emitted: vec![None; count],
            source_task,
        })
    }

    /// Applies incoming quotes and official updates until a coin's comparison
    /// changes, then returns it. Returns `None` once the feed has shut down.
    pub async fn recv(&mut self) -> Option<OracleUpdate> {
        loop {
            let update = self.rx.recv().await.ok()?;
            let asset = match update {
                Update::Quote(quote) => {
                    let asset = quote.asset as usize;
                    if !self.book.apply_quote(quote) {
                        continue;
                    }
                    asset
                }
                Update::Official(official) => {
                    let asset = official.asset as usize;
                    if !self.book.apply_official(official) {
                        continue;
                    }
                    asset
                }
            };
            if let Some(comparison) = self.book.comparison(asset, Instant::now())
                && self.last_emitted[asset] != Some(comparison)
            {
                self.last_emitted[asset] = Some(comparison);
                return Some(OracleUpdate { asset, comparison });
            }
        }
    }

    /// The current comparison for a coin, computed on demand. `None` before the
    /// coin has any fresh source, or for an out-of-range index.
    pub fn latest(&self, asset: usize) -> Option<Comparison> {
        if asset >= self.assets.len() {
            return None;
        }
        self.book.comparison(asset, Instant::now())
    }

    /// The perp coin name for an asset index.
    pub fn coin(&self, asset: usize) -> Option<&str> {
        self.assets.get(asset).map(|asset| asset.coin.as_str())
    }

    /// The configured assets, in index order.
    pub fn assets(&self) -> &[Asset] {
        &self.assets
    }

    /// Total updates dropped because the queue was full.
    pub fn dropped_updates(&self) -> u64 {
        self.stats.dropped_updates()
    }
}

impl Drop for OracleFeed {
    fn drop(&mut self) {
        self.source_task.abort();
    }
}
