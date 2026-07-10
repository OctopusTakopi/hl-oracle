use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

pub const VENUE_COUNT: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Venue {
    Binance = 0,
    Okx = 1,
    Bybit = 2,
    Kraken = 3,
    Kucoin = 4,
    Gate = 5,
    Mexc = 6,
    Hyperliquid = 7,
}

impl Venue {
    pub const ALL: [Self; VENUE_COUNT] = [
        Self::Binance,
        Self::Okx,
        Self::Bybit,
        Self::Kraken,
        Self::Kucoin,
        Self::Gate,
        Self::Mexc,
        Self::Hyperliquid,
    ];

    pub const fn weight(self) -> u8 {
        [3, 2, 2, 1, 1, 1, 1, 1][self as usize]
    }

    pub const fn name(self) -> &'static str {
        [
            "binance",
            "okx",
            "bybit",
            "kraken",
            "kucoin",
            "gate",
            "mexc",
            "hyperliquid_spot",
        ][self as usize]
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QuoteUpdate {
    pub venue: Venue,
    pub asset: u16,
    pub bid: f64,
    pub ask: f64,
    pub received_at: Instant,
    /// Local receipt wall-clock minus the exchange's send timestamp, in
    /// milliseconds. `None` when the venue does not timestamp its messages
    /// (e.g. Binance bookTicker). Meaningful only with an NTP-synced clock.
    pub latency_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
pub struct OfficialUpdate {
    pub asset: u16,
    pub oracle: f64,
    pub received_at: Instant,
}

#[derive(Clone, Copy, Debug)]
pub enum Update {
    Quote(QuoteUpdate),
    Official(OfficialUpdate),
}

#[derive(Default)]
pub struct FeedStats {
    dropped_updates: AtomicU64,
}

impl FeedStats {
    pub fn record_drop(&self) {
        self.dropped_updates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dropped_updates(&self) -> u64 {
        self.dropped_updates.load(Ordering::Relaxed)
    }
}
