use std::time::{Duration, Instant};

use crate::model::{OfficialUpdate, QuoteUpdate, VENUE_COUNT, Venue};

#[derive(Clone, Copy, Debug)]
struct Quote {
    mid: f64,
    received_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct Official {
    price: f64,
    received_at: Instant,
}

#[derive(Debug)]
struct AssetState {
    quotes: [Option<Quote>; VENUE_COUNT],
    official: Option<Official>,
}

impl AssetState {
    fn new() -> Self {
        Self {
            quotes: [None; VENUE_COUNT],
            official: None,
        }
    }
}

#[derive(Debug)]
pub struct OracleBook {
    assets: Vec<AssetState>,
    max_quote_age: Duration,
    max_official_age: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Comparison {
    pub local: f64,
    pub official: Option<f64>,
    pub difference_bps: Option<f64>,
    pub sources: u8,
    pub total_weight: u8,
    pub active_venues: [bool; VENUE_COUNT],
}

impl OracleBook {
    pub fn new(asset_count: usize, max_quote_age: Duration, max_official_age: Duration) -> Self {
        Self {
            assets: (0..asset_count).map(|_| AssetState::new()).collect(),
            max_quote_age,
            max_official_age,
        }
    }

    pub fn apply_quote(&mut self, update: QuoteUpdate) -> bool {
        if !valid_bbo(update.bid, update.ask) {
            return false;
        }
        let Some(asset) = self.assets.get_mut(update.asset as usize) else {
            return false;
        };
        asset.quotes[update.venue as usize] = Some(Quote {
            mid: update.bid.midpoint(update.ask),
            received_at: update.received_at,
        });
        true
    }

    pub fn apply_official(&mut self, update: OfficialUpdate) -> bool {
        if !update.oracle.is_finite() || update.oracle <= 0.0 {
            return false;
        }
        let Some(asset) = self.assets.get_mut(update.asset as usize) else {
            return false;
        };
        asset.official = Some(Official {
            price: update.oracle,
            received_at: update.received_at,
        });
        true
    }

    pub fn comparison(&self, asset: usize, now: Instant) -> Option<Comparison> {
        let state = self.assets.get(asset)?;
        let mut values = [(0.0_f64, 0_u8); VENUE_COUNT];
        let mut active_venues = [false; VENUE_COUNT];
        let mut count = 0;
        let mut weight = 0;

        for venue in Venue::ALL {
            if let Some(quote) = state.quotes[venue as usize]
                && now.saturating_duration_since(quote.received_at) <= self.max_quote_age
            {
                values[count] = (quote.mid, venue.weight());
                active_venues[venue as usize] = true;
                count += 1;
                weight += venue.weight();
            }
        }

        let local = weighted_median(&mut values[..count])?;
        let official = state.official.and_then(|value| {
            (now.saturating_duration_since(value.received_at) <= self.max_official_age)
                .then_some(value.price)
        });
        Some(Comparison {
            local,
            official,
            difference_bps: official.map(|value| 10_000.0 * (local / value - 1.0)),
            sources: count as u8,
            total_weight: weight,
            active_venues,
        })
    }
}

fn valid_bbo(bid: f64, ask: f64) -> bool {
    bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid
}

/// Lowest price whose cumulative source weight reaches half of all valid weight.
pub fn weighted_median(values: &mut [(f64, u8)]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    let total: u16 = values.iter().map(|(_, weight)| *weight as u16).sum();
    let threshold = total.div_ceil(2);
    let mut cumulative = 0_u16;
    for (price, weight) in values {
        cumulative += *weight as u16;
        if cumulative >= threshold {
            return Some(*price);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Venue;

    #[test]
    fn weighted_median_uses_documented_weighting() {
        let mut values = [(100.0, 3), (101.0, 2), (102.0, 2), (200.0, 1)];
        assert_eq!(weighted_median(&mut values), Some(101.0));
    }

    #[test]
    fn stale_quotes_are_excluded() {
        let now = Instant::now();
        let mut book = OracleBook::new(1, Duration::from_millis(10), Duration::from_secs(1));
        assert!(book.apply_quote(QuoteUpdate {
            venue: Venue::Binance,
            asset: 0,
            bid: 99.0,
            ask: 101.0,
            received_at: now - Duration::from_secs(1),
            latency_ms: None,
        }));
        assert!(book.apply_quote(QuoteUpdate {
            venue: Venue::Okx,
            asset: 0,
            bid: 109.0,
            ask: 111.0,
            received_at: now,
            latency_ms: None,
        }));
        assert_eq!(book.comparison(0, now).unwrap().local, 110.0);
    }

    #[test]
    fn crossed_quotes_are_rejected() {
        let mut book = OracleBook::new(1, Duration::from_secs(1), Duration::from_secs(1));
        assert!(!book.apply_quote(QuoteUpdate {
            venue: Venue::Binance,
            asset: 0,
            bid: 101.0,
            ask: 100.0,
            received_at: Instant::now(),
            latency_ms: None,
        }));
    }

    #[test]
    fn official_price_has_its_own_freshness_window() {
        let now = Instant::now();
        let mut book = OracleBook::new(1, Duration::from_millis(10), Duration::from_secs(6));
        assert!(book.apply_quote(QuoteUpdate {
            venue: Venue::Binance,
            asset: 0,
            bid: 99.0,
            ask: 101.0,
            received_at: now,
            latency_ms: None,
        }));
        assert!(book.apply_official(OfficialUpdate {
            asset: 0,
            oracle: 100.0,
            received_at: now - Duration::from_secs(3),
        }));
        assert_eq!(book.comparison(0, now).unwrap().official, Some(100.0));
    }
}
