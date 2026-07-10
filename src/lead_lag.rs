use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct PriceSample {
    pub at: Instant,
    pub price: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadStatus {
    Insufficient,
    Weak,
    Candidate,
}

#[derive(Clone, Copy, Debug)]
pub struct LeadLag {
    pub lead_ms: i64,
    pub peak_correlation: f64,
    pub zero_lag_correlation: Option<f64>,
    pub correlation_gain: Option<f64>,
    pub samples: usize,
    /// Time span the oracle history currently covers. Warm-up is gated on this,
    /// not on `samples`, because the oracle only *changes value* a handful of
    /// times a minute — far too few to fill a sample-count target.
    pub coverage: Duration,
    pub status: LeadStatus,
}

impl Default for LeadLag {
    fn default() -> Self {
        Self {
            lead_ms: 0,
            peak_correlation: 0.0,
            zero_lag_correlation: None,
            correlation_gain: None,
            samples: 0,
            coverage: Duration::ZERO,
            status: LeadStatus::Insufficient,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LeadLagConfig {
    pub window: Duration,
    pub max_quote_age: Duration,
    pub minimum_lag: Duration,
    pub maximum_lag: Duration,
    pub step: Duration,
    pub minimum_history: Duration,
    pub minimum_samples: usize,
    pub minimum_correlation: f64,
    pub minimum_gain: f64,
}

impl LeadLagConfig {
    /// How much oracle history must accumulate before a reading is reported.
    ///
    /// Warm-up is gated on elapsed coverage rather than a sample count. The
    /// oracle only *changes value* a handful of times a minute — measured live,
    /// roughly 20 times over a 5-minute window for BTC — so any target phrased
    /// as "N matched intervals" near that ceiling is unreachable and leaves the
    /// monitor stuck on "warming up" forever. Two minutes of history is enough
    /// to hold a meaningful correlation while completing reliably.
    pub const MINIMUM_HISTORY: Duration = Duration::from_secs(120);

    /// Correlation floor: the fewest distinct oracle moves that can yield a
    /// non-degenerate correlation. This is a guard for a nearly-flat oracle, not
    /// the warm-up gate — [`Self::MINIMUM_HISTORY`] is. An active market clears
    /// it well within the warm-up window.
    pub const MINIMUM_SAMPLES: usize = 5;

    pub const fn market_making(max_quote_age: Duration) -> Self {
        Self {
            window: Duration::from_secs(300),
            max_quote_age,
            minimum_lag: Duration::from_secs(3),
            maximum_lag: Duration::from_secs(6),
            step: Duration::from_millis(100),
            minimum_history: Self::MINIMUM_HISTORY,
            minimum_samples: Self::MINIMUM_SAMPLES,
            minimum_correlation: 0.30,
            minimum_gain: 0.05,
        }
    }
}

pub fn estimate(
    local: &[PriceSample],
    official: &[PriceSample],
    now: Instant,
    config: LeadLagConfig,
) -> LeadLag {
    let official = official_window(official, now, config.window);
    if official.len() < 2 || config.step.is_zero() {
        return LeadLag::default();
    }
    // Elapsed span of oracle history. This drives warm-up: it grows with wall
    // time even when the oracle rarely changes value, so the monitor always
    // leaves "warming up" once enough history exists.
    let coverage = official
        .last()
        .zip(official.first())
        .map(|(last, first)| last.at.saturating_duration_since(first.at))
        .unwrap_or_default();

    let min_lag_ms = config.minimum_lag.as_millis() as i64;
    let max_lag_ms = config.maximum_lag.as_millis() as i64;
    let step_ms = config.step.as_millis() as i64;
    let mut best: Option<(i64, f64, usize)> = None;
    let mut zero_lag_correlation = None;

    for lag_ms in (-min_lag_ms..=max_lag_ms).step_by(step_ms as usize) {
        let pairs = return_pairs(local, official, lag_ms, config.max_quote_age);
        let Some(correlation) = correlation(&pairs) else {
            continue;
        };
        if lag_ms == 0 {
            zero_lag_correlation = Some(correlation);
        }
        let candidate = (lag_ms, correlation, pairs.len());
        if best.is_none_or(|current| {
            candidate.1 > current.1
                || (candidate.1 == current.1
                    && candidate.0.unsigned_abs() < current.0.unsigned_abs())
        }) {
            best = Some(candidate);
        }
    }

    let Some((lead_ms, peak_correlation, samples)) = best else {
        return LeadLag::default();
    };
    let correlation_gain = zero_lag_correlation.map(|zero| peak_correlation - zero);
    // Warm-up is time-gated. The sample floor only rules out a nearly-flat
    // oracle that cannot support a correlation at all.
    let status = if coverage < config.minimum_history || samples < config.minimum_samples {
        LeadStatus::Insufficient
    } else if lead_ms > 0
        && peak_correlation >= config.minimum_correlation
        && correlation_gain.is_some_and(|gain| gain >= config.minimum_gain)
    {
        LeadStatus::Candidate
    } else {
        LeadStatus::Weak
    };
    LeadLag {
        lead_ms,
        peak_correlation,
        zero_lag_correlation,
        correlation_gain,
        samples,
        coverage,
        status,
    }
}

fn official_window(samples: &[PriceSample], now: Instant, window: Duration) -> &[PriceSample] {
    let start = now.checked_sub(window).unwrap_or(now);
    let first = samples.partition_point(|sample| sample.at < start);
    &samples[first..]
}

fn return_pairs(
    local: &[PriceSample],
    official: &[PriceSample],
    lead_ms: i64,
    max_quote_age: Duration,
) -> Vec<(f64, f64)> {
    official
        .windows(2)
        .filter_map(|interval| {
            let previous = interval[0];
            let current = interval[1];
            let local_previous = local_at(local, shift(previous.at, lead_ms)?, max_quote_age)?;
            let local_current = local_at(local, shift(current.at, lead_ms)?, max_quote_age)?;
            let official_return = log_return(previous.price, current.price)?;
            let local_return = log_return(local_previous.price, local_current.price)?;
            (official_return.abs() > f64::EPSILON).then_some((local_return, official_return))
        })
        .collect()
}

fn shift(at: Instant, lead_ms: i64) -> Option<Instant> {
    if lead_ms >= 0 {
        at.checked_sub(Duration::from_millis(lead_ms as u64))
    } else {
        at.checked_add(Duration::from_millis(lead_ms.unsigned_abs()))
    }
}

fn local_at(samples: &[PriceSample], at: Instant, max_quote_age: Duration) -> Option<PriceSample> {
    let index = samples
        .partition_point(|sample| sample.at <= at)
        .checked_sub(1)?;
    let sample = samples[index];
    // The local history is forward-filled: a sample is recorded only when the
    // price changes, so it stays the live price until the next recorded change.
    // If a later sample exists, `at` falls strictly inside a held interval and
    // the value is current regardless of how long it was held. Only at the
    // trailing edge — no later sample — do we require freshness, so a stalled
    // feed does not get extrapolated forward indefinitely.
    let held_across_a_later_change = index + 1 < samples.len();
    let fresh_at_the_edge = at.saturating_duration_since(sample.at) <= max_quote_age;
    (held_across_a_later_change || fresh_at_the_edge).then_some(sample)
}

fn log_return(previous: f64, current: f64) -> Option<f64> {
    (previous.is_finite() && current.is_finite() && previous > 0.0 && current > 0.0)
        .then(|| (current / previous).ln())
}

fn correlation(pairs: &[(f64, f64)]) -> Option<f64> {
    if pairs.len() < 2 {
        return None;
    }
    let count = pairs.len() as f64;
    let mean_x = pairs.iter().map(|(x, _)| x).sum::<f64>() / count;
    let mean_y = pairs.iter().map(|(_, y)| y).sum::<f64>() / count;
    let (covariance, variance_x, variance_y) = pairs.iter().fold(
        (0.0, 0.0, 0.0),
        |(covariance, variance_x, variance_y), (x, y)| {
            let dx = x - mean_x;
            let dy = y - mean_y;
            (
                covariance + dx * dy,
                variance_x + dx * dx,
                variance_y + dy * dy,
            )
        },
    );
    let denominator = (variance_x * variance_y).sqrt();
    (denominator > f64::EPSILON).then_some(covariance / denominator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_a_known_local_lead() {
        let start = Instant::now() - Duration::from_secs(360);
        let local: Vec<_> = (0..3600)
            .map(|index| {
                let x = index as f64;
                PriceSample {
                    at: start + Duration::from_millis(index * 100),
                    price: 100.0 + (x * 0.071).sin() + (x * 0.019).cos(),
                }
            })
            .collect();
        let official: Vec<_> = (30..3600)
            .step_by(30)
            .map(|index| PriceSample {
                at: start + Duration::from_millis(index * 100),
                price: local[index as usize - 5].price,
            })
            .collect();
        let result = estimate(
            &local,
            &official,
            start + Duration::from_secs(359),
            LeadLagConfig::market_making(Duration::from_millis(100)),
        );
        assert_eq!(result.lead_ms, 500);
        assert!(result.peak_correlation > 0.99);
        assert_eq!(result.status, LeadStatus::Candidate);
    }

    // The monitor stores the local oracle as a forward-filled series: a sample
    // is only recorded when the price *changes*, so a steady price leaves gaps
    // longer than `max_quote_age`. The estimator must treat a held value as the
    // current price across such a gap, otherwise interior intervals are dropped
    // and the matched-sample count is capped well below what the data supports —
    // one of the reasons the monitor was stuck on "warming up" forever.
    #[test]
    fn counts_intervals_across_held_prices() {
        let window = Duration::from_secs(300);
        let start = Instant::now() - window - Duration::from_secs(10);
        let config = LeadLagConfig::market_making(Duration::from_millis(1500));

        // Local oracle as a forward-filled staircase: it changes only every
        // 2.5 s, so consecutive recorded samples are 2.5 s apart — beyond the
        // 1500 ms freshness window. The value is still the live price in between.
        let local: Vec<PriceSample> = (0..130)
            .map(|step| {
                let seconds = step as f64 * 2.5;
                PriceSample {
                    at: start + Duration::from_secs_f64(seconds),
                    price: 100.0 + (step as f64 * 0.6).sin() + (step as f64 * 0.17).cos(),
                }
            })
            .collect();

        // Official publishes every 3 s; each value is distinct, so there are
        // ~100 intervals available in the window.
        let official: Vec<PriceSample> = (0..100)
            .map(|step| {
                let seconds = step as f64 * 3.0 + 0.5;
                PriceSample {
                    at: start + Duration::from_secs_f64(seconds),
                    price: 100.0 + step as f64 * 0.01,
                }
            })
            .collect();

        let now = start + window;
        let result = estimate(&local, &official, now, config);

        // Almost every interval should match: the held local value is valid
        // across the 2 s gaps. Before the fix the freshness gate dropped the
        // ones sampled more than 1500 ms after the last recorded change.
        assert!(
            result.samples >= 90,
            "held prices dropped too many intervals: only {} of ~99 matched",
            result.samples
        );
    }

    // Live measurement: the oracle only *changes value* ~20 times over a
    // 5-minute window (it publishes every ~3 s but mostly repeats). Warm-up is
    // gated on elapsed coverage, not on that move count, so it must complete
    // even though `samples` stays far below any move-count target — while a
    // history that has not yet spanned `minimum_history` must stay in warm-up.
    #[test]
    fn warmup_is_gated_on_coverage_not_move_count() {
        let window = Duration::from_secs(300);
        let start = Instant::now() - window - Duration::from_secs(10);
        let config = LeadLagConfig::market_making(Duration::from_millis(1500));

        // Dense, always-fresh local oracle that leads by ~1 s.
        let local: Vec<PriceSample> = (0..1500)
            .map(|tick| {
                let seconds = tick as f64 * 0.2;
                PriceSample {
                    at: start + Duration::from_secs_f64(seconds),
                    price: 100.0 + (seconds * 0.05).sin() + 0.3 * (seconds * 0.31).cos(),
                }
            })
            .collect();

        // A sticky oracle: it publishes every 3 s but the value only *moves* ~20
        // times across the whole window (the rest of the publications repeat and
        // are de-duplicated away). Model that directly as 20 distinct steps
        // spread across the window, tracking the local price 1 s earlier.
        let official: Vec<PriceSample> = (0..20)
            .map(|move_index| {
                let seconds = move_index as f64 * 15.0; // one move every ~15 s
                let src = seconds - 1.0;
                PriceSample {
                    at: start + Duration::from_secs_f64(seconds),
                    price: 100.0 + (src * 0.05).sin() + 0.3 * (src * 0.31).cos(),
                }
            })
            .collect();

        // Full window: coverage ~285 s, far past the warm-up gate. The oracle
        // moved far fewer than the old sample-count targets, yet warm-up
        // completes because it is time-gated.
        assert!(
            official.len() < 30,
            "test setup expected a sticky oracle, got {} moves",
            official.len()
        );
        let full = estimate(&local, &official, start + window, config);
        assert!(
            full.samples < 30,
            "sanity: move count should be small, got {}",
            full.samples
        );
        assert_ne!(
            full.status,
            LeadStatus::Insufficient,
            "warm-up never completes: coverage {:?}, {} moves",
            full.coverage,
            full.samples
        );

        // Only the first 90 s of the same feed: coverage is below
        // `minimum_history`, so it must still be warming up regardless of how
        // the correlation looks.
        let early_cut = start + Duration::from_secs(90);
        let early: Vec<PriceSample> = official
            .iter()
            .copied()
            .filter(|s| s.at <= early_cut)
            .collect();
        let early_result = estimate(&local, &early, early_cut, config);
        assert!(early_result.coverage < config.minimum_history);
        assert_eq!(early_result.status, LeadStatus::Insufficient);
    }
}
