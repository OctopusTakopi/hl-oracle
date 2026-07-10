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
    pub minimum_samples: usize,
    pub minimum_correlation: f64,
    pub minimum_gain: f64,
}

impl LeadLagConfig {
    pub const fn market_making(max_quote_age: Duration) -> Self {
        Self {
            window: Duration::from_secs(300),
            max_quote_age,
            minimum_lag: Duration::from_secs(3),
            maximum_lag: Duration::from_secs(6),
            step: Duration::from_millis(100),
            minimum_samples: 60,
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
    let status = if samples < config.minimum_samples {
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
    (at.saturating_duration_since(sample.at) <= max_quote_age).then_some(sample)
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
}
