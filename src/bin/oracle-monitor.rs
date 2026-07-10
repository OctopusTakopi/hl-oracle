use std::{
    collections::VecDeque,
    io::{self, stdout},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use hl_oracle::{
    config::build_assets,
    lead_lag::{self, LeadLag, LeadLagConfig, LeadStatus, PriceSample},
    model::{FeedStats, Update, VENUE_COUNT, Venue},
    oracle::{Comparison, OracleBook},
    sources::{self, Asset},
    ws,
};
use kanal::{AsyncReceiver, bounded_async};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    symbols::Marker,
    widgets::{
        Axis, Block, Borders, Cell, Chart, Dataset, GraphType, LegendPosition, Paragraph, Row,
        Table,
    },
};

#[derive(Debug, Parser)]
#[command(version, about = "Live local-oracle versus Hyperliquid oracle monitor")]
struct Args {
    /// Perp coins to monitor, for example: BTC ETH SOL.
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

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum View {
    Table,
    Chart,
}

#[derive(Default)]
struct AssetMetrics {
    local_at: Option<Instant>,
    official_at: Option<Instant>,
    local_updates: u64,
    official_updates: u64,
    diff_samples: u64,
    mean_abs_diff_bps: f64,
    peak_abs_diff_bps: f64,
    local_history: VecDeque<PriceSample>,
    official_history: VecDeque<PriceSample>,
    lead_lag: LeadLag,
    lead_streak: u8,
    last_lead_official: Option<Instant>,
    /// Local receipt time of the last quote from each venue, for staleness.
    source_at: [Option<Instant>; VENUE_COUNT],
    /// Last measured network latency per venue: local receipt wall-clock minus
    /// the exchange send timestamp, in milliseconds. `None` when unavailable.
    source_latency: [Option<i64>; VENUE_COUNT],
}

impl AssetMetrics {
    fn observe_local(&mut self, at: Instant, price: Option<f64>) {
        self.local_at = Some(at);
        self.local_updates += 1;
        if let Some(price) = price
            && self
                .local_history
                .back()
                .is_none_or(|sample| sample.price != price)
        {
            self.local_history.push_back(PriceSample { at, price });
        }
    }

    fn observe_official(&mut self, at: Instant, price: f64) {
        self.official_at = Some(at);
        self.official_updates += 1;
        // activeAssetCtx is pushed far more often than the oracle changes
        // (every ~3 s). De-dup by value so lead/lag intervals span real oracle
        // updates and the estimator is not recomputed on every duplicate push.
        if self
            .official_history
            .back()
            .is_none_or(|sample| sample.price != price)
        {
            self.official_history.push_back(PriceSample { at, price });
        }
    }

    fn update_lead_lag(&mut self, now: Instant, config: LeadLagConfig) {
        let Some(official_at) = self.official_history.back().map(|sample| sample.at) else {
            return;
        };
        if self.last_lead_official == Some(official_at) {
            return;
        }
        self.last_lead_official = Some(official_at);
        trim_history(
            &mut self.local_history,
            now,
            config.window + config.maximum_lag,
        );
        trim_history(&mut self.official_history, now, config.window);
        let local: Vec<_> = self.local_history.iter().copied().collect();
        let official: Vec<_> = self.official_history.iter().copied().collect();
        let estimate = lead_lag::estimate(&local, &official, now, config);
        if estimate.status == LeadStatus::Candidate
            && self.lead_lag.status == LeadStatus::Candidate
            && estimate.lead_ms.signum() == self.lead_lag.lead_ms.signum()
        {
            self.lead_streak = self.lead_streak.saturating_add(1);
        } else if estimate.status == LeadStatus::Candidate {
            self.lead_streak = 1;
        } else {
            self.lead_streak = 0;
        }
        self.lead_lag = estimate;
    }

    fn lead_label(&self) -> String {
        match self.lead_lag.status {
            LeadStatus::Insufficient => {
                let target = LeadLagConfig::MINIMUM_HISTORY.as_secs();
                let elapsed = self.lead_lag.coverage.as_secs().min(target);
                format!("warming up ({elapsed}s/{target}s)")
            }
            LeadStatus::Weak => "inconclusive".to_owned(),
            LeadStatus::Candidate if self.lead_streak >= 3 => format!(
                "{:+} ms r={:.2} confirmed",
                self.lead_lag.lead_ms, self.lead_lag.peak_correlation
            ),
            LeadStatus::Candidate => format!("validating ({}/3)", self.lead_streak),
        }
    }

    fn observe_comparison(&mut self, comparison: Comparison) {
        let Some(diff) = comparison.difference_bps else {
            return;
        };
        let absolute = diff.abs();
        self.diff_samples += 1;
        self.mean_abs_diff_bps += (absolute - self.mean_abs_diff_bps) / self.diff_samples as f64;
        self.peak_abs_diff_bps = self.peak_abs_diff_bps.max(absolute);
    }

    /// Positive: the most recent local source update followed the official update.
    /// Negative: the latest local source update preceded the official update.
    fn local_minus_official_ms(&self) -> Option<i128> {
        let local = self.local_at?;
        let official = self.official_at?;
        Some(if local >= official {
            local.duration_since(official).as_millis() as i128
        } else {
            -(official.duration_since(local).as_millis() as i128)
        })
    }
}

fn trim_history(history: &mut VecDeque<PriceSample>, now: Instant, retention: Duration) {
    while history
        .front()
        .is_some_and(|sample| now.saturating_duration_since(sample.at) > retention)
    {
        history.pop_front();
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.queue_capacity == 0 {
        bail!("--queue-capacity must be greater than zero");
    }
    let assets = Arc::new(build_assets(
        &args.coins,
        &args.hyperliquid_spot,
        &args.include_external_for,
    )?);
    let (tx, rx) = bounded_async(args.queue_capacity);
    let stats = Arc::new(FeedStats::default());
    let tls = ws::tls_config();
    let source_assets = Arc::clone(&assets);
    let source_stats = Arc::clone(&stats);
    tokio::spawn(async move { sources::run_all(source_assets, tx, source_stats, tls).await });
    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    let result = monitor(
        &mut terminal,
        rx,
        &assets,
        Duration::from_millis(args.max_quote_age_ms),
        Duration::from_millis(args.max_official_age_ms),
        stats,
    )
    .await;
    terminal.show_cursor()?;
    result
}

async fn monitor(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    rx: AsyncReceiver<Update>,
    assets: &[Asset],
    max_quote_age: Duration,
    max_official_age: Duration,
    stats: Arc<FeedStats>,
) -> Result<()> {
    let mut book = OracleBook::new(assets.len(), max_quote_age, max_official_age);
    let mut metrics: Vec<AssetMetrics> =
        (0..assets.len()).map(|_| AssetMetrics::default()).collect();
    let mut latest: Vec<Option<Comparison>> = vec![None; assets.len()];
    let mut redraw = tokio::time::interval(Duration::from_millis(100));
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut selected = 0usize;
    let mut view = View::Table;

    terminal.draw(|frame| {
        draw(
            frame,
            assets,
            &latest,
            &metrics,
            max_quote_age,
            stats.dropped_updates(),
            selected,
            view,
        )
    })?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = redraw.tick() => {
                while event::poll(Duration::ZERO)? {
                    if let Event::Key(key) = event::read()?
                        && key.kind == KeyEventKind::Press
                    {
                        match (view, key.code) {
                            (_, KeyCode::Char('q')) => return Ok(()),
                            (View::Table, KeyCode::Esc) => return Ok(()),
                            (View::Chart, KeyCode::Esc) => view = View::Table,
                            (View::Table, KeyCode::Enter) => view = View::Chart,
                            (View::Chart, KeyCode::Enter) => view = View::Table,
                            (_, KeyCode::Up | KeyCode::Char('k')) => {
                                selected = selected.saturating_sub(1);
                            }
                            (_, KeyCode::Down | KeyCode::Char('j'))
                                if selected + 1 < assets.len() =>
                            {
                                selected += 1;
                            }
                            _ => {}
                        }
                    }
                }
                let now = Instant::now();
                for (index, value) in latest.iter_mut().enumerate() {
                    *value = book.comparison(index, now);
                }
                // Bound history here as well as in update_lead_lag: the latter
                // only runs on official updates, so a stalled Hyperliquid feed
                // would otherwise let local_history grow without limit.
                let config = LeadLagConfig::market_making(max_quote_age);
                for metric in metrics.iter_mut() {
                    trim_history(&mut metric.local_history, now, config.window + config.maximum_lag);
                    trim_history(&mut metric.official_history, now, config.window);
                }
                terminal.draw(|frame| draw(frame, assets, &latest, &metrics, max_quote_age, stats.dropped_updates(), selected, view))?;
            },
            update = rx.recv() => {
                let update = match update {
                    Ok(update) => update,
                    Err(_) => return Ok(()),
                };
                let now = Instant::now();
                let asset = match update {
                    Update::Quote(quote) => {
                        let asset = quote.asset as usize;
                        if !book.apply_quote(quote) { continue; }
                        metrics[asset].source_at[quote.venue as usize] = Some(quote.received_at);
                        if let Some(latency) = quote.latency_ms {
                            metrics[asset].source_latency[quote.venue as usize] = Some(latency);
                        }
                        asset
                    }
                    Update::Official(official) => {
                        let asset = official.asset as usize;
                        if !book.apply_official(official) { continue; }
                        metrics[asset].observe_official(official.received_at, official.oracle);
                        metrics[asset].update_lead_lag(now, LeadLagConfig::market_making(max_quote_age));
                        asset
                    }
                };
                let comparison = book.comparison(asset, now);
                latest[asset] = comparison;
                if let Update::Quote(quote) = update {
                    metrics[asset].observe_local(quote.received_at, comparison.map(|value| value.local));
                }
                if let Some(comparison) = comparison {
                    metrics[asset].observe_comparison(comparison);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn draw(
    frame: &mut ratatui::Frame,
    assets: &[Asset],
    latest: &[Option<Comparison>],
    metrics: &[AssetMetrics],
    max_quote_age: Duration,
    dropped_updates: u64,
    selected: usize,
    view: View,
) {
    if view == View::Chart {
        draw_chart(frame, assets, metrics, selected);
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(VENUE_COUNT as u16 + 3),
            Constraint::Length(3),
        ])
        .split(frame.area());
    frame.render_widget(
        Paragraph::new("Hyperliquid Local Oracle Monitor  (\u{2191}/\u{2193} select \u{00b7} Enter chart \u{00b7} q quit)").block(
            Block::default()
                .borders(Borders::ALL)
                .title("Observed Feed Comparison"),
        ),
        layout[0],
    );

    let wide = frame.area().width >= 165;
    let rows = assets.iter().enumerate().map(|(index, asset)| {
        let comparison = latest[index];
        let local = comparison.map_or_else(|| "-".to_owned(), |value| price(value.local));
        let official = comparison
            .and_then(|value| value.official)
            .map_or_else(|| "-".to_owned(), price);
        let difference = comparison
            .and_then(|value| value.difference_bps)
            .map_or_else(|| "-".to_owned(), signed_bps);
        let lead = metrics[index].lead_label();
        let source_weight = comparison.map_or_else(
            || "-".to_owned(),
            |value| {
                format!(
                    "{}/{} ({}/{})",
                    value.sources,
                    asset.expected_source_count(),
                    value.total_weight,
                    asset.expected_source_weight(),
                )
            },
        );
        let local_age = metrics[index].local_at.map_or_else(|| "-".to_owned(), age);
        let official_age = metrics[index]
            .official_at
            .map_or_else(|| "-".to_owned(), age);
        let gap = metrics[index]
            .local_minus_official_ms()
            .map_or_else(|| "-".to_owned(), |value| format!("{value:+} ms"));
        let error = if metrics[index].diff_samples == 0 {
            "-".to_owned()
        } else {
            format!(
                "{:.3}/{:.3}",
                metrics[index].mean_abs_diff_bps, metrics[index].peak_abs_diff_bps
            )
        };
        let cells = if wide {
            vec![
                Cell::from(asset.coin.clone()),
                Cell::from(local),
                Cell::from(official),
                Cell::from(difference),
                Cell::from(lead),
                Cell::from(source_weight),
                Cell::from(local_age),
                Cell::from(official_age),
                Cell::from(gap),
                Cell::from(error),
                Cell::from(format!(
                    "{}/{}",
                    metrics[index].local_updates, metrics[index].official_updates
                )),
            ]
        } else {
            vec![
                Cell::from(asset.coin.clone()),
                Cell::from(local),
                Cell::from(official),
                Cell::from(difference),
                Cell::from(lead),
                Cell::from(source_weight),
            ]
        };
        let row = Row::new(cells);
        if index == selected {
            row.style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            row
        }
    });
    let (header, widths) = if wide {
        (
            Row::new([
                "Coin",
                "Local",
                "HL official",
                "Diff bps",
                "Lead/corr",
                "Fresh/expected",
                "Local age",
                "HL age",
                "L-HL gap",
                "Avg/Max abs bps",
                "Updates",
            ]),
            vec![
                Constraint::Length(8),
                Constraint::Length(14),
                Constraint::Length(14),
                Constraint::Length(11),
                Constraint::Length(20),
                Constraint::Length(15),
                Constraint::Length(11),
                Constraint::Length(10),
                Constraint::Length(11),
                Constraint::Length(17),
                Constraint::Length(12),
            ],
        )
    } else {
        (
            Row::new([
                "Coin",
                "Local",
                "HL official",
                "Diff bps",
                "Lead/corr",
                "Fresh/expected",
            ]),
            vec![
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(17),
                Constraint::Min(7),
            ],
        )
    };
    frame.render_widget(
        Table::new(rows, widths)
            .header(header.style(Style::default().fg(Color::Cyan)))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Prices and Observed Timing"),
            ),
        layout[1],
    );
    frame.render_widget(
        source_latency_table(assets, metrics, max_quote_age),
        layout[2],
    );
    let footer = if wide {
        format!(
            "\u{2191}/\u{2193} select \u{00b7} Enter opens the price chart \u{00b7} L-HL gap = latest local minus latest HL update time (negative = local first). Queue drops: {dropped_updates}. q exits."
        )
    } else {
        format!(
            "\u{2191}/\u{2193} select \u{00b7} Enter opens the price chart. Use a 165-column terminal for ages, gap, and rolling error. Queue drops: {dropped_updates}. q exits."
        )
    };
    frame.render_widget(
        Paragraph::new(footer).block(Block::default().borders(Borders::ALL)),
        layout[3],
    );
}

/// Resamples a chronological `(x, price)` series onto a uniform grid of spacing
/// `step`, holding the last observed price forward (pandas-style `ffill`). The
/// result is a step function: flat between updates, vertical at each change.
fn forward_fill(points: &[(f64, f64)], step: f64) -> Vec<(f64, f64)> {
    let Some(&(start, first)) = points.first() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut index = 0;
    let mut last = first;
    let mut x = start;
    while x < 0.0 {
        while index < points.len() && points[index].0 <= x {
            last = points[index].1;
            index += 1;
        }
        out.push((x, last));
        x += step;
    }
    // Anchor the final point at "now" with the most recent price.
    out.push((0.0, points[points.len() - 1].1));
    out
}

/// Full-screen line chart of the local oracle versus the Hyperliquid oracle for
/// the selected coin. The two lines share a price axis so the spread and any
/// lead/lag between them are directly visible.
fn draw_chart(
    frame: &mut ratatui::Frame,
    assets: &[Asset],
    metrics: &[AssetMetrics],
    selected: usize,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(3)])
        .split(frame.area());

    let asset = &assets[selected];
    let metric = &metrics[selected];
    let now = Instant::now();
    let window = 300.0_f64;
    let to_points = |history: &VecDeque<PriceSample>| -> Vec<(f64, f64)> {
        history
            .iter()
            .map(|sample| {
                (
                    -now.saturating_duration_since(sample.at).as_secs_f64(),
                    sample.price,
                )
            })
            .filter(|(x, _)| *x >= -window)
            .collect()
    };
    let local_raw = to_points(&metric.local_history);
    let official_raw = to_points(&metric.official_history);

    let footer_text = format!(
        "{} \u{00b7} local vs HL oracle \u{00b7} lead/corr: {} \u{00b7} \u{2191}/\u{2193} change coin \u{00b7} Enter/Esc back \u{00b7} q quit",
        asset.coin,
        metric.lead_label(),
    );
    let footer = Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL));

    let prices: Vec<f64> = local_raw
        .iter()
        .chain(official_raw.iter())
        .map(|(_, y)| *y)
        .collect();
    if prices.is_empty() {
        frame.render_widget(
            Paragraph::new("Collecting price history\u{2026}").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("{} \u{2014} local vs HL oracle", asset.coin)),
            ),
            layout[0],
        );
        frame.render_widget(footer, layout[1]);
        return;
    }

    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for price in &prices {
        lo = lo.min(*price);
        hi = hi.max(*price);
    }
    let pad = ((hi - lo) * 0.1)
        .max(hi.abs() * 1e-5)
        .max(f64::MIN_POSITIVE);
    let (ylo, yhi) = (lo - pad, hi + pad);
    let x_min = local_raw
        .iter()
        .chain(official_raw.iter())
        .map(|(x, _)| *x)
        .fold(0.0_f64, f64::min);
    // Guarantee a non-zero-width axis; ratatui normalises by (max - min).
    let x_min = if x_min < 0.0 { x_min } else { -1.0 };

    // Forward-fill onto a dense uniform grid so each series is a proper
    // staircase: the price is held constant between updates and jumps only when
    // a new value arrives. The Hyperliquid oracle updates every ~3 s, so without
    // this it renders as sparse diagonal segments; with it, both lines are
    // dense, aligned step functions.
    let step = ((-x_min) / 600.0).max(0.02);
    let local = forward_fill(&local_raw, step);
    let official = forward_fill(&official_raw, step);

    let datasets = vec![
        Dataset::default()
            .name("local")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&local),
        Dataset::default()
            .name("HL oracle")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Yellow))
            .data(&official),
    ];
    let x_axis = Axis::default()
        .title("t (s, 0 = now)")
        .style(Style::default().fg(Color::Gray))
        .bounds([x_min, 0.0])
        .labels([
            format!("{x_min:.0}"),
            format!("{:.0}", x_min / 2.0),
            "0".to_owned(),
        ]);
    let y_axis = Axis::default()
        .title("price")
        .style(Style::default().fg(Color::Gray))
        .bounds([ylo, yhi])
        .labels([price(ylo), price((ylo + yhi) / 2.0), price(yhi)]);
    let chart = Chart::new(datasets)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "{} \u{2014} local (cyan) vs HL oracle (yellow)",
            asset.coin
        )))
        .x_axis(x_axis)
        .y_axis(y_axis)
        .legend_position(Some(LegendPosition::TopRight));
    frame.render_widget(chart, layout[0]);
    frame.render_widget(footer, layout[1]);
}

/// Per-source health matrix (rows are venues, columns are coins). Each cell is
/// `latency · since`, where latency is the last `local receive time - exchange
/// send timestamp` in milliseconds and `since` is the time elapsed since the
/// last quote from that venue. Latency is `-` when the venue does not timestamp
/// its messages (Binance bookTicker); the whole cell is `-` when the venue is
/// not a source for the coin. Colour tracks latency (green/yellow/red, magenta
/// for a negative value from an unsynchronised clock) and dims when stale.
fn source_latency_table<'a>(
    assets: &'a [Asset],
    metrics: &[AssetMetrics],
    max_quote_age: Duration,
) -> Table<'a> {
    let now = Instant::now();
    let rows = Venue::ALL.into_iter().map(|venue| {
        let mut cells = vec![Cell::from(venue.name())];
        for metric in metrics.iter().take(assets.len()) {
            let cell = match metric.source_at[venue as usize] {
                Some(at) => {
                    let since = now.saturating_duration_since(at);
                    let latency = metric.source_latency[venue as usize];
                    let latency_text =
                        latency.map_or_else(|| "-".to_owned(), |value| value.to_string());
                    let stale = since > max_quote_age * 2;
                    Cell::from(format!("{latency_text} \u{00b7} {}", since_label(since)))
                        .style(source_cell_style(latency, stale))
                }
                None => Cell::from("-"),
            };
            cells.push(cell);
        }
        Row::new(cells)
    });
    let mut header = vec![Cell::from("Source")];
    header.extend(assets.iter().map(|asset| Cell::from(asset.coin.clone())));
    let mut widths = vec![Constraint::Length(16)];
    widths.extend(std::iter::repeat_n(Constraint::Length(15), assets.len()));
    Table::new(rows, widths)
        .header(Row::new(header).style(Style::default().fg(Color::Cyan)))
        .block(
            Block::default().borders(Borders::ALL).title(
                "Sources: latency ms \u{00b7} since last update (Binance and KuCoin report no send timestamp)",
            ),
        )
}

fn since_label(since: Duration) -> String {
    let millis = since.as_millis();
    if millis < 1_000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", since.as_secs_f64())
    }
}

fn source_cell_style(latency_ms: Option<i64>, stale: bool) -> Style {
    if stale {
        return Style::default().fg(Color::DarkGray);
    }
    let color = match latency_ms {
        Some(value) if value < 0 => Color::Magenta,
        Some(value) if value < 100 => Color::Green,
        Some(value) if value < 400 => Color::Yellow,
        Some(_) => Color::Red,
        // Fresh, but the venue gives no timestamp to measure latency.
        None => Color::Green,
    };
    Style::default().fg(color)
}

fn price(value: f64) -> String {
    if value >= 1_000.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.6}")
    }
}

fn signed_bps(value: f64) -> String {
    format!("{value:+.3}")
}

fn age(at: Instant) -> String {
    format!("{} ms", at.elapsed().as_millis())
}
