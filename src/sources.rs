use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use fastwebsockets::{Frame, OpCode, Payload};
use hyper::Method;
use kanal::AsyncSender;
use prost::Message;
use serde::{
    Deserialize,
    de::{self, Visitor},
};
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

use crate::{
    model::{FeedStats, OfficialUpdate, QuoteUpdate, Update, Venue},
    ws,
};

#[derive(Clone, Debug)]
pub struct Asset {
    pub coin: String,
    pub hyperliquid_spot: Option<String>,
    pub include_external: bool,
}

impl Asset {
    pub const EXTERNAL_SOURCE_COUNT: u8 = 7;
    pub const EXTERNAL_SOURCE_WEIGHT: u8 = 11;

    pub fn expected_source_count(&self) -> u8 {
        u8::from(self.include_external) * Self::EXTERNAL_SOURCE_COUNT
            + u8::from(self.hyperliquid_spot.is_some())
    }

    pub fn expected_source_weight(&self) -> u8 {
        u8::from(self.include_external) * Self::EXTERNAL_SOURCE_WEIGHT
            + u8::from(self.hyperliquid_spot.is_some())
    }

    fn venue_symbol(&self, venue: Venue) -> String {
        match venue {
            Venue::Binance | Venue::Bybit | Venue::Mexc => format!("{}USDT", self.coin),
            Venue::Okx | Venue::Kucoin => format!("{}-USDT", self.coin),
            Venue::Kraken => format!("{}/USD", self.coin),
            Venue::Gate => format!("{}_USDT", self.coin),
            Venue::Hyperliquid => self.hyperliquid_spot.clone().unwrap_or_default(),
        }
    }
}

pub async fn run_all(
    assets: Arc<Vec<Asset>>,
    tx: AsyncSender<Update>,
    stats: Arc<FeedStats>,
    tls: Arc<rustls::ClientConfig>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for venue in Venue::ALL {
        if venue != Venue::Hyperliquid && !assets.iter().any(|asset| asset.include_external) {
            continue;
        }
        let assets = Arc::clone(&assets);
        let tx = tx.clone();
        let stats = Arc::clone(&stats);
        let tls = Arc::clone(&tls);
        tasks.spawn(async move {
            if venue == Venue::Hyperliquid {
                run_hyperliquid(assets, tx, stats, tls).await
            } else {
                run_venue(venue, assets, tx, stats, tls).await
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            warn!(?error, "oracle source task terminated");
        }
    }
}

async fn run_venue(
    venue: Venue,
    assets: Arc<Vec<Asset>>,
    tx: AsyncSender<Update>,
    stats: Arc<FeedStats>,
    tls: Arc<rustls::ClientConfig>,
) {
    let endpoint = match venue {
        // KuCoin requires a per-connection token fetched over HTTPS.
        Venue::Kucoin => Endpoint::Kucoin,
        _ => Endpoint::Static(venue_url(venue).expect("external venue has a URL")),
    };
    let subscriptions = subscriptions(venue, &assets);
    reconnect_loop(
        venue.name(),
        endpoint,
        subscriptions,
        tls,
        move |payload, received_at, received_ms| {
            if venue == Venue::Mexc {
                if let Some((symbol, bid, ask, server_ms)) = parse_mexc_bbo(payload)
                    && let Some(asset) = asset_index(venue, &symbol, &assets)
                {
                    publish(
                        &tx,
                        &stats,
                        Update::Quote(QuoteUpdate {
                            venue,
                            asset,
                            bid,
                            ask,
                            received_at,
                            latency_ms: server_ms.map(|server| received_ms - server),
                        }),
                    );
                }
            } else if let Some((symbol, bid, ask, server_ms)) = parse_external(venue, payload)
                && let Some(asset) = asset_index(venue, symbol, &assets)
            {
                publish(
                    &tx,
                    &stats,
                    Update::Quote(QuoteUpdate {
                        venue,
                        asset,
                        bid,
                        ask,
                        received_at,
                        latency_ms: server_ms.map(|server| received_ms - server),
                    }),
                );
            }
        },
    )
    .await;
}

async fn run_hyperliquid(
    assets: Arc<Vec<Asset>>,
    tx: AsyncSender<Update>,
    stats: Arc<FeedStats>,
    tls: Arc<rustls::ClientConfig>,
) {
    let mut subscriptions = Vec::with_capacity(assets.len() * 2);
    for asset in assets.iter() {
        subscriptions.push(format!(
            r#"{{"method":"subscribe","subscription":{{"type":"activeAssetCtx","coin":"{}"}}}}"#,
            asset.coin
        ));
        if let Some(spot) = &asset.hyperliquid_spot {
            subscriptions.push(format!(
                r#"{{"method":"subscribe","subscription":{{"type":"bbo","coin":"{}"}}}}"#,
                spot
            ));
        }
    }
    reconnect_loop(
        "hyperliquid",
        Endpoint::Static("wss://api.hyperliquid.xyz/ws"),
        subscriptions,
        tls,
        move |payload, received_at, received_ms| match parse_hyperliquid(payload) {
            Some(HyperliquidMessage::Official { coin, oracle }) => {
                if let Some(asset) = asset_index_perp(coin, &assets) {
                    publish(
                        &tx,
                        &stats,
                        Update::Official(OfficialUpdate {
                            asset,
                            oracle,
                            received_at,
                        }),
                    );
                }
            }
            Some(HyperliquidMessage::Spot {
                coin,
                bid,
                ask,
                time,
            }) => {
                if let Some(asset) = asset_index_spot(coin, &assets) {
                    publish(
                        &tx,
                        &stats,
                        Update::Quote(QuoteUpdate {
                            venue: Venue::Hyperliquid,
                            asset,
                            bid,
                            ask,
                            received_at,
                            latency_ms: time.map(|server| received_ms - server),
                        }),
                    );
                }
            }
            None => {}
        },
    )
    .await;
}

fn publish(tx: &AsyncSender<Update>, stats: &FeedStats, update: Update) {
    if tx.try_send(update).is_err() {
        stats.record_drop();
    }
}

#[derive(Clone, Copy)]
enum Endpoint {
    Static(&'static str),
    Kucoin,
}

async fn resolve_endpoint(endpoint: Endpoint, tls: &Arc<rustls::ClientConfig>) -> Result<String> {
    match endpoint {
        Endpoint::Static(url) => Ok(url.to_owned()),
        Endpoint::Kucoin => kucoin_ws_url(tls).await,
    }
}

/// Fetches a public KuCoin WebSocket token and builds the connect URL. KuCoin
/// rejects tokenless connections, so this must run before every reconnect.
async fn kucoin_ws_url(tls: &Arc<rustls::ClientConfig>) -> Result<String> {
    #[derive(Deserialize)]
    struct Bullet {
        data: BulletData,
    }
    #[derive(Deserialize)]
    struct BulletData {
        token: String,
        #[serde(rename = "instanceServers")]
        instance_servers: Vec<Server>,
    }
    #[derive(Deserialize)]
    struct Server {
        endpoint: String,
    }
    let body = ws::https_request(
        Method::POST,
        "https://api.kucoin.com/api/v1/bullet-public",
        Arc::clone(tls),
    )
    .await
    .context("KuCoin bullet-public request failed")?;
    let bullet: Bullet =
        serde_json::from_slice(&body).context("invalid KuCoin bullet-public response")?;
    let server = bullet
        .data
        .instance_servers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("KuCoin returned no instance servers"))?;
    Ok(format!(
        "{}?token={}&connectId=hl-oracle",
        server.endpoint, bullet.data.token
    ))
}

async fn reconnect_loop<F>(
    name: &'static str,
    endpoint: Endpoint,
    subscriptions: Vec<String>,
    tls: Arc<rustls::ClientConfig>,
    mut on_message: F,
) where
    F: FnMut(&[u8], Instant, i64),
{
    let mut failures = 0_u32;
    loop {
        let connection_started = Instant::now();
        let connection = match resolve_endpoint(endpoint, &tls).await {
            Ok(url) => ws::connect(&url, Arc::clone(&tls)).await,
            Err(error) => Err(error),
        };
        match connection {
            Ok(mut socket) => {
                info!(source = name, "connected");
                let result: Result<()> = async {
                    for subscription in &subscriptions {
                        write_text(&mut socket, subscription).await?;
                    }
                    let mut heartbeat = time::interval(Duration::from_secs(15));
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    heartbeat.tick().await;
                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if let Some(message) = heartbeat_message(name) {
                                    write_text(&mut socket, &message).await?;
                                }
                            }
                            frame = socket.read_frame() => {
                                let frame = frame?;
                                match frame.opcode {
                                    OpCode::Text | OpCode::Binary => {
                                        // Capture receipt time once, as close to the wire as
                                        // possible: Instant for freshness, wall-clock for latency.
                                        let received_at = Instant::now();
                                        let received_ms = unix_millis();
                                        let bytes: &[u8] = match &frame.payload {
                                            Payload::Owned(bytes) => bytes,
                                            Payload::Borrowed(bytes) => bytes,
                                            Payload::BorrowedMut(bytes) => bytes,
                                            Payload::Bytes(bytes) => bytes,
                                        };
                                        on_message(bytes, received_at, received_ms);
                                    }
                                    OpCode::Close => return Err(anyhow!("server closed websocket")),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                .await;
                if let Err(error) = result {
                    warn!(source = name, ?error, "websocket session ended");
                }
                if connection_started.elapsed() > Duration::from_secs(30) {
                    failures = 0;
                }
            }
            Err(error) => warn!(source = name, ?error, "websocket connect failed"),
        }
        failures = failures.saturating_add(1);
        let delay = Duration::from_millis((250_u64 << failures.min(6)).min(10_000));
        time::sleep(delay).await;
    }
}

async fn write_text(socket: &mut ws::WsStream, message: &str) -> Result<()> {
    socket
        .write_frame(Frame::text(Payload::Owned(message.as_bytes().to_vec())))
        .await
        .context("websocket write failed")
}

fn venue_url(venue: Venue) -> Option<&'static str> {
    Some(match venue {
        Venue::Binance => "wss://stream.binance.com:9443/ws",
        Venue::Okx => "wss://ws.okx.com:8443/ws/v5/public",
        Venue::Bybit => "wss://stream.bybit.com/v5/public/spot",
        Venue::Kraken => "wss://ws.kraken.com/v2",
        // KuCoin uses a dynamic token URL resolved via Endpoint::Kucoin.
        Venue::Kucoin => return None,
        Venue::Gate => "wss://api.gateio.ws/ws/v4/",
        Venue::Mexc => "wss://wbs-api.mexc.com/ws",
        Venue::Hyperliquid => return None,
    })
}

fn subscriptions(venue: Venue, assets: &[Asset]) -> Vec<String> {
    let symbols: Vec<String> = assets
        .iter()
        .filter(|asset| asset.include_external)
        .map(|asset| asset.venue_symbol(venue))
        .collect();
    match venue {
        Venue::Binance => vec![format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":1}}"#,
            symbols
                .iter()
                .map(|symbol| format!(r#""{}@bookTicker""#, symbol.to_lowercase()))
                .collect::<Vec<_>>()
                .join(",")
        )],
        Venue::Okx => vec![format!(
            r#"{{"op":"subscribe","args":[{}]}}"#,
            symbols
                .iter()
                .map(|symbol| format!(r#"{{"channel":"tickers","instId":"{}"}}"#, symbol))
                .collect::<Vec<_>>()
                .join(",")
        )],
        Venue::Bybit => vec![format!(
            r#"{{"op":"subscribe","args":[{}]}}"#,
            symbols
                .iter()
                .map(|symbol| format!(r#""orderbook.1.{}""#, symbol))
                .collect::<Vec<_>>()
                .join(",")
        )],
        // event_trigger:bbo makes Kraken push on best bid/ask changes; the
        // default ("trades") only refreshes the quote when a trade prints.
        Venue::Kraken => vec![format!(
            r#"{{"method":"subscribe","params":{{"channel":"ticker","event_trigger":"bbo","symbol":[{}]}}}}"#,
            symbols
                .iter()
                .map(|symbol| format!(r#""{}""#, symbol))
                .collect::<Vec<_>>()
                .join(",")
        )],
        Venue::Kucoin => vec![format!(
            r#"{{"id":"oracle-sub","type":"subscribe","topic":"/market/ticker:{}","response":true}}"#,
            symbols.join(",")
        )],
        Venue::Gate => vec![format!(
            r#"{{"time":{},"channel":"spot.book_ticker","event":"subscribe","payload":[{}]}}"#,
            unix_seconds(),
            symbols
                .iter()
                .map(|symbol| format!(r#""{}""#, symbol))
                .collect::<Vec<_>>()
                .join(",")
        )],
        Venue::Mexc => vec![format!(
            r#"{{"method":"SUBSCRIPTION","params":[{}]}}"#,
            symbols
                .iter()
                .map(|symbol| format!(
                    r#""spot@public.aggre.bookTicker.v3.api.pb@10ms@{}""#,
                    symbol
                ))
                .collect::<Vec<_>>()
                .join(",")
        )],
        Venue::Hyperliquid => Vec::new(),
    }
}

fn heartbeat_message(name: &str) -> Option<String> {
    Some(match name {
        "okx" => "ping".to_owned(),
        "bybit" => r#"{"op":"ping"}"#.to_owned(),
        "kraken" => r#"{"method":"ping"}"#.to_owned(),
        "kucoin" => r#"{"id":"oracle-ping","type":"ping"}"#.to_owned(),
        "gate" => format!(r#"{{"time":{},"channel":"spot.ping"}}"#, unix_seconds()),
        "mexc" => r#"{"method":"PING"}"#.to_owned(),
        "hyperliquid" => r#"{"method":"ping"}"#.to_owned(),
        // Binance keepalive is handled by WebSocket-level auto-pong; an
        // application ping is an invalid request that only draws errors.
        _ => return None,
    })
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Parses the fixed-format RFC 3339 timestamps Kraken sends
/// (e.g. `2026-07-10T14:20:14.823647Z`) into Unix epoch milliseconds.
fn parse_rfc3339_millis(text: &str) -> Option<i64> {
    let text = text.strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut date = date.split('-');
    let year: i64 = date.next()?.parse().ok()?;
    let month: i64 = date.next()?.parse().ok()?;
    let day: i64 = date.next()?.parse().ok()?;
    let mut time = time.split(':');
    let hour: i64 = time.next()?.parse().ok()?;
    let minute: i64 = time.next()?.parse().ok()?;
    let seconds = time.next()?;
    let (whole, frac) = seconds.split_once('.').unwrap_or((seconds, ""));
    let whole: i64 = whole.parse().ok()?;
    // Take up to millisecond precision from the fractional part. Read the first
    // three ASCII digits directly so a malformed (e.g. non-ASCII) fraction can
    // never index into the middle of a UTF-8 code point and panic.
    let mut digits = frac.bytes().filter(u8::is_ascii_digit);
    let mut millis = 0_i64;
    for place in [100, 10, 1] {
        millis += place * digits.next().map_or(0, |byte| i64::from(byte - b'0'));
    }
    // Days since the Unix epoch via the days-from-civil algorithm.
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + whole;
    Some(secs * 1_000 + millis)
}

fn asset_index(venue: Venue, symbol: &str, assets: &[Asset]) -> Option<u16> {
    assets
        .iter()
        .position(|asset| {
            asset.include_external && venue_symbol_matches(venue, &asset.coin, symbol)
        })
        .map(|index| index as u16)
}

fn venue_symbol_matches(venue: Venue, coin: &str, symbol: &str) -> bool {
    let base = match venue {
        Venue::Binance | Venue::Bybit | Venue::Mexc => symbol.strip_suffix("USDT"),
        Venue::Okx | Venue::Kucoin => symbol.strip_suffix("-USDT"),
        Venue::Kraken => symbol.strip_suffix("/USD"),
        Venue::Gate => symbol.strip_suffix("_USDT"),
        Venue::Hyperliquid => None,
    };
    base.is_some_and(|base| base.eq_ignore_ascii_case(coin))
}

fn asset_index_perp(coin: &str, assets: &[Asset]) -> Option<u16> {
    assets
        .iter()
        .position(|asset| asset.coin == coin)
        .map(|index| index as u16)
}

fn asset_index_spot(coin: &str, assets: &[Asset]) -> Option<u16> {
    assets
        .iter()
        .position(|asset| asset.hyperliquid_spot.as_deref() == Some(coin))
        .map(|index| index as u16)
}

fn parse_price(text: &str) -> Option<f64> {
    let price = text.parse::<f64>().ok()?;
    (price.is_finite() && price > 0.0).then_some(price)
}

#[derive(Clone, Copy)]
struct Price(f64);

impl<'de> Deserialize<'de> for Price {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PriceVisitor;

        impl Visitor<'_> for PriceVisitor {
            type Value = Price;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a finite positive numeric price or numeric price string")
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Price, E>
            where
                E: de::Error,
            {
                valid_price(value).ok_or_else(|| E::custom("invalid price"))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Price, E>
            where
                E: de::Error,
            {
                self.visit_f64(value as f64)
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Price, E>
            where
                E: de::Error,
            {
                self.visit_f64(value as f64)
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Price, E>
            where
                E: de::Error,
            {
                parse_price(value)
                    .map(Price)
                    .ok_or_else(|| E::custom("invalid price"))
            }
        }

        deserializer.deserialize_any(PriceVisitor)
    }
}

fn valid_price(value: f64) -> Option<Price> {
    (value.is_finite() && value > 0.0).then_some(Price(value))
}

/// Returns `(symbol, bid, ask, server_send_millis)`. The last element is the
/// exchange-provided send timestamp in Unix epoch milliseconds, or `None` when
/// the venue does not include one (Binance bookTicker).
fn parse_external(venue: Venue, payload: &[u8]) -> Option<(&str, f64, f64, Option<i64>)> {
    match venue {
        Venue::Binance => {
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                s: &'a str,
                #[serde(borrow)]
                b: &'a str,
                #[serde(borrow)]
                a: &'a str,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            // Binance bookTicker carries no server timestamp.
            Some((message.s, parse_price(message.b)?, parse_price(message.a)?, None))
        }
        Venue::Okx => {
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                data: Vec<Ticker<'a>>,
            }
            #[derive(Deserialize)]
            struct Ticker<'a> {
                #[serde(borrow, rename = "instId")]
                symbol: &'a str,
                #[serde(borrow, rename = "bidPx")]
                bid: &'a str,
                #[serde(borrow, rename = "askPx")]
                ask: &'a str,
                #[serde(borrow, default)]
                ts: Option<&'a str>,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            let ticker = message.data.first()?;
            Some((
                ticker.symbol,
                parse_price(ticker.bid)?,
                parse_price(ticker.ask)?,
                ticker.ts.and_then(|ts| ts.parse().ok()),
            ))
        }
        Venue::Bybit => {
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                data: Book<'a>,
                #[serde(default)]
                ts: Option<i64>,
            }
            #[derive(Deserialize)]
            struct Book<'a> {
                #[serde(borrow)]
                s: &'a str,
                #[serde(borrow)]
                b: Vec<[&'a str; 2]>,
                #[serde(borrow)]
                a: Vec<[&'a str; 2]>,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            // Bybit spot `orderbook.1` publishes full snapshots (verified live:
            // no deltas, so both sides are always present). If Bybit ever sent a
            // single-sided delta, `first()?` drops that frame rather than
            // emitting a wrong mid; the stale quote then ages out normally.
            Some((
                message.data.s,
                parse_price(message.data.b.first()?[0])?,
                parse_price(message.data.a.first()?[0])?,
                message.ts,
            ))
        }
        Venue::Kraken => {
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                data: Vec<Ticker<'a>>,
            }
            #[derive(Deserialize)]
            struct Ticker<'a> {
                #[serde(borrow)]
                symbol: &'a str,
                bid: Price,
                ask: Price,
                #[serde(borrow, default)]
                timestamp: Option<&'a str>,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            let ticker = message.data.first()?;
            Some((
                ticker.symbol,
                ticker.bid.0,
                ticker.ask.0,
                ticker.timestamp.and_then(parse_rfc3339_millis),
            ))
        }
        Venue::Kucoin => {
            // Live format: {"topic":"/market/ticker:BTC-USDT","subject":"trade.ticker",
            //               "data":{"bestBid":"...","bestAsk":"...","time":<ms>}}
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                topic: &'a str,
                #[serde(borrow)]
                data: Ticker<'a>,
            }
            #[derive(Deserialize)]
            struct Ticker<'a> {
                #[serde(borrow, rename = "bestBid")]
                bid: &'a str,
                #[serde(borrow, rename = "bestAsk")]
                ask: &'a str,
                time: Option<i64>,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            let symbol = message
                .topic
                .strip_prefix("/market/ticker:")?
                .split(',')
                .next()?;
            Some((
                symbol,
                parse_price(message.data.bid)?,
                parse_price(message.data.ask)?,
                message.data.time,
            ))
        }
        Venue::Gate => {
            #[derive(Deserialize)]
            struct Message<'a> {
                #[serde(borrow)]
                result: Ticker<'a>,
            }
            #[derive(Deserialize)]
            struct Ticker<'a> {
                #[serde(borrow)]
                s: &'a str,
                #[serde(borrow)]
                b: &'a str,
                #[serde(borrow)]
                a: &'a str,
                t: Option<i64>,
            }
            let message: Message<'_> = serde_json::from_slice(payload).ok()?;
            Some((
                message.result.s,
                parse_price(message.result.b)?,
                parse_price(message.result.a)?,
                message.result.t,
            ))
        }
        Venue::Mexc => {
            let _ = payload;
            None
        }
        Venue::Hyperliquid => None,
    }
}

// MEXC PushDataV3ApiWrapper: channel=1, symbol=3, sendTime=6, publicAggreBookTicker=315.
// The field numbers are from MEXC's published protobuf schema (verified live).
#[derive(Message)]
struct MexcPush {
    #[prost(string, optional, tag = "3")]
    symbol: Option<String>,
    #[prost(int64, optional, tag = "6")]
    send_time: Option<i64>,
    #[prost(message, optional, tag = "315")]
    ticker: Option<MexcBookTicker>,
}

#[derive(Message)]
struct MexcBookTicker {
    #[prost(string, tag = "1")]
    bid_price: String,
    #[prost(string, tag = "3")]
    ask_price: String,
}

fn parse_mexc_bbo(payload: &[u8]) -> Option<(String, f64, f64, Option<i64>)> {
    let message = MexcPush::decode(payload).ok()?;
    let symbol = message.symbol?;
    let ticker = message.ticker?;
    Some((
        symbol,
        parse_price(&ticker.bid_price)?,
        parse_price(&ticker.ask_price)?,
        message.send_time,
    ))
}

enum HyperliquidMessage<'a> {
    Official {
        coin: &'a str,
        oracle: f64,
    },
    Spot {
        coin: &'a str,
        bid: f64,
        ask: f64,
        time: Option<i64>,
    },
}

fn parse_hyperliquid(payload: &[u8]) -> Option<HyperliquidMessage<'_>> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        channel: &'a str,
        #[serde(borrow)]
        data: &'a serde_json::value::RawValue,
    }
    let envelope: Envelope<'_> = serde_json::from_slice(payload).ok()?;
    match envelope.channel {
        "activeAssetCtx" => {
            #[derive(Deserialize)]
            struct Data<'a> {
                #[serde(borrow)]
                coin: &'a str,
                ctx: Ctx,
            }
            #[derive(Deserialize)]
            struct Ctx {
                #[serde(rename = "oraclePx")]
                oracle: Price,
            }
            let data: Data<'_> = serde_json::from_str(envelope.data.get()).ok()?;
            Some(HyperliquidMessage::Official {
                coin: data.coin,
                oracle: data.ctx.oracle.0,
            })
        }
        "bbo" => {
            #[derive(Deserialize)]
            struct Data<'a> {
                #[serde(borrow)]
                coin: &'a str,
                time: Option<i64>,
                #[serde(borrow)]
                bbo: [Option<Level<'a>>; 2],
            }
            #[derive(Deserialize)]
            struct Level<'a> {
                #[serde(borrow)]
                px: &'a str,
            }
            let data: Data<'_> = serde_json::from_str(envelope.data.get()).ok()?;
            Some(HyperliquidMessage::Spot {
                coin: data.coin,
                bid: parse_price(data.bbo[0].as_ref()?.px)?,
                ask: parse_price(data.bbo[1].as_ref()?.px)?,
                time: data.time,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_bbo() {
        let input = br#"{"s":"BTCUSDT","b":"100.0","a":"101.0"}"#;
        assert_eq!(
            parse_external(Venue::Binance, input),
            Some(("BTCUSDT", 100.0, 101.0, None))
        );
    }

    #[test]
    fn parses_current_kucoin_bbo() {
        let input = br#"{"topic":"/market/ticker:BTC-USDT","type":"message","subject":"trade.ticker","data":{"bestAsk":"101.0","bestAskSize":"1","bestBid":"100.0","bestBidSize":"1","price":"100.5","time":1700000000000}}"#;
        assert_eq!(
            parse_external(Venue::Kucoin, input),
            Some(("BTC-USDT", 100.0, 101.0, Some(1_700_000_000_000)))
        );
    }

    #[test]
    fn parses_okx_bbo_with_timestamp() {
        let input = br#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","bidPx":"100.0","askPx":"101.0","ts":"1700000000000"}]}"#;
        assert_eq!(
            parse_external(Venue::Okx, input),
            Some(("BTC-USDT", 100.0, 101.0, Some(1_700_000_000_000)))
        );
    }

    #[test]
    fn parses_kraken_bbo_with_timestamp() {
        let input = br#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":100.0,"ask":101.0,"timestamp":"2023-11-14T22:13:20.000000Z"}]}"#;
        assert_eq!(
            parse_external(Venue::Kraken, input),
            Some(("BTC/USD", 100.0, 101.0, Some(1_700_000_000_000)))
        );
    }

    #[test]
    fn parses_mexc_protobuf_bbo() {
        let payload = MexcPush {
            symbol: Some("BTCUSDT".to_owned()),
            send_time: Some(1_700_000_000_000),
            ticker: Some(MexcBookTicker {
                bid_price: "100.0".to_owned(),
                ask_price: "101.0".to_owned(),
            }),
        }
        .encode_to_vec();
        assert_eq!(
            parse_mexc_bbo(&payload),
            Some(("BTCUSDT".to_owned(), 100.0, 101.0, Some(1_700_000_000_000)))
        );
    }

    #[test]
    fn rfc3339_millis_matches_known_epoch() {
        // 2023-11-14T22:13:20Z == 1_700_000_000 s.
        assert_eq!(
            parse_rfc3339_millis("2023-11-14T22:13:20.000000Z"),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            parse_rfc3339_millis("2023-11-14T22:13:20.123Z"),
            Some(1_700_000_000_123)
        );
        // No fractional part is fine.
        assert_eq!(
            parse_rfc3339_millis("2023-11-14T22:13:20Z"),
            Some(1_700_000_000_000)
        );
        // A malformed non-ASCII fraction must not panic (it did when byte-sliced).
        assert!(parse_rfc3339_millis("2023-11-14T22:13:20.é9Z").is_some());
    }

    #[test]
    fn parses_official_hyperliquid_oracle() {
        let input =
            br#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"oraclePx":"100000"}}}"#;
        let Some(HyperliquidMessage::Official { coin, oracle }) = parse_hyperliquid(input) else {
            panic!("missing official update")
        };
        assert_eq!((coin, oracle), ("BTC", 100000.0));
    }

    #[test]
    fn parses_numeric_hyperliquid_oracle() {
        let input =
            br#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"oraclePx":100000.5}}}"#;
        let Some(HyperliquidMessage::Official { coin, oracle }) = parse_hyperliquid(input) else {
            panic!("missing official update")
        };
        assert_eq!((coin, oracle), ("BTC", 100000.5));
    }
}
