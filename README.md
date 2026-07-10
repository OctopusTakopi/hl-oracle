# hl-oracle

A local reconstruction of Hyperliquid's spot oracle, and a monitor that compares it against the oracle Hyperliquid actually publishes.

The feed subscribes to top-of-book data from Binance, OKX, Bybit, Kraken, KuCoin, Gate, and MEXC, takes the mid of each book, and computes a weighted median. Hyperliquid documents the same venues and weights for its spot oracle:

| Venue | Weight |
| --- | ---: |
| Binance | 3 |
| OKX | 2 |
| Bybit | 2 |
| Kraken | 1 |
| KuCoin | 1 |
| Gate | 1 |
| MEXC | 1 |
| Hyperliquid spot | 1 |

It also subscribes to Hyperliquid's `activeAssetCtx` and reports `oraclePx` next to the local number. The weighting and source rules follow the [Hyperliquid oracle docs](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/oracle).

## Build and run

```sh
cargo build --release

# JSON Lines on stdout, logs on stderr
target/release/hl-oracle BTC ETH SOL

# Terminal monitor
target/release/oracle-monitor BTC ETH SOL
```

Quit the monitor with `q` or `Esc`.

## Use as a library

Add the crate and drive the feed directly. `OracleBuilder` configures it and `OracleFeed` runs the readers in the background; `recv` returns each time a coin's comparison changes.

```rust
use hl_oracle::OracleBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut feed = OracleBuilder::new(["BTC", "ETH"])
        .max_quote_age(std::time::Duration::from_millis(1_500))
        .spawn()?;

    while let Some(update) = feed.recv().await {
        let coin = feed.coin(update.asset).unwrap_or("?");
        println!("{coin}: local {:.2}, official {:?}", update.comparison.local, update.comparison.official);
    }
    Ok(())
}
```

`feed.latest(asset)` reads a coin's current comparison without waiting for the next change. For the raw update stream (individual venue quotes), use `sources::run_all` and `oracle::OracleBook` directly, as `oracle-monitor` does. A runnable version of the above is in `examples/feed.rs`:

```sh
cargo run --example feed -- BTC ETH SOL
```

## Output

`hl-oracle` writes one JSON object per line, emitted whenever the local value, the official value, or the set of fresh sources changes. It is not a raw quote recorder.

```json
{
  "coin": "BTC",
  "local_oracle": 100000.0,
  "hyperliquid_oracle": 99998.0,
  "difference_bps": 0.2,
  "sources": 7,
  "weight": 11,
  "expected_sources": 7,
  "expected_weight": 11,
  "active_sources": ["binance", "okx", "bybit", "kraken", "kucoin", "gate", "mexc"],
  "dropped_updates": 0
}
```

`sources` and `weight` count only fresh quotes; `expected_sources` and `expected_weight` describe the configured policy for the coin. A quote drops out when its book is invalid, crossed, or older than `--max-quote-age-ms` (default 1500).

## Source policy

Hyperliquid leaves external venues out of the oracle for assets whose main spot market is on Hyperliquid (HYPE, currently), and leaves Hyperliquid spot out for everything else. hl-oracle follows both rules:

- BTC and similar assets use the seven external venues.
- HYPE defaults to `HYPE/USDC` on Hyperliquid spot alone.

The public API does not expose when an external-primary asset has enough Hyperliquid liquidity to flip, so that switch is manual:

```sh
target/release/hl-oracle HYPE --include-external-for HYPE
```

Add more spot mappings with `--hl-spot COIN=SPOT_COIN`.

## Timing

- `--max-quote-age-ms` (default 1500) is the freshness window for exchange and Hyperliquid-spot books.
- `--max-official-age-ms` (default 6000) is the freshness window for `oraclePx`. Hyperliquid publishes it every three seconds, so it needs a looser bound than the books.
- `--queue-capacity` (default 8192) sizes the channel between the WebSocket readers and the aggregator.

## Monitor

The top table shows local and official prices, their difference in basis points, the fresh-versus-expected source counts, and, on a terminal at least 165 columns wide, per-feed ages, the local-versus-official receipt gap, the rolling mean and max absolute error, and update counts.

`Lead/corr` is a rolling cross-correlation between the official and local log returns. For each pair of consecutive oracle moves it correlates the official return with the local return over the same interval shifted by a candidate lag, scanning -3s to +6s in 100ms steps over five minutes of history. A positive lag means the local return moved first. It reports a lag only when the correlation is at least 0.30, improves by at least 0.05 over zero lag, and holds across three candidate windows in a row; otherwise it shows `inconclusive`.

Warm-up is timed, not counted. The oracle publishes every ~3 seconds but usually repeats its last value — in practice it only *changes* on the order of twenty times in five minutes — so a target phrased as a number of matched intervals is never reached and the panel would sit on `warming up` forever. Instead it warms up until the oracle history spans two minutes, then reports a reading (or `inconclusive`) from whatever moves occurred. It is evidence of aligned price moves, not a measurement of validator or network latency.

`L-HL gap` is the difference between the most recent local quote and the most recent official update, by local receipt time. It is useful for watching feed timing, not for measuring validator-side publication latency.

Below the table, the sources panel shows two numbers per venue and coin: network latency (local receipt time minus the exchange's send timestamp) and the time since the last update from that venue. Binance's bookTicker carries no timestamp, so its latency shows `-` while its age still updates. The latency reading only means anything on an NTP-synced host.

Select a coin with the arrow keys and press `Enter` for a chart of the local oracle against the Hyperliquid oracle over the last five minutes. Both are drawn as step lines, since the oracle moves in discrete three-second steps and the books hold their last price between updates.

## Limits

- The reader-to-aggregator queue is bounded. When the aggregator falls behind, new updates are dropped rather than blocking a reader, and the running count shows up as `dropped_updates` and in the monitor footer.
- One process handles at most 30 external coins, matching MEXC's per-connection subscription limit. Larger configurations are rejected at startup.
- Feeds reconnect with capped exponential backoff.
- KuCoin hands out a short-lived token from its `bullet-public` endpoint, fetched before each connection.
- KuCoin's ticker timestamp is the last-trade time rather than a send time, so its latency reading reflects trade recency, not transport delay. The "since last update" column shows the feed is still live.
- MEXC uses its protobuf book-ticker feed; the other venues use public JSON.

## Scope

This is not the clearinghouse oracle. Hyperliquid takes a stake-weighted median across validator submissions, may apply source-eligibility rules that are not public, and mixes USD, USDT, and USDC markets. Treat it as a reference feed and keep comparing it against `oraclePx`.
