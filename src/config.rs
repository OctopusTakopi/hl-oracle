use std::collections::BTreeSet;

use anyhow::{Result, bail};

use crate::sources::Asset;

pub fn build_assets(
    coins: &[String],
    mappings: &[String],
    external_for: &[String],
) -> Result<Vec<Asset>> {
    let mut spot = vec![("HYPE".to_owned(), "HYPE/USDC".to_owned())];
    for mapping in mappings {
        let (coin, symbol) = mapping.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid --hl-spot mapping {mapping:?}; expected COIN=SPOT_COIN")
        })?;
        let coin = coin.to_ascii_uppercase();
        let symbol = symbol.to_ascii_uppercase();
        if !valid_coin(&coin) || !valid_spot_symbol(&symbol) {
            bail!("invalid --hl-spot mapping {mapping:?}");
        }
        spot.retain(|(known, _)| known != &coin);
        spot.push((coin, symbol));
    }

    let mut seen = BTreeSet::new();
    let coins: Vec<String> = coins
        .iter()
        .map(|coin| coin.to_ascii_uppercase())
        .map(|coin| {
            if !valid_coin(&coin) {
                bail!("unsupported coin name {coin:?}");
            }
            if !seen.insert(coin.clone()) {
                bail!("duplicate coin {coin:?}");
            }
            Ok(coin)
        })
        .collect::<Result<_>>()?;

    let external_for: BTreeSet<String> = external_for
        .iter()
        .map(|coin| {
            let coin = coin.to_ascii_uppercase();
            if !valid_coin(&coin) || !coins.contains(&coin) {
                bail!("--include-external-for must name a requested coin: {coin:?}");
            }
            Ok(coin)
        })
        .collect::<Result<_>>()?;

    let assets: Vec<Asset> = coins
        .iter()
        .map(|coin| {
            let hyperliquid_spot = spot
                .iter()
                .find(|(mapped, _)| mapped == coin)
                .map(|(_, symbol)| symbol.clone());
            let include_external = hyperliquid_spot.is_none() || external_for.contains(coin);
            Ok(Asset {
                coin: coin.clone(),
                hyperliquid_spot,
                include_external,
            })
        })
        .collect::<Result<_>>()?;

    if assets.iter().filter(|asset| asset.include_external).count() > 30 {
        bail!(
            "at most 30 external coins are supported per process because MEXC limits a WebSocket connection to 30 subscriptions"
        );
    }
    Ok(assets)
}

fn valid_coin(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b':' || byte == b'-')
}

fn valid_spot_symbol(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'-' | b'_' | b'/'))
}

#[cfg(test)]
mod tests {
    use super::build_assets;

    #[test]
    fn hyperliquid_primary_spot_defaults_to_no_external_sources() {
        let assets = build_assets(&["HYPE".to_owned(), "BTC".to_owned()], &[], &[]).unwrap();
        assert_eq!(assets[0].expected_source_count(), 1);
        assert_eq!(assets[0].expected_source_weight(), 1);
        assert_eq!(assets[1].expected_source_count(), 7);
        assert_eq!(assets[1].expected_source_weight(), 11);
    }

    #[test]
    fn external_override_is_explicit() {
        let assets = build_assets(&["HYPE".to_owned()], &[], &["HYPE".to_owned()]).unwrap();
        assert_eq!(assets[0].expected_source_count(), 8);
        assert_eq!(assets[0].expected_source_weight(), 12);
    }

    #[test]
    fn duplicate_coins_are_rejected() {
        assert!(build_assets(&["BTC".to_owned(), "btc".to_owned()], &[], &[]).is_err());
    }
}
