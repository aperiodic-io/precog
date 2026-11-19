//! Native-protocol proofs of concept (`src/bin/proto.rs`): a deterministic market-data-like
//! record set and the batch encodings the POCs compare (`codec`).

pub mod codec;

/// One producer event: what a live-producer replica hands the client library.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub time_us: u64,
    /// Index into [`SYMBOLS`]; the symbol is also inside the protobuf payload.
    pub symbol: u8,
    /// Dedup id, e.g. `binance-futures-p3-123456`.
    pub id: String,
    /// A protobuf-encoded trade or quote, ~150-260 bytes.
    pub payload: Vec<u8>,
}

pub const SYMBOLS: [&str; 24] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "DOTUSDT",
    "TRXUSDT",
    "MATICUSDT",
    "NEARUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "SUIUSDT",
    "PEPEUSDT",
    "WIFUSDT",
    "INJUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "FILUSDT",
];

fn splitmix(s: &mut u64) -> u64 {
    *s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn pb_str(out: &mut Vec<u8>, field: u8, s: &str) {
    out.push(field << 3 | 2);
    varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn pb_int(out: &mut Vec<u8>, field: u8, v: u64) {
    out.push(field << 3);
    varint(out, v);
}

/// `n` records, the same for a given seed: ~1 event per 5 µs, a skewed symbol mix, 70% trades
/// and 30% book tickers, each carrying the exchange's raw JSON as most protobuf records do.
pub fn records(n: usize, seed: u64) -> Vec<Record> {
    let mut s = seed;
    let mut time_us = 1_763_000_000_000_000u64;
    let mut trade_id = 5_000_000_000u64;
    (0..n)
        .map(|i| {
            time_us += splitmix(&mut s) % 10;
            // Skewed: half the events on the first 4 symbols.
            let r = splitmix(&mut s);
            let symbol = if r.is_multiple_of(2) { r / 2 % 4 } else { r / 2 % 24 } as u8;
            let sym = SYMBOLS[symbol as usize];
            let ms = time_us / 1000;
            let price = 100_000 + splitmix(&mut s) % 9_000_000;
            let (whole, frac) = (price / 100, price % 100);
            let qty = 1 + splitmix(&mut s) % 50_000;
            let mut p = Vec::with_capacity(256);
            pb_str(&mut p, 1, "binance-futures");
            pb_str(&mut p, 2, sym);
            pb_int(&mut p, 3, ms);
            pb_int(&mut p, 4, time_us);
            let raw = if splitmix(&mut s) % 10 < 7 {
                trade_id += 1 + splitmix(&mut s) % 3;
                pb_str(&mut p, 5, &format!("{whole}.{frac:02}"));
                pb_str(&mut p, 6, &format!("0.{qty:05}"));
                pb_int(&mut p, 7, trade_id);
                pb_int(&mut p, 8, splitmix(&mut s) % 2);
                format!(
                    r#"{{"e":"aggTrade","E":{ms},"a":{trade_id},"s":"{sym}","p":"{whole}.{frac:02}","q":"0.{qty:05}","f":{},"l":{},"T":{},"m":{}}}"#,
                    trade_id * 2,
                    trade_id * 2 + 1,
                    ms - 1,
                    qty.is_multiple_of(2)
                )
            } else {
                let ask = price + 1 + splitmix(&mut s) % 20;
                format!(
                    r#"{{"e":"bookTicker","u":{},"s":"{sym}","b":"{whole}.{frac:02}","B":"{qty}","a":"{}.{:02}","A":"{}","T":{ms},"E":{ms}}}"#,
                    trade_id * 7,
                    ask / 100,
                    ask % 100,
                    qty / 3 + 1
                )
            };
            pb_str(&mut p, 9, &raw);
            Record {
                time_us,
                symbol,
                id: format!("binance-futures-p{}-{}", symbol % 6, 100_000 + i),
                payload: p,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_deterministic_and_market_data_sized() {
        let a = records(10_000, 7);
        assert_eq!(a, records(10_000, 7), "every POC must see the same records");
        let mean = a.iter().map(|r| r.payload.len()).sum::<usize>() / a.len();
        assert!((150..=260).contains(&mean), "mean payload {mean} B");
        assert!(a.iter().all(|r| (100..=300).contains(&r.payload.len())));
        assert!(a.windows(2).all(|w| w[0].time_us <= w[1].time_us));
        assert!(a[0].id.starts_with("binance-futures-p"));
    }
}
