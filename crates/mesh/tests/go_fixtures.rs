//! Golden bytes for the Go client (`clients/go`): batches, frames, control JSON and xxh3 values
//! as this crate produces them. The Go tests encode the same inputs and must match byte for byte.
//! Fails when the committed fixture drifts from the Rust encoders; regenerate it with
//! `PRECOG_WRITE_FIXTURES=1 cargo test -p precog-mesh --test go_fixtures`.

use bytes::{Bytes, BytesMut};
use precog_mesh::proto::{
    Batch, Control, ControlResp, Frame, Hello, Kind, LogId, encode_append, hex_encode,
};
use serde_json::json;
use xxhash_rust::xxh3::xxh3_64;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../clients/go/testdata/golden.json"
);

fn hex(b: &[u8]) -> String {
    hex_encode(b)
}

fn golden() -> serde_json::Value {
    let log = LogId {
        stream: "raw.trades.x".into(),
        writer: "producer-a".into(),
        epoch: 1_700_000_000_123_456_789,
    };
    let key = xxh3_64(b"trade-1").to_le_bytes();
    // 7 payload bytes: too short for an lz4 match, so every compressor writes the same block
    let small: [(u64, &[u8], &[u8]); 3] = [
        (1_700_000_000_000_001, &key, b"t1"),
        (2, b"", b""),
        (3, b"k", b"hello"),
    ];
    let pcb2 = Batch::encode_keyed(&log, 42, &small).unwrap();
    let pcz2 = Batch::encode_compressed(&log, 42, &small).unwrap();
    let rep = "abc".repeat(40);
    let matches: [(u64, &[u8], &[u8]); 3] = [
        (10, b"a", rep.as_bytes()),
        (11, b"b", rep.as_bytes()),
        (12, b"c", rep.as_bytes()),
    ];
    let pcz2_matches = Batch::encode_compressed(&log, 0, &matches).unwrap();
    let drop = [false, true, false];
    let pcb2_dropped = Batch::parse(pcb2.clone()).unwrap().without(&drop).unwrap();
    let pcz2_dropped = Batch::parse(pcz2_matches.clone())
        .unwrap()
        .without(&drop)
        .unwrap();
    let mut frame = BytesMut::new();
    Frame::new(Kind::Append, 7, encode_append(2, &pcb2)).encode_into(&mut frame);

    let cursor = vec![("producer-a".to_string(), 7u64, 3u64), ("b".into(), 1, 0)];
    let controls = json!({
        "read_cursor": serde_json::to_string(&Control::ReadCursor {
            stream: "s".into(), cursor: cursor.clone(), max_bytes: 1024 }).unwrap(),
        "read_cursor_empty": serde_json::to_string(&Control::ReadCursor {
            stream: "s".into(), cursor: vec![], max_bytes: 1 }).unwrap(),
        "cursor_at": serde_json::to_string(&Control::CursorAt {
            stream: "s".into(), time_ms: u64::MAX }).unwrap(),
        "commit": serde_json::to_string(&Control::Commit {
            group: "g".into(), stream: "s".into(), cursor: cursor.clone() }).unwrap(),
        "position": serde_json::to_string(&Control::Position {
            group: "g".into(), stream: "s".into() }).unwrap(),
        "latest": serde_json::to_string(&Control::Latest { stream: "s".into() }).unwrap(),
    });
    let resps = json!({
        "cursor": serde_json::to_string(&ControlResp::Cursor { cursor: Some(cursor) }).unwrap(),
        "no_cursor": serde_json::to_string(&ControlResp::Cursor { cursor: None }).unwrap(),
        "committed": serde_json::to_string(&ControlResp::Committed).unwrap(),
        "latest": serde_json::to_string(&ControlResp::Latest {
            entries: vec![(b"k1".to_vec(), b"v\xff".to_vec())] }).unwrap(),
    });
    let xxh3: Vec<_> = [0usize, 1, 3, 7, 16, 17, 128, 129, 240, 241, 1000]
        .iter()
        .map(|&n| (0..n).map(|i| (i * 7 % 251) as u8).collect::<Vec<u8>>())
        .chain([b"raw.trades.x".to_vec(), b"trade-1".to_vec()])
        .map(|v| json!({ "in": hex(&v), "hash": xxh3_64(&v).to_string() }))
        .collect();
    json!({
        "pcb2": hex(&pcb2),
        "pcz2": hex(&pcz2),
        "pcz2_matches": hex(&pcz2_matches),
        "pcb2_dropped": hex(&pcb2_dropped),
        "pcz2_dropped": hex(&pcz2_dropped),
        "append_frame": hex(&frame),
        "hello": serde_json::to_string(&Hello { token: "tok".into(), node_id: None }).unwrap(),
        "controls": controls,
        "responses": resps,
        "xxh3": xxh3,
    })
}

#[test]
fn go_client_golden_fixture_matches_the_rust_encoders() {
    let want = serde_json::to_string_pretty(&golden()).unwrap() + "\n";
    if std::env::var_os("PRECOG_WRITE_FIXTURES").is_some() {
        std::fs::write(FIXTURE, &want).unwrap();
    }
    let have = std::fs::read_to_string(FIXTURE).unwrap_or_default();
    assert!(
        have == want,
        "{FIXTURE} is stale; regenerate it (see this file's header)"
    );
    // the dropped fixtures really carry dropped events
    for k in ["pcb2_dropped", "pcz2_dropped"] {
        let h = golden()[k].as_str().unwrap().to_string();
        let b: Vec<u8> = (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(Batch::parse(Bytes::from(b)).unwrap().events().count(), 2);
    }
}
