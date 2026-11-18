//! The node's Prometheus endpoint and health check (what live-ingestion's alerts and the
//! StatefulSet probes read in place of Redpanda's and kafka-exporter's metrics).

use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let code = out.split(' ').nth(1).unwrap().parse().unwrap();
    (code, out.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_report_appends_dedup_and_storage_and_health_follows_the_fence() {
    let d = tempfile::tempdir().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut c = NodeConfig::single(0, "a", d.path().to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c.listen_metrics = Some(format!("127.0.0.1:{port}").parse().unwrap());
    c.storage_failpoints.fail_data_sync_after = Some(2);
    let node = Node::open(c).await.unwrap();
    node.spawn_listeners().await.unwrap();
    let addr = node.cfg.listen_metrics.unwrap();
    let log = |w: &str| LogId {
        stream: "raw.trades.x".into(),
        writer: w.into(),
        epoch: 1,
    };
    for w in ["a", "b"] {
        let b = Batch::encode_keyed(&log(w), 0, &[(1, b"id1", b"x")]).unwrap();
        assert!(node.append(1, b).await.is_success());
    }
    let (code, body) = get(addr, "/metrics").await;
    assert_eq!(code, 200);
    for line in [
        "precog_batches 2",
        "precog_dedup_dropped_total 1",
        "precog_fatal 0",
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in\n{body}"
        );
    }
    assert!(body.contains("precog_dedup_window_seconds "), "{body}");
    assert!(body.contains("precog_fsyncs_total "), "{body}");
    assert_eq!(get(addr, "/health").await.0, 200);
    // the third append's fsync fails: the node is fenced and must be restarted
    let b = Batch::encode(&log("c"), 0, &[(1, b"x")]).unwrap();
    assert!(!node.append(1, b).await.is_success());
    assert_eq!(get(addr, "/health").await.0, 503);
    assert!(
        get(addr, "/metrics")
            .await
            .1
            .lines()
            .any(|l| l == "precog_fatal 1")
    );
    assert_eq!(get(addr, "/nope").await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_report_each_groups_lag_behind_its_stream() {
    // what kafka-exporter gives live-ingestion's alerts today: per consumer group and topic, lag
    let d = tempfile::tempdir().unwrap();
    let mut c = NodeConfig::single(0, "a", d.path().to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    let node = Node::open(c).await.unwrap();
    let log = LogId {
        stream: "raw.trades.x".into(),
        writer: "p".into(),
        epoch: 1,
    };
    for seq in (0..10).step_by(2) {
        let b = Batch::encode(&log, seq, &[(1, b"a"), (2, b"b")]).unwrap();
        assert!(node.append(1, b).await.is_success());
    }
    let pos: precog_mesh::store::Cursor = [(log, 6)].into_iter().collect();
    node.commit_position("raw-archiver-trades-x", "raw.trades.x", &pos)
        .await
        .unwrap();
    let m = node.metrics();
    let lag = r#"precog_group_lag_events{group="raw-archiver-trades-x",stream="raw.trades.x"} 4"#;
    assert!(m.lines().any(|l| l == lag), "missing {lag:?} in\n{m}");
    assert!(m.contains(r#"precog_group_commit_age_seconds{group="raw-archiver-trades-x",stream="raw.trades.x"} "#), "{m}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_split_across_tcp_writes_is_read_whole() {
    // a probe's request can arrive in pieces: the path is read only once the headers end
    let d = tempfile::tempdir().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut c = NodeConfig::single(0, "a", d.path().to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c.listen_metrics = Some(format!("127.0.0.1:{port}").parse().unwrap());
    let node = Node::open(c).await.unwrap();
    node.spawn_listeners().await.unwrap();
    let mut s = tokio::net::TcpStream::connect(node.cfg.listen_metrics.unwrap())
        .await
        .unwrap();
    s.set_nodelay(true).unwrap();
    s.write_all(b"GET /hea").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    s.write_all(b"lth HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
}
