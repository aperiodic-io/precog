//! The node's Prometheus endpoint and health check (what live-ingestion's alerts and the
//! StatefulSet probes read in place of Redpanda's and kafka-exporter's metrics).

use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A free port, held (bound with SO_REUSEADDR, as the node binds, but not listening) for as
/// long as the socket lives, so no test running in parallel takes it before the node binds it.
fn reserve() -> (tokio::net::TcpSocket, std::net::SocketAddr) {
    let s = tokio::net::TcpSocket::new_v4().unwrap();
    s.set_reuseaddr(true).unwrap();
    s.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let a = s.local_addr().unwrap();
    (s, a)
}

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
    let (_held, addr) = reserve();
    let mut c = NodeConfig::single(0, "a", d.path().to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c.listen_metrics = Some(addr);
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
async fn a_request_split_across_tcp_writes_is_read_whole() {
    // a probe's request can arrive in pieces: the path is read only once the headers end
    let d = tempfile::tempdir().unwrap();
    let (_held, addr) = reserve();
    let mut c = NodeConfig::single(0, "a", d.path().to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c.listen_metrics = Some(addr);
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
