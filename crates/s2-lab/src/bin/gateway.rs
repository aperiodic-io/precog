//! An S2 REST v1 subset served over Precog (evidence, not product).
//!
//! One S2 stream `basin/stream` is one Precog writer log `(basin/stream, "s2", 0)` whose
//! sequence numbers are the stream's seq_nums. Each stream has a streamer task (as s2-lite
//! has) that owns a `precog_lab::s2::Sequencer`: it assigns seq_nums and timestamps, checks
//! `match_seq_num` and the fencing token, then hands the batch to a durability sink with many
//! batches in flight, and acknowledges in seq order once each is durable.
//!
//! Sinks: `mesh` (Precog, `copies` of the nodes durable before the ack), `s3` (a group-commit
//! write-ahead log of objects on an S3-compatible store: one PUT per commit), `none` (ack at
//! once: the HTTP and sequencing overhead alone). Reads are served from the mesh only.
//!
//!   gateway --listen 0.0.0.0:8080 --sink mesh --nodes tcp://a:7000,... [--copies 2]
//!   gateway --sink s3 --s3-endpoint http://silo:9000 --bucket b [--flush-ms 0]

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use precog_client::{Client, Cursor};
use precog_lab::s2::{Record, Reject, Sequencer};
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{Batch, LogId};
use precog_s2_lab::*;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, oneshot, watch};

fn arg(k: &str, d: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == k)
        .map_or(d.to_string(), |i| a[i + 1].clone())
}

type Reply = oneshot::Sender<std::result::Result<AppendAck, (StatusCode, serde_json::Value)>>;

struct Req {
    records: Vec<(Option<u64>, Record)>,
    match_seq_num: Option<u64>,
    fencing_token: Option<String>,
    reply: Reply,
}

struct Stream {
    tx: mpsc::Sender<Req>,
    tail: watch::Receiver<Pos>,
    live: broadcast::Sender<Arc<Vec<SequencedRecord>>>,
}

enum Sink {
    Mesh { mc: Arc<MeshClient>, copies: u8 },
    S3(mpsc::Sender<(Bytes, oneshot::Sender<bool>)>),
    None,
}

impl Sink {
    async fn append(&self, stream: &str, batch: Bytes) -> bool {
        match self {
            Sink::Mesh { mc, copies } => {
                let n = mc.node_count() as u64;
                let preferred = (xxhash_rust::xxh3::xxh3_64(stream.as_bytes()) % n) as usize;
                mc.append(preferred, &batch, *copies).await.is_ok()
            }
            Sink::S3(tx) => {
                let (r, w) = oneshot::channel();
                tx.send((batch, r)).await.is_ok() && w.await.unwrap_or(false)
            }
            Sink::None => true,
        }
    }
}

/// Group commit to object storage: everything queued while a PUT slot is busy (or within
/// `flush_ms` of the first queued batch) goes into one object; waiters are acked after its PUT.
/// ponytail: objects are never read back, compacted or deleted: the WAL's latency and PUT rate
/// are what the POC measures.
fn s3_wal(store: Arc<dyn object_store::ObjectStore>, flush_ms: u64, slots: usize) -> Sink {
    let (tx, mut rx) = mpsc::channel::<(Bytes, oneshot::Sender<bool>)>(65536);
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(slots));
        let mut n = 0u64;
        while let Some(first) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let mut group = vec![first];
            let deadline = tokio::time::Instant::now() + Duration::from_millis(flush_ms);
            let mut size = group[0].0.len();
            while size < 8 << 20 {
                match rx.try_recv() {
                    Ok(x) => {
                        size += x.0.len();
                        group.push(x);
                    }
                    Err(_) if flush_ms == 0 => break,
                    Err(_) => match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(x)) => {
                            size += x.0.len();
                            group.push(x);
                        }
                        _ => break,
                    },
                }
            }
            let mut buf = Vec::with_capacity(size + 4 * group.len());
            for (b, _) in &group {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            n += 1;
            let path = object_store::path::Path::from(format!("wal/{n:020}"));
            let store = store.clone();
            tokio::spawn(async move {
                let ok = store.put(&path, buf.into()).await.is_ok();
                drop(permit);
                for (_, w) in group {
                    let _ = w.send(ok);
                }
            });
        }
    });
    Sink::S3(tx)
}

struct App {
    sink: Sink,
    reader: Option<Client>,
    streams: Mutex<HashMap<String, Arc<Stream>>>,
}

fn log_of(key: &str) -> LogId {
    LogId {
        stream: key.into(),
        writer: "s2".into(),
        epoch: 0,
    }
}

fn to_seq(e: &precog_mesh::proto::Event) -> SequencedRecord {
    let r = Record::decode(&e.payload).unwrap_or_default();
    let s = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    SequencedRecord {
        seq_num: e.seq,
        timestamp: e.event_time_us / 1000,
        headers: r.headers.iter().map(|(k, v)| (s(k), s(v))).collect(),
        body: s(&r.body),
    }
}

impl App {
    /// Records `from..until` (at most `limit`), read through the mesh.
    async fn read(
        &self,
        key: &str,
        from: u64,
        until: u64,
        limit: usize,
    ) -> Result<Vec<SequencedRecord>> {
        let reader = self.reader.as_ref().context("this sink serves no reads")?;
        let mut cursor = Cursor::new();
        cursor.insert(log_of(key), from);
        let mut c = reader.consumer_at(key, cursor);
        let mut out = vec![];
        while out.len() < limit && from + (out.len() as u64) < until {
            let evs = c.poll(1 << 20).await?;
            if evs.is_empty() {
                break;
            }
            out.extend(
                evs.iter()
                    .filter(|e| e.seq >= from && e.seq < until)
                    .map(to_seq),
            );
        }
        out.truncate(limit);
        Ok(out)
    }

    async fn stream(self: &Arc<Self>, key: &str) -> Result<Arc<Stream>> {
        let mut g = self.streams.lock().await;
        if let Some(s) = g.get(key) {
            return Ok(s.clone());
        }
        // a new owner recovers the tail, the fencing token and the clock from the log
        let seq = if self.reader.is_some() {
            let recs = self.read(key, 0, u64::MAX, usize::MAX).await?;
            let rs: Vec<(u64, u64, Record)> = recs
                .iter()
                .map(|r| {
                    let rec = Record {
                        headers: r
                            .headers
                            .iter()
                            .map(|(k, v)| (k.clone().into_bytes(), v.clone().into_bytes()))
                            .collect(),
                        body: r.body.clone().into_bytes(),
                    };
                    (r.seq_num, r.timestamp, rec)
                })
                .collect();
            Sequencer::recover(rs.iter().map(|(s, t, r)| (*s, *t, r)))
        } else {
            Sequencer::new()
        };
        let (t, ts) = seq.tail();
        let (tail_tx, tail) = watch::channel(Pos {
            seq_num: t,
            timestamp: ts,
        });
        let (tx, rx) = mpsc::channel(1024);
        let (live, _) = broadcast::channel(1024);
        let s = Arc::new(Stream {
            tx,
            tail,
            live: live.clone(),
        });
        tokio::spawn(streamer(
            self.clone(),
            key.to_string(),
            rx,
            seq,
            tail_tx,
            live,
        ));
        g.insert(key.to_string(), s.clone());
        Ok(s)
    }
}

async fn streamer(
    app: Arc<App>,
    key: String,
    mut rx: mpsc::Receiver<Req>,
    mut seq: Sequencer,
    tail_tx: watch::Sender<Pos>,
    live: broadcast::Sender<Arc<Vec<SequencedRecord>>>,
) {
    let log = log_of(&key);
    let poisoned = Arc::new(AtomicBool::new(false));
    // acks leave in seq order: a batch is acknowledged only after every earlier one is durable
    type Done = (
        tokio::task::JoinHandle<bool>,
        AppendAck,
        Reply,
        Arc<Vec<SequencedRecord>>,
    );
    let (done_tx, mut done_rx) = mpsc::channel::<Done>(256);
    let p = poisoned.clone();
    tokio::spawn(async move {
        while let Some((h, ack, reply, recs)) = done_rx.recv().await {
            if h.await.unwrap_or(false) && !p.load(Ordering::Relaxed) {
                let _ = tail_tx.send(ack.end);
                let _ = live.send(recs);
                let _ = reply.send(Ok(AppendAck {
                    tail: ack.end,
                    ..ack
                }));
            } else {
                // ponytail: a failed or unknown write leaves a hole the sequencer has already
                // numbered past; the stream refuses further appends. A product re-reads the
                // tail on a quorum and continues under a new epoch (docs/S2-PLAN.md, F2).
                p.store(true, Ordering::Relaxed);
                let _ = reply.send(Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!("stream fenced after a failed write"),
                )));
            }
        }
    });
    while let Some(req) = rx.recv().await {
        if poisoned.load(Ordering::Relaxed) {
            let _ = req.reply.send(Err((
                StatusCode::SERVICE_UNAVAILABLE,
                json!("stream fenced after a failed write"),
            )));
            continue;
        }
        let refs: Vec<(Option<u64>, &Record)> = req.records.iter().map(|(t, r)| (*t, r)).collect();
        let now = now_us() / 1000;
        let a = match seq.admit(
            &refs,
            req.match_seq_num,
            req.fencing_token.as_deref().map(str::as_bytes),
            now,
        ) {
            Ok(a) => a,
            Err(r) => {
                let e = match r {
                    Reject::SeqNumMismatch(t) => (
                        StatusCode::PRECONDITION_FAILED,
                        json!({"seq_num_mismatch": t}),
                    ),
                    Reject::FencingTokenMismatch(t) => (
                        StatusCode::PRECONDITION_FAILED,
                        json!({"fencing_token_mismatch": String::from_utf8_lossy(&t)}),
                    ),
                    Reject::Invalid(m) => (StatusCode::BAD_REQUEST, json!(m)),
                };
                let _ = req.reply.send(Err(e));
                continue;
            }
        };
        let payloads: Vec<Vec<u8>> = req.records.iter().map(|(_, r)| r.encode()).collect();
        let evs: Vec<(u64, &[u8], &[u8])> = payloads
            .iter()
            .zip(&a.timestamps)
            .map(|(p, t)| (t * 1000, &[][..], &p[..]))
            .collect();
        let batch = Batch::encode_keyed(&log, a.start, &evs).expect("encode");
        let recs: Arc<Vec<SequencedRecord>> = Arc::new(
            req.records
                .iter()
                .zip(a.start..)
                .zip(&a.timestamps)
                .map(|(((_, r), s), t)| {
                    let st = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
                    SequencedRecord {
                        seq_num: s,
                        timestamp: *t,
                        headers: r.headers.iter().map(|(k, v)| (st(k), st(v))).collect(),
                        body: st(&r.body),
                    }
                })
                .collect(),
        );
        let last_ts = *a.timestamps.last().unwrap();
        let ack = AppendAck {
            start: Pos {
                seq_num: a.start,
                timestamp: a.timestamps[0],
            },
            end: Pos {
                seq_num: a.end,
                timestamp: last_ts,
            },
            tail: Pos {
                seq_num: a.end,
                timestamp: last_ts,
            },
        };
        let (app2, key2) = (app.clone(), key.clone());
        let h = tokio::spawn(async move { app2.sink.append(&key2, batch).await });
        if done_tx.send((h, ack, req.reply, recs)).await.is_err() {
            break;
        }
    }
}

fn basin(h: &HeaderMap) -> String {
    h.get("s2-basin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default")
        .to_string()
}

fn err(code: StatusCode, v: serde_json::Value) -> Response {
    (code, Json(v)).into_response()
}

async fn append(
    State(app): State<Arc<App>>,
    Path(stream): Path<String>,
    h: HeaderMap,
    Json(input): Json<AppendInput>,
) -> Response {
    let key = format!("{}/{}", basin(&h), stream);
    let s = match app.stream(&key).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::SERVICE_UNAVAILABLE, json!(e.to_string())),
    };
    let records = input
        .records
        .into_iter()
        .map(|r| {
            (
                r.timestamp,
                Record {
                    headers: r
                        .headers
                        .into_iter()
                        .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
                        .collect(),
                    body: r.body.into_bytes(),
                },
            )
        })
        .collect();
    let (reply, rx) = oneshot::channel();
    let req = Req {
        records,
        match_seq_num: input.match_seq_num,
        fencing_token: input.fencing_token,
        reply,
    };
    if s.tx.send(req).await.is_err() {
        return err(StatusCode::SERVICE_UNAVAILABLE, json!("streamer gone"));
    }
    match rx.await {
        Ok(Ok(ack)) => Json(ack).into_response(),
        Ok(Err((c, v))) => err(c, v),
        Err(_) => err(StatusCode::SERVICE_UNAVAILABLE, json!("dropped")),
    }
}

#[derive(Deserialize)]
struct ReadQ {
    seq_num: Option<u64>,
    tail_offset: Option<u64>,
    count: Option<usize>,
    clamp: Option<bool>,
}

async fn check_tail(
    State(app): State<Arc<App>>,
    Path(stream): Path<String>,
    h: HeaderMap,
) -> Response {
    let key = format!("{}/{}", basin(&h), stream);
    match app.stream(&key).await {
        Ok(s) => Json(json!({ "tail": *s.tail.borrow() })).into_response(),
        Err(e) => err(StatusCode::SERVICE_UNAVAILABLE, json!(e.to_string())),
    }
}

async fn read(
    State(app): State<Arc<App>>,
    Path(stream): Path<String>,
    Query(q): Query<ReadQ>,
    h: HeaderMap,
) -> Response {
    let key = format!("{}/{}", basin(&h), stream);
    let s = match app.stream(&key).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::SERVICE_UNAVAILABLE, json!(e.to_string())),
    };
    let sse = h
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"));
    let mut live = s.live.subscribe(); // before reading the tail: nothing falls in between
    let tail = *s.tail.borrow();
    let mut start = q
        .seq_num
        .unwrap_or_else(|| tail.seq_num.saturating_sub(q.tail_offset.unwrap_or(0)));
    if start > tail.seq_num || (!sse && start == tail.seq_num) {
        if q.clamp == Some(true) {
            start = tail.seq_num;
        } else {
            return err(StatusCode::RANGE_NOT_SATISFIABLE, json!({ "tail": tail }));
        }
    }
    let limit = q.count.unwrap_or(1000).min(1000);
    if !sse {
        return match app.read(&key, start, tail.seq_num, limit).await {
            Ok(records) => Json(ReadBatch {
                records,
                tail: Some(tail),
            })
            .into_response(),
            Err(e) => err(StatusCode::SERVICE_UNAVAILABLE, json!(e.to_string())),
        };
    }
    let (tx, rx) = mpsc::channel::<std::result::Result<SseEvent, std::convert::Infallible>>(64);
    tokio::spawn(async move {
        let (mut next, mut count, mut bytes) = (start, 0usize, 0usize);
        let mut emit = |recs: Vec<SequencedRecord>, next: &mut u64| {
            let recs: Vec<_> = recs.into_iter().filter(|r| r.seq_num >= *next).collect();
            let last = recs.last()?.seq_num;
            *next = last + 1;
            count += recs.len();
            bytes += recs.iter().map(|r| 8 + r.body.len()).sum::<usize>();
            let data = serde_json::to_string(&ReadBatch {
                records: recs,
                tail: None,
            })
            .ok()?;
            Some(
                SseEvent::default()
                    .event("batch")
                    .id(format!("{last},{count},{bytes}"))
                    .data(data),
            )
        };
        let mut caught_up = tail.seq_num;
        loop {
            // catch up from the mesh, then follow the streamer's acknowledged batches
            while next < caught_up {
                let Ok(recs) = app.read(&key, next, caught_up, 1000).await else {
                    return;
                };
                if recs.is_empty() {
                    break;
                }
                if let Some(e) = emit(recs, &mut next)
                    && tx.send(Ok(e)).await.is_err()
                {
                    return;
                }
            }
            match live.recv().await {
                Ok(recs) => {
                    if recs[0].seq_num > next {
                        caught_up = recs[0].seq_num; // a gap: read it first
                        continue;
                    }
                    if let Some(e) = emit(recs.to_vec(), &mut next)
                        && tx.send(Ok(e)).await.is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => caught_up = s.tail.borrow().seq_num,
                Err(_) => return,
            }
        }
    });
    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn create_basin(Json(v): Json<serde_json::Value>) -> Response {
    (
        StatusCode::CREATED,
        Json(json!({"name": v["basin"], "state": "active"})),
    )
        .into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    let nodes = arg("--nodes", "");
    let reader = if nodes.is_empty() {
        None
    } else {
        Some(Client::connect(
            &nodes.split(',').collect::<Vec<_>>(),
            &arg("--token", "in-cluster"),
        )?)
    };
    let sink = match arg("--sink", "mesh").as_str() {
        "mesh" => {
            let addrs = nodes
                .split(',')
                .map(|a| a.parse())
                .collect::<Result<Vec<_>>>()?;
            let mc = MeshClient::new(addrs, arg("--token", "in-cluster"))?;
            let copies = arg("--copies", &mc.node_count().min(2).to_string()).parse()?;
            Sink::Mesh { mc, copies }
        }
        "s3" => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_endpoint(arg("--s3-endpoint", "http://127.0.0.1:9000"))
                .with_allow_http(true)
                .with_bucket_name(arg("--bucket", "precogwal"))
                .with_region("us-east-1")
                .build()?;
            s3_wal(
                Arc::new(store),
                arg("--flush-ms", "0").parse()?,
                arg("--put-slots", "4").parse()?,
            )
        }
        _ => Sink::None,
    };
    let app = Arc::new(App {
        sink,
        reader,
        streams: Mutex::default(),
    });
    let r = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/basins", post(create_basin))
        .route("/v1/streams/{stream}/records", post(append).get(read))
        .route("/v1/streams/{stream}/records/tail", get(check_tail))
        .with_state(app);
    let l = tokio::net::TcpListener::bind(arg("--listen", "0.0.0.0:8080")).await?;
    axum::serve(l, r).await?;
    Ok(())
}
