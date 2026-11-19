//! precog-harness: runs broker scenarios against a 3-node Docker cluster of Precog (or of the
//! Redpanda dedup fork, where a scenario applies to both) and writes `results.json` plus
//! `summary.md`. Needs Docker and root (iptables, tmpfs mounts).
//!
//!   precog-harness list
//!   precog-harness run [--system precog|redpanda] [--only a,b] [--out DIR] [--node-bin PATH]

mod check;
mod docker;
mod precog;
mod redpanda;
mod scenarios;

use anyhow::{Result, bail};
use docker::{Cluster, System};
use scenarios::{ALL, Ctx};
use serde_json::json;
use std::path::PathBuf;
use std::time::Instant;

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter()
        .position(|a| a == k)
        .and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("list") => {
            for s in ALL {
                println!(
                    "{:28} {:16} {}",
                    s.name,
                    if s.both { "precog,redpanda" } else { "precog" },
                    s.about
                );
            }
            Ok(())
        }
        Some("run") => run(&args).await,
        _ => bail!(
            "usage: precog-harness list | run [--system precog|redpanda] [--only a,b] [--out DIR] [--node-bin PATH]"
        ),
    }
}

async fn run(args: &[String]) -> Result<()> {
    let system = match arg(args, "--system").as_deref().unwrap_or("precog") {
        "precog" => System::Precog,
        "redpanda" => System::Redpanda,
        s => bail!("unknown system {s}"),
    };
    let only: Option<Vec<String>> =
        arg(args, "--only").map(|s| s.split(',').map(str::to_owned).collect());
    if let Some(o) = &only
        && let Some(bad) = o.iter().find(|n| !ALL.iter().any(|s| s.name == n.as_str()))
    {
        bail!("no scenario {bad} (see `precog-harness list`)");
    }
    let out = PathBuf::from(arg(args, "--out").unwrap_or_else(|| "results/harness".into()));
    std::fs::create_dir_all(&out)?;
    let work = std::path::absolute(out.join("work"))?;
    std::fs::create_dir_all(&work)?;
    if system == System::Precog {
        let bin = arg(args, "--node-bin").unwrap_or_else(|| "target/release/precog-node".into());
        docker::build_precog_image(std::path::Path::new(&bin), &work).await?;
    }
    let mut ctx = Ctx {
        system,
        work: work.clone(),
        c: Cluster::new(system, &work),
    };
    let mut results = vec![];
    for s in ALL {
        if only
            .as_ref()
            .is_some_and(|o| !o.iter().any(|n| n == s.name))
            || (system == System::Redpanda && !s.both)
        {
            continue;
        }
        eprintln!("== {} ({:?})", s.name, system);
        let t = Instant::now();
        let r = scenarios::run(s.name, &mut ctx).await;
        ctx.c.save_logs(&out.join("logs").join(s.name)).await;
        let _ = ctx.c.down().await;
        let secs = t.elapsed().as_secs_f64();
        let rec = match r {
            Ok(o) => {
                let status = if o.skipped.is_some() {
                    "skip"
                } else if o.checks.iter().all(|c| c.ok || c.info) {
                    "pass"
                } else {
                    "fail"
                };
                json!({"scenario": s.name, "about": s.about, "status": status, "secs": secs,
                       "skipped": o.skipped, "checks": o.checks, "metrics": o.metrics})
            }
            Err(e) => json!({"scenario": s.name, "about": s.about, "status": "error", "secs": secs,
                             "error": format!("{e:#}")}),
        };
        eprintln!("   {} in {:.0} s", rec["status"], secs);
        if let Some(cs) = rec["checks"].as_array() {
            for c in cs.iter().filter(|c| c["ok"] == false) {
                eprintln!("   FAILED: {} {}", c["name"], c["detail"]);
            }
        }
        if let Some(e) = rec["error"].as_str() {
            eprintln!("   ERROR: {e}");
        }
        results.push(rec);
        // written after every scenario: a long run that dies keeps what it had
        write(&out, system, &results)?;
    }
    let _ = std::fs::remove_dir_all(&work);
    let failed = results
        .iter()
        .filter(|r| r["status"] == "fail" || r["status"] == "error")
        .count();
    eprintln!(
        "{} scenarios, {failed} failed; results in {}",
        results.len(),
        out.display()
    );
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn write(out: &std::path::Path, system: System, results: &[serde_json::Value]) -> Result<()> {
    std::fs::write(
        out.join("results.json"),
        serde_json::to_vec_pretty(&json!({"system": system, "scenarios": results}))?,
    )?;
    let mut md = format!(
        "# Broker harness: {system:?}\n\n| Scenario | Result | Checks | Seconds |\n|---|---|---|---:|\n"
    );
    for r in results {
        let checks = r["checks"].as_array().map_or(0, |c| c.len());
        let failed: Vec<String> = r["checks"]
            .as_array()
            .map(|c| {
                c.iter()
                    .filter(|c| c["ok"] == false)
                    .map(|c| c["name"].as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let note = match r["status"].as_str() {
            Some("fail") => format!("failed: {}", failed.join("; ")),
            Some("error") => format!(
                "error: {}",
                r["error"]
                    .as_str()
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("")
            ),
            Some("skip") => format!("skipped: {}", r["skipped"].as_str().unwrap_or("")),
            _ => format!("{checks} passed"),
        };
        md += &format!(
            "| {} | {} | {} | {:.0} |\n",
            r["scenario"].as_str().unwrap_or(""),
            r["status"].as_str().unwrap_or(""),
            note.replace('|', "/"),
            r["secs"].as_f64().unwrap_or(0.0)
        );
    }
    std::fs::write(out.join("summary.md"), md)?;
    Ok(())
}
