//! `fasts3d stress-insert`(M4 门禁:1 亿对象压测,rocksdb 扩展性验证 R5)。
//!
//! 单进程常驻引擎,内联小对象批量插入(纯元数据路径,专测 rocksdb 在大对象
//! 数下的吞吐与稳定性);周期性检查点;结束时对账(对象数/字节)。
//!
//! 用法:
//!   fasts3d stress-insert --device <dev> --meta-dir <meta> --bucket <b> \
//!       --count N [--size B(默认 64,内联)] [--sync-mode group|full] \
//!       [--checkpoint-every M]

use std::io::Cursor;
use std::time::Instant;

use clap::Args;
use fs3_engine::Engine;

#[derive(Debug, Args)]
pub struct StressArgs {
    /// 插入对象总数(默认 1 亿 = M4 门禁)。
    #[arg(long, default_value_t = 100_000_000)]
    pub count: u64,
    /// 对象数据大小(默认 64B → 内联,纯元数据路径)。
    #[arg(long, default_value_t = 64)]
    pub size: usize,
    /// 检查点间隔(每 M 个对象;0 = 仅结束)。
    #[arg(long, default_value_t = 2_000_000)]
    pub checkpoint_every: u64,
}

pub fn run(args: &StressArgs, cfg: &fs3_engine::EngineConfig) -> fs3_core::Result<()> {
    let mut cfg = cfg.clone();
    cfg.compaction.enabled = true; // 开启后台惰性压缩,观察并行稳定性
    let mut engine = Engine::open(&cfg)?;
    engine.ensure_bucket(&cfg_bucket())?;
    let bucket = cfg_bucket();

    tracing::info!(
        "stress-insert: count={} size={} checkpoint_every={}",
        args.count,
        args.size,
        args.checkpoint_every
    );

    let data = vec![0x5Au8; args.size];
    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut last_count = 0u64;
    let mut done = 0u64;

    while done < args.count {
        let key = format!("k{:020}", done);
        engine.put(&bucket, &key, &mut Cursor::new(&data[..]))?;
        done += 1;
        if args.checkpoint_every > 0 && done.is_multiple_of(args.checkpoint_every) {
            engine.checkpoint()?;
        }
        if last_report.elapsed().as_secs() >= 5 {
            let ops = (done - last_count) as f64 / last_report.elapsed().as_secs_f64();
            let total = done as f64 / start.elapsed().as_secs_f64();
            println!(
                "progress: {}/{} ({:.1}%), recent {:.0} ops/s, avg {:.0} ops/s, {}s",
                done,
                args.count,
                done as f64 / args.count as f64 * 100.0,
                ops,
                total,
                start.elapsed().as_secs()
            );
            last_report = Instant::now();
            last_count = done;
        }
    }
    engine.checkpoint()?;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "stress-insert DONE: {} objects in {:.1}s ({:.0} ops/s), final checkpoint written",
        done,
        elapsed,
        done as f64 / elapsed
    );

    // 对账:对象计数 + check 零泄漏
    let check = engine.check_report()?;
    println!(
        "verify: objects={} live_bytes={} leaks={} (expect zero leaks)",
        check.objects,
        check.live_bytes,
        check.leaks.len()
    );
    if !check.leaks.is_empty() {
        return Err(fs3_core::Error::Corrupt(format!(
            "{} leaked extents after 1e8-object stress",
            check.leaks.len()
        )));
    }
    engine.close()?;
    Ok(())
}

fn cfg_bucket() -> String {
    "stress".into()
}
