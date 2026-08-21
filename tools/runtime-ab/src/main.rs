//! G4 运行时 A/B —— tokio-uring 设备层微基准(独立 crate,不进主 workspace)。
//!
//! 对比同机两种方式对同一 O_DIRECT 文件的批量读吞吐:
//!   - 自研(现状,fasts3d bench --io-backend uring):thread-per-core + 直连
//!     io_uring,批量提交/收割,零运行时调度层;
//!   - 本程序(tokio-uring):tokio 多线程 runtime + tokio-uring 驱动。
//!
//! 结论由 docs/DESIGN.md ADR-10 依据两方数据与工程分析给出。引擎零改动:
//! 双方都只直接做设备层 4KiB/64KiB O_DIRECT 读,不经过 FastS3 Engine。
//!
//! 用法:
//!   cd tools/runtime-ab
//!   cargo run --release -- <tmpfile> [block=4096] [iodepth=64] [threads=4] [secs=5]
//!   (先在宿主建 2GiB 临时文件;块设备用 O_DIRECT 直开亦可)

use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ALIGN: usize = 4096;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("usage: runtime-ab <file> [block] [depth] [threads] [secs]");
    let block: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(4096);
    let depth: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(64);
    let threads: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(4);
    let secs: u64 = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(5);

    assert!(block % ALIGN == 0, "block must be multiple of {ALIGN}");
    let p = path.clone();
    let f = std::fs::OpenOptions::new().read(true).open(&p)?;
    let file_len = f.metadata()?.len();
    let max_off = (file_len / block as u64).max(1) - 1;
    drop(f);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(secs.max(1)));
        stop2.store(true, Ordering::SeqCst);
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;

    println!("== runtime-ab: tokio-uring batched O_DIRECT read ==");
    println!("file={path} block={block} depth={depth} threads={threads} secs={secs} size={file_len}");
    let start = Instant::now();
    let mut bytes = 0u64;
    let mut ops = 0u64;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let stop = stop.clone();
            let path = path.clone();
            let block = block;
            let depth = depth;
            let max_off = max_off;
            let t = t as u64;
            std::thread::Builder::new()
                .name(format!("ab-{t}"))
                .spawn(move || {
                    rt.block_on(async move {
                        let file = std::fs::File::open(&path)?;
                        let fd = file.as_raw_fd();
                        let mut file = unsafe { tokio_uring::fs::File::from_raw_fd(fd) };
                        // tokio-uring 需要缓冲按块对齐(内部对齐到 page);这里用其内存池
                        let mut rng_x = t * 0x9E3779B97F4A7C15 + 1;
                        let (mut ops, mut bytes) = (0u64, 0u64);
                        while !stop.load(Ordering::SeqCst) {
                            // 批量:先创建 depth 个读 future(加入 ring),再用
                            // join_all 并发 poll → 同批提交、按批收割(fair 对照)。
                            let mut futs = Vec::with_capacity(depth);
                            for _ in 0..depth {
                                rng_x ^= rng_x >> 12;
                                rng_x ^= rng_x << 25;
                                rng_x ^= rng_x >> 27;
                                let off = (rng_x % (max_off + 1)) * block as u64;
                                let mut buf = vec![0u8; block];
                                futs.push(file.read_at(std::mem::take(&mut buf), off));
                            }
                            let res = futures::future::join_all(futs).await;
                            for r in res {
                                let _ = r??;
                                ops += 1;
                                bytes += block as u64;
                            }
                            let _ = std::hint::black_box(());
                        }
                        anyhow::Ok((ops, bytes))
                    })
                })
                .unwrap()
                .join()
                .unwrap()
        })
        .collect();

    for h in handles {
        let (o, b) = h?;
        ops += o;
        bytes += b;
    }
    drop(rt);
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "tokio-uring: ops={ops} bytes={bytes} ops/s={:.0} MB/s={:.1}",
        ops as f64 / elapsed,
        bytes as f64 / elapsed / 1024.0 / 1024.0
    );
    println!("对比参考:同机 `fasts3d benchmark bench --rw randread --block 4KiB --iodepth 64 --threads N` (自研 io_uring)");
    Ok(())
}
