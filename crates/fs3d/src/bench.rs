//! 引擎内部基准 harness(A2):设备层直测,不经 S3 协议。
//!
//! 与 fio 基线同一方法论(DESIGN §11.2):O_DIRECT、io_uring、对齐 I/O。
//! 数据区直写/直读,报告 IOPS / MB/s / p99 延迟。
//!
//! M5 追加 `md5` 目标:SIMD 4 路多缓冲 MD5 vs 单缓冲(md-5)吞吐对比。

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use clap::Args;
use fs3_core::{Error, Result, SECTOR_SIZE};
use fs3_device::{AlignedBuffer, BlockDevice, ImageFile};
use fs3_engine::io::{IoEngine, IoOp};

use crate::config::parse_size;

#[derive(Args, Debug, Clone)]
pub struct BenchArgs {
    /// I/O 块大小(4KiB / 64KiB / 128KiB)
    #[arg(long, default_value = "64KiB")]
    block: String,
    /// 模式:read | write | randread | randwrite
    #[arg(long, default_value = "randread")]
    rw: String,
    /// 运行时长(秒)
    #[arg(long, default_value = "5")]
    duration: u64,
    /// 队列深度(每线程批量提交数)
    #[arg(long, default_value = "64")]
    iodepth: u32,
    /// 并发线程数
    #[arg(long, default_value = "1")]
    threads: u32,
    /// io_uring IOPOLL(轮询完成;需设备支持;不兼容则自动降级)
    #[arg(long)]
    iopoll: bool,
    /// io_uring COOP_TASKRUN
    #[arg(long)]
    coop_taskrun: bool,
    /// io_uring SINGLE_ISSUER(6.0+)
    #[arg(long)]
    single_issuer: bool,
    /// I/O 后端:uring(默认) | pread(强制 pread/pwrite 兜底,引擎零改动对照)
    #[arg(long, default_value = "uring")]
    io_backend: String,
}

struct ThreadResult {
    ops: u64,
    bytes: u64,
    p99_us: f64,
    avg_us: f64,
}

/// xorshift64* 伪随机(线程本地偏移生成)。
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

pub fn run(cfg: &fs3_engine::EngineConfig, args: BenchArgs) -> Result<()> {
    let block = parse_size(&args.block)?;
    if block < SECTOR_SIZE || !block.is_multiple_of(SECTOR_SIZE) {
        return Err(Error::InvalidArgument(format!(
            "block {block} must be a multiple of {SECTOR_SIZE}"
        )));
    }
    let mode = match args.rw.as_str() {
        "read" => Mode::Read,
        "write" => Mode::Write,
        "randread" => Mode::RandRead,
        "randwrite" => Mode::RandWrite,
        other => {
            return Err(Error::InvalidArgument(format!(
                "unknown rw mode {other} (read|write|randread|randwrite)"
            )))
        }
    };
    // 用已初始化设备的布局信息:数据区偏移
    let dev = ImageFile::open(&cfg.devices[0], false)?;
    let sb = fs3_device::read_superblock(&dev)?;
    let data_start = sb.data_start;
    let data_len = sb.data_end - sb.data_start;
    drop(dev);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let timer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(args.duration.max(1)));
        stop2.store(true, Ordering::SeqCst);
    });

    let device_path = cfg.devices[0].clone();
    let io_opts = fs3_engine::io::IoUringOptions {
        iopoll: args.iopoll,
        coop_taskrun: args.coop_taskrun,
        single_issuer: args.single_issuer,
    };
    let prefer_uring = args.io_backend != "pread";
    let mut handles = vec![];
    for t in 0..args.threads {
        let stop = stop.clone();
        let path = device_path.clone();
        handles.push(std::thread::spawn(move || {
            bench_thread(&BenchCtx {
                device_path: path,
                block,
                mode,
                iodepth: args.iodepth,
                data_start,
                data_len,
                seed: t as u64,
                stop,
                prefer_uring,
                io_opts,
            })
        }));
    }
    let mut results = Vec::with_capacity(handles.len());
    for h in handles {
        results.push(
            h.join()
                .map_err(|_| Error::InvalidArgument("bench thread panic".into()))?,
        );
    }
    let _ = timer.join();

    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let total_bytes: u64 = results.iter().map(|r| r.bytes).sum();
    let dur = args.duration.max(1) as f64;
    let iops = total_ops as f64 / dur;
    let mbps = total_bytes as f64 / dur / (1024.0 * 1024.0);
    let p99 = results.iter().map(|r| r.p99_us).fold(0.0, f64::max);
    let avg = results.iter().map(|r| r.avg_us).sum::<f64>() / results.len().max(1) as f64;

    println!("== FastS3 engine bench (device layer, O_DIRECT) ==");
    println!(
        "mode={} block={}B iodepth={} threads={} duration={}s io={} iopoll={} coop={} single={}",
        args.rw,
        block,
        args.iodepth,
        args.threads,
        args.duration,
        args.io_backend,
        args.iopoll,
        args.coop_taskrun,
        args.single_issuer
    );
    println!(
        "data region: [{data_start}, {}) {} bytes",
        data_start + data_len,
        data_len
    );
    println!("IOPS:        {:.0}", iops);
    println!("throughput:  {:.1} MB/s", mbps);
    println!("avg latency: {:.1} us", avg);
    println!("p99 latency: {:.1} us", p99);
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Read,
    Write,
    RandRead,
    RandWrite,
}

// ──────────────── M12:Object Lock 判定微基准 ────────────────

/// `fasts3d bench-lock` 参数:M12 门禁「锁判定在元数据层(<1µs,无感)」。
#[derive(Args, Debug, Clone)]
pub struct LockCheckArgs {
    /// 迭代次数
    #[arg(long, default_value = "10000000")]
    rounds: u64,
}

pub fn run_lock_check(args: &LockCheckArgs) -> Result<()> {
    use fs3_core::{ObjectMeta, Retention, RetentionMode};
    use fs3_engine::lifecycle::lock_blocks_delete;

    let rounds = args.rounds.max(1) as usize;
    // 最坏形态样本:COMPLIANCE 未到期 + Legal Hold(两判定都走)。
    let now = 2_000_000_000i64;
    let mk = |hold: bool, until: i64| ObjectMeta {
        size: 4096,
        etag: [0u8; 16],
        mtime: now,
        extents: vec![],
        content_type: String::new(),
        user_meta: vec![],
        inline: None,
        parts: vec![],
        resp_headers: vec![],
        version_id: Some([7u8; 16]),
        is_delete_marker: false,
        tags: vec![],
        sse: None,
        checksum: None,
        retention: Some(Retention {
            mode: RetentionMode::Compliance,
            retain_until: until,
        }),
        legal_hold: hold,
        part_checksums: vec![],
        compressed: None,
    };
    let locked = mk(true, now + 30 * 86_400); // 未到期 + hold:最坏判定
    let expired = mk(false, now - 1); // 已到期 + 无 hold:常规路径
    let mut rng = Rng::new(0x5EED_1234); // xorshift:输入随迭代变化,防常量折叠
                                         // 预热(分支预测/缓存)
    let mut sink = 0u8;
    for _ in 0..100_000u64 {
        let m = if rng.next() & 1 == 0 {
            &locked
        } else {
            &expired
        };
        sink ^= lock_blocks_delete(m, now, false).is_some() as u8;
    }
    let t0 = Instant::now();
    for _ in 0..rounds as u64 {
        let m = if rng.next() & 1 == 0 {
            &locked
        } else {
            &expired
        };
        sink ^= lock_blocks_delete(m, now, false).is_some() as u8;
    }
    let secs = t0.elapsed().as_secs_f64();
    let ns = secs * 1e9 / rounds as f64;
    std::hint::black_box(&sink);

    println!("== Object Lock 判定微基准(M12 门禁:元数据层 <1µs,无感)==");
    println!("sample=COMPLIANCE+legal_hold(unexpired) rounds={rounds} total={secs:.3}s");
    println!(
        "lock_blocks_delete avg: {ns:.1} ns/op ({:.3} µs/op)",
        ns / 1000.0
    );
    if ns < 1000.0 {
        println!("RESULT: PASS (lock check < 1µs, metadata-layer)");
    } else {
        println!("RESULT: FAIL (lock check >= 1µs)");
    }
    Ok(())
}

// ──────────────── M5:MD5 多缓冲基准 ────────────────

/// `fasts3d bench-md5` 参数:对比 SIMD 4 路多缓冲 MD5 与单缓冲聚合吞吐。
#[derive(Args, Debug, Clone)]
pub struct Md5BenchArgs {
    /// 每条消息(缓冲区)大小
    #[arg(long, default_value = "1MiB")]
    size: String,
    /// 轮数(每轮 4 条 lane × size 字节)
    #[arg(long, default_value = "128")]
    rounds: u32,
}

pub fn run_md5(args: &Md5BenchArgs) -> Result<()> {
    use fs3_core::md5x4::Md5Multi4;
    use md5::Digest;

    let size = parse_size(&args.size)? as usize;
    if size == 0 || size > 256 * 1024 * 1024 {
        return Err(Error::InvalidArgument(format!("bad --size {}", args.size)));
    }
    let rounds = args.rounds.max(1) as usize;
    let total = 4u64 * rounds as u64 * size as u64;

    // 4 条确定性数据 lane(size 对齐 64 的倍数由内部处理)。
    let lanes: [Vec<u8>; 4] = std::array::from_fn(|l| {
        (0..size)
            .map(|i| ((i as u64 + l as u64 * 31) % 251) as u8)
            .collect()
    });
    let lane_refs: [&[u8]; 4] = lanes
        .iter()
        .map(|v| v.as_slice())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    // A) 单缓冲基线:4×rounds 次独立 md5。
    let t0 = Instant::now();
    let mut sink = [0u8; 16];
    for _ in 0..rounds {
        for v in lanes.iter() {
            sink = md5::Md5::digest(v).into();
        }
    }
    let single_secs = t0.elapsed().as_secs_f64();

    // B) 4 路多缓冲:rounds 轮 × 4 lane。
    let t1 = Instant::now();
    let mut sink4 = [[0u8; 16]; 4];
    for _ in 0..rounds {
        sink4 = Md5Multi4::digest(&lane_refs);
    }
    let multi_secs = t1.elapsed().as_secs_f64();
    // 防止被优化掉
    std::hint::black_box(&sink);
    std::hint::black_box(&sink4);

    let single_mbps = total as f64 / single_secs / (1024.0 * 1024.0);
    let multi_mbps = total as f64 / multi_secs / (1024.0 * 1024.0);
    println!("== MD5 基准(M5 SIMD 多缓冲)==");
    println!(
        "size={size} rounds={rounds} total={} MiB",
        total / (1024 * 1024)
    );
    println!(
        "single(md5 crate): {single_mbps:8.0} MiB/s ({:.2}s)",
        single_secs
    );
    println!(
        "multi  (md5x4)   : {multi_mbps:8.0} MiB/s ({:.2}s)",
        multi_secs
    );
    println!("speedup          : {:.2}x", multi_mbps / single_mbps);
    Ok(())
}

struct BenchCtx {
    device_path: std::path::PathBuf,
    block: u64,
    mode: Mode,
    iodepth: u32,
    data_start: u64,
    data_len: u64,
    seed: u64,
    stop: Arc<AtomicBool>,
    prefer_uring: bool,
    io_opts: fs3_engine::io::IoUringOptions,
}

fn bench_thread(ctx: &BenchCtx) -> ThreadResult {
    let dev = ImageFile::open(&ctx.device_path, false).expect("open device");
    let fd: RawFd = dev.raw_fd();
    let mut io: Box<dyn IoEngine> =
        fs3_engine::io::open_io_engine_opts(ctx.prefer_uring, ctx.io_opts).expect("io engine");
    let depth = ctx.iodepth.max(1) as usize;
    let block = ctx.block;

    let mut bufs: Vec<AlignedBuffer> = (0..depth)
        .map(|_| AlignedBuffer::new(block as usize).expect("buf"))
        .collect();
    // 写模式:填充确定性数据
    if matches!(ctx.mode, Mode::Write | Mode::RandWrite) {
        for (i, b) in bufs.iter_mut().enumerate() {
            for (j, byte) in b.as_mut_slice().iter_mut().enumerate() {
                *byte = ((i * 31 + j) % 251) as u8;
            }
        }
    }

    let mut rng = Rng::new(ctx.seed.wrapping_mul(0x9E3779B97F4A7C15) + 1);
    let max_off = (ctx.data_len / block).saturating_sub(1).max(1);
    let mut cursor: u64 = 0; // 顺序模式游标

    let mut samples: Vec<f64> = Vec::with_capacity(200_000);
    let mut ops: u64 = 0;
    let mut bytes: u64 = 0;
    let start = Instant::now();

    'outer: loop {
        if ctx.stop.load(Ordering::SeqCst) {
            break;
        }
        let t0 = Instant::now();
        let mut ops_vec = Vec::with_capacity(depth);
        for _ in 0..depth {
            let off = match ctx.mode {
                Mode::Read | Mode::Write => {
                    let o = cursor * block;
                    cursor = (cursor + 1) % (max_off + 1);
                    o
                }
                Mode::RandRead | Mode::RandWrite => rng.next() % (max_off + 1) * block,
            };
            let dev_off = ctx.data_start + off;
            ops_vec.push(if matches!(ctx.mode, Mode::Write | Mode::RandWrite) {
                let i = ops_vec.len() % depth;
                IoOp::Write {
                    fd,
                    buf: bufs[i].as_ptr(),
                    len: block as u32,
                    offset: dev_off,
                }
            } else {
                let i = ops_vec.len() % depth;
                IoOp::Read {
                    fd,
                    buf: bufs[i].as_mut_ptr(),
                    len: block as u32,
                    offset: dev_off,
                }
            });
        }
        let submit_t = Instant::now();
        if let Err(e) = io.submit(&ops_vec) {
            // 只读时允许越界(未格式化尾部);其余错误上报
            eprintln!("bench io error: {e}");
            break 'outer;
        }
        let lat = submit_t.elapsed().as_secs_f64() * 1e6;
        if samples.len() < samples.capacity() {
            samples.push(lat / depth as f64);
        }
        ops += depth as u64;
        bytes += block * depth as u64;
        if t0.elapsed().as_secs_f64() > 0.5 {
            // 空闲保护:io 完成远快于计时时避免忙等烧核
            std::thread::yield_now();
        }
    }
    let _ = start;

    // p99
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99 = samples
        .get((samples.len() as f64 * 0.99) as usize)
        .copied()
        .unwrap_or(0.0);
    let avg = if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    };
    ThreadResult {
        ops,
        bytes,
        p99_us: p99,
        avg_us: avg,
    }
}
