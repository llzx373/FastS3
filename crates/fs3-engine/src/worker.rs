//! 后台 worker 通用抽象(ADR-12 DL2 / DESIGN-FUTURE §4.1.2)。
//!
//! 从 Tier 2 压缩 worker(ADR-9 §6)重构提取:压缩、生命周期执行器
//! (L2-2)及未来再平衡 worker(M13)共享同一套调度纪律与**全局同一
//! 令牌桶**——后台任务叠加不得侵蚀前台。
//!
//! 分工:
//! - 抽象层(本模块):线程 spawn/stop/pause、轮询间隔钳制、共享
//!   [`Throttle`] 令牌桶、批额度汇报([`BatchOutcome`]);
//! - 业务方:实现 [`BackgroundWorker::run_batch`],在每轮批处理内向
//!   桶申领消费,自行保证批内原子性与失败幂等。
//!
//! 锁域纪律(照 ADR-9 §6.3 压缩先例,所有实例必须遵守):
//! - worker **不得持有引擎大锁**;只经 meta(rocksdb 快照/乐观事务)、
//!   alloc(内部 Mutex)、io(Mutex 短临界区)交互;
//! - 批内长流程读用快照,提交走短事务(唯一排队点);
//! - 失败/暂停/中断只让收敛变慢,绝不破坏正确性(压缩是空间回收
//!   加速器,生命周期执行器同理)。
//!
//! 例外(M11 L2-2):生命周期执行器的单条删除 = 元数据事务(无长设备
//! I/O),复用 `&mut Engine` 删除原语(seal-on-delete/检查点不变量随之
//! 成立),经 `lifecycle::EngineAccess` 持服务层引擎写锁短临界区——等价
//! 一次前台 DELETE 的锁口径;扫描仍直读 meta 不经引擎锁(详见
//! lifecycle.rs 模块文档)。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fs3_core::Result;

/// 突发窗口:与压缩既有 100ms grace 口径一致(ADR-9 §6.4)。
const BURST: Duration = Duration::from_millis(100);

/// 全局共享令牌桶(ADR-12 DL2):所有后台 worker 的速率预算同源,
/// 全局消费总量恒定 ≤ rate × 已过时间 + 一次突发额度。
///
/// 口径保持压缩既有语义(ADR-9 §6.4):
/// - 补充速率 = `rate_limit_bytes_per_sec`(默认 64 MiB/s);
/// - 桶容量 = rate × 100ms(小批量一次完成的突发额度;批间闲置
///   回充以容量为顶,不无限累积);
/// - **允许透支**:单个业务条目(如一个对象/分片的段序列)必须
///   整体完成,超支部分记负余额,之后所有 worker 在余额回正前都被
///   制动——与旧实现「预算检查在 item 前、item 内不受限」逐点等价。
///
/// 申领纪律:worker 在开启**下一个业务条目**前查 [`Throttle::overdrawn`],
/// 透支即结束本轮批处理(余下工作留下轮);条目完成后按实际字节
/// [`Throttle::consume`] 记账。
#[derive(Debug)]
pub struct Throttle {
    inner: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    /// 补充速率(字节/秒)。
    rate: f64,
    /// 桶容量(rate × BURST)。
    burst: f64,
    /// 当前余额(可为负 = 透支)。
    tokens: f64,
    last: Instant,
}

impl Throttle {
    /// 建桶(初始满突发额度,等价旧实现每批启动时的 rate × 100ms
    /// grace)。返回 `Arc` 以强调共享语义:同一 `Arc<Throttle>` 克隆给
    /// 每个 worker,即为全局同一令牌桶。
    pub fn new(rate_bytes_per_sec: u64) -> Arc<Self> {
        let burst = rate_bytes_per_sec as f64 * BURST.as_secs_f64();
        Arc::new(Throttle {
            inner: Mutex::new(Bucket {
                rate: rate_bytes_per_sec as f64,
                burst,
                tokens: burst,
                last: Instant::now(),
            }),
        })
    }

    /// 桶余额是否已透支。worker 在开启下一个业务条目前检查;透支即
    /// 结束本轮批处理(等价旧实现 `copied_bytes > allowed → break`)。
    pub fn overdrawn(&self) -> bool {
        let mut b = self.inner.lock().unwrap();
        Self::refill(&mut b);
        b.tokens < 0.0
    }

    /// 记账 `bytes` 字节(允许透支为负;见类型文档)。消耗字节与
    /// `overdrawn` 检查配合即构成完整申领。
    pub fn consume(&self, bytes: u64) {
        let mut b = self.inner.lock().unwrap();
        Self::refill(&mut b);
        b.tokens -= bytes as f64;
    }

    /// 按真实流逝时间匀速回充,以桶容量为顶。
    fn refill(b: &mut Bucket) {
        let now = Instant::now();
        b.tokens = (b.tokens + b.rate * now.duration_since(b.last).as_secs_f64()).min(b.burst);
        b.last = now;
    }
}

/// 一轮批处理结果(汇报;批额度纪律的调度侧输入)。
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchOutcome {
    /// 本轮向桶记账的字节数。
    pub bytes: u64,
    /// 本轮完成的业务条目数(压缩迁移对象/分片,生命周期过期删除等)。
    pub items: u64,
    /// 是否还有积压(预留调度倾斜;当前不改变轮询节律,对齐压缩
    /// worker 既有「每批间恒睡 poll」行为)。
    pub more: bool,
}

/// 后台 worker 业务回调(ADR-12 DL2)。实现方每轮:
/// 1. 干活前向 `budget` 申领(`overdrawn`/`consume`,见 Throttle 文档);
/// 2. 返回本轮批额度汇报。
///
/// 实现必须是加速器而非正确性组件:任何一轮失败只延迟收敛。
pub trait BackgroundWorker: Send + 'static {
    fn run_batch(&mut self, budget: &Throttle) -> Result<BatchOutcome>;
}

/// 后台 worker 句柄(语义对齐原压缩 `CompactorHandle`):
/// - `set_paused(true)`:worker 空转——线程仍按 poll 节律循环,但不
///   调用 `run_batch`(零消费);
/// - `stop`:置停止位并 join 回收线程(幂等);停止延迟 ≤10ms——
///   轮询睡眠按 10ms 分片检查停止位(整块 `sleep(poll)` 会让 stop/join
///   最坏阻塞一整个 poll 周期:binlog 截断 worker 周期 60s,启动期
///   错误路径(RP4 角色矛盾 fail-fast)曾在 drop 回收处挂死整周期,
///   进程呈现「admin/复制口活、S3 不绑」僵尸态,M21 崩溃门禁实测);
/// - `Drop` 自动 `stop`。
pub struct WorkerHandle {
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl WorkerHandle {
    /// spawn 一个后台 worker。`name` 进线程名(日志/排障);`poll`
    /// 下限 10ms(对齐压缩 worker 既有钳制);`throttle` 为全局共享
    /// 令牌桶,多实例传入同一 `Arc` 克隆即完成注册。
    pub fn spawn<W: BackgroundWorker>(
        name: &str,
        mut worker: W,
        throttle: Arc<Throttle>,
        poll: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let (s, p) = (stop.clone(), paused.clone());
        let poll = poll.max(Duration::from_millis(10));
        let name = name.to_string();
        let join = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                'outer: while !s.load(Ordering::Acquire) {
                    if !p.load(Ordering::Acquire) {
                        if let Err(e) = worker.run_batch(&throttle) {
                            tracing::warn!("{name} batch failed: {e}");
                        }
                    }
                    // 分片睡眠(对齐 lib.rs 检查点 ticker 先例):停止位
                    // 10ms 粒度可见,stop()/join 不被 poll 周期放大
                    let start = std::time::Instant::now();
                    while start.elapsed() < poll {
                        if s.load(Ordering::Acquire) {
                            break 'outer;
                        }
                        std::thread::sleep(Duration::from_millis(10).min(poll));
                    }
                }
            })
            .expect("spawn background worker thread");
        WorkerHandle {
            stop,
            paused,
            join: Some(join),
        }
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }

    /// 停止并回收线程(引擎关闭/崩溃模拟时调用;幂等)。
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    /// 测试 worker:每轮向桶申领 `chunk` 字节并计数(`fail_first` 模拟
    /// 首轮批处理失败,验证错误不杀死调度线程)。
    struct MeteredWorker {
        chunk: u64,
        consumed: Arc<AtomicU64>,
        rounds: Arc<AtomicU64>,
        fail_first: bool,
    }

    impl MeteredWorker {
        fn new(chunk: u64) -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
            let consumed = Arc::new(AtomicU64::new(0));
            let rounds = Arc::new(AtomicU64::new(0));
            (
                MeteredWorker {
                    chunk,
                    consumed: consumed.clone(),
                    rounds: rounds.clone(),
                    fail_first: false,
                },
                consumed,
                rounds,
            )
        }
    }

    impl BackgroundWorker for MeteredWorker {
        fn run_batch(&mut self, budget: &Throttle) -> Result<BatchOutcome> {
            self.rounds.fetch_add(1, Ordering::Relaxed);
            if self.fail_first {
                self.fail_first = false;
                return Err(fs3_core::Error::Corrupt("injected batch failure".into()));
            }
            let mut done = 0;
            if !budget.overdrawn() {
                budget.consume(self.chunk);
                self.consumed.fetch_add(self.chunk, Ordering::Relaxed);
                done = self.chunk;
            }
            Ok(BatchOutcome {
                bytes: done,
                items: 1,
                more: true,
            })
        }
    }

    fn wait_until(mut cond: impl FnMut() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("condition not met within 1s");
    }

    /// 令牌桶速率收敛:突发 = rate × 100ms;透支后按 rate 匀速回正;
    /// 闲置回充以突发容量为顶。
    #[test]
    fn throttle_rate_converges() {
        // 1KB/s,突发 100B
        let t = Throttle::new(1_000);
        // 初始满突发:100B 恰好不透支,再 1B 透支
        t.consume(100);
        assert!(!t.overdrawn());
        t.consume(1);
        assert!(t.overdrawn());
        // 负余额(-1B)按 1KB/s 回充:120ms 后应回正且有余
        std::thread::sleep(Duration::from_millis(120));
        assert!(!t.overdrawn());
        // 闲置回充封顶于突发(100B):再睡 300ms 余额仍 ≈100B,
        // 消费 100B 后不透支、再 1B 即透支
        std::thread::sleep(Duration::from_millis(300));
        t.consume(100);
        assert!(!t.overdrawn());
        t.consume(1);
        assert!(t.overdrawn());
    }

    /// pause 期间零消费;resume 后继续;stop join 后线程不再跑。
    #[test]
    fn worker_pause_resume_stop() {
        let t = Throttle::new(1 << 20);
        let (w, consumed, rounds) = MeteredWorker::new(1024);
        let mut h = WorkerHandle::spawn("fs3-test-pause", w, t, Duration::from_millis(10));
        // 先跑起来
        wait_until(|| rounds.load(Ordering::Relaxed) >= 2);
        // pause:在飞轮落地后采样,之后窗口内必须零增长
        h.set_paused(true);
        std::thread::sleep(Duration::from_millis(30));
        let r = rounds.load(Ordering::Relaxed);
        let c = consumed.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(rounds.load(Ordering::Relaxed), r, "paused:一轮都不跑");
        assert_eq!(consumed.load(Ordering::Relaxed), c, "paused:零消费");
        // resume
        h.set_paused(false);
        wait_until(|| rounds.load(Ordering::Relaxed) > r);
        assert!(consumed.load(Ordering::Relaxed) > c);
        // stop:join 后不再跑;二次 stop 幂等
        h.stop();
        let r2 = rounds.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(rounds.load(Ordering::Relaxed), r2, "stop 后线程已回收");
        h.stop();
    }

    /// 长 poll 周期下 stop() 必须快速返回(分片睡眠;回归:整块
    /// `sleep(poll)` 时代 stop/join 最坏阻塞一个完整 poll 周期——
    /// binlog 截断 worker 周期 60s,启动期错误路径曾在 drop 回收处
    /// 挂死整周期,进程呈「admin/复制口活、S3 不绑」僵尸态)。
    #[test]
    fn worker_stop_not_blocked_by_poll_sleep() {
        let t = Throttle::new(1 << 20);
        let (w, _, rounds) = MeteredWorker::new(1024);
        let mut h = WorkerHandle::spawn("fs3-test-stop", w, t, Duration::from_secs(60));
        // 等线程进过首轮批处理(随后即进入 60s 轮询睡眠)
        wait_until(|| rounds.load(Ordering::Relaxed) >= 1);
        let start = std::time::Instant::now();
        h.stop();
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "stop 被 poll 睡眠阻塞 {:?}(须 ≤10ms 粒度)",
            start.elapsed()
        );
    }

    /// 批处理错误只告警,不杀死调度线程。
    #[test]
    fn worker_batch_error_is_not_fatal() {
        let t = Throttle::new(1 << 20);
        let (mut w, consumed, rounds) = MeteredWorker::new(1024);
        w.fail_first = true;
        let mut h = WorkerHandle::spawn("fs3-test-err", w, t, Duration::from_millis(10));
        wait_until(|| rounds.load(Ordering::Relaxed) >= 3);
        assert!(
            consumed.load(Ordering::Relaxed) > 0,
            "首轮失败后仍被继续调度"
        );
        h.stop();
    }

    /// 多实例共享全局同一令牌桶(ADR-12 DL2;生命周期执行器注册点
    /// 验证):两 worker 并发申领,总消费 ≤ 突发 + rate × 时间(容差内)。
    #[test]
    fn shared_throttle_caps_total_consumption() {
        let rate = 1u64 << 20; // 1MiB/s
        let t = Throttle::new(rate);
        let chunk = 16u64 << 10; // 16KiB/轮
        let (w1, consumed1, _) = MeteredWorker::new(chunk);
        let (w2, consumed2, _) = MeteredWorker::new(chunk);
        let mut h1 = WorkerHandle::spawn("fs3-test-a", w1, t.clone(), Duration::from_millis(10));
        let mut h2 = WorkerHandle::spawn("fs3-test-b", w2, t, Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(300));
        h1.stop();
        h2.stop();
        let total = consumed1.load(Ordering::Relaxed) + consumed2.load(Ordering::Relaxed);
        // 理论上限:突发(rate×0.1) + rate×窗口 + 两侧在飞轮透支(各 ≤chunk);
        // 窗口取 300ms + 150ms 调度/启停裕量
        let cap = rate as f64 * 0.1 + rate as f64 * 0.45 + 2.0 * chunk as f64;
        assert!(
            (total as f64) <= cap,
            "两 worker 总消费 {total} 超过全局桶上限 {cap}(叠加侵蚀前台)"
        );
        assert!(total > 0, "两侧确实在消费");
        assert!(
            consumed1.load(Ordering::Relaxed) > 0 && consumed2.load(Ordering::Relaxed) > 0,
            "两个实例都在申领(共享而非独占)"
        );
    }
}
