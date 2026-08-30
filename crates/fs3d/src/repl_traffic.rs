//! 中继流量优先级令牌桶(M21 E2;ADR-33 裁定 4「中继流量优先级可配」/
//! RP8.4;docs/replication-design.md §3.5「投递下游 > 后台回填 > 读路径
//! 按需拉取,经 worker 共享令牌桶按优先级配权重」)。
//!
//! **实现形态(旁路,注释钉死)**:仓内 `fs3_engine::worker::Throttle`
//! 是「全局同桶、无类别」的共享令牌桶(ADR-12 DL2),不支持优先级
//! 语义;本模块不动 Throttle(压缩/生命周期等既有后台桶零影响),旁路
//! 实现「**共享桶 + 每类保底信用**」的加权调度:
//! - 一个共享桶 `tokens` 以速率 R 回充(顶 = burst = R × 100ms,同
//!   Throttle 口径)——桶有余量时任一类都可开下一条目(**work
//!   conserving**:空闲容量可被任意类使用,不浪费);
//! - 每类一条保底信用 `credit[i]`,以 R × w_i/W 回充(顶 =
//!   burst × w_i/W)——桶被打空(tokens ≤ 0)后,类 i 只在信用为正时
//!   才能开新条目,稳态份额收敛于 w_i/W;
//! - **无饿死**:信用恒以正速率回充,低优先级打满只耗尽共享桶与
//!   自己的信用,高优先级的保底信用不受其消耗;**高优先级不被低
//!   优先级拖停**——同理高优先级超发只耗自己信用与共享桶,低优先级
//!   的保底份额仍在。
//! - 申领/记账纪律照 Throttle(条目原子):开下一个业务条目前查
//!   [`ReplTraffic::overdrawn`],透支即制动等回充;条目完成后按实际
//!   字节 [`ReplTraffic::consume`] 记账(共享桶与本类信用同扣,允许
//!   透支为负——单条目必须整体完成,同 Throttle「预算检查在 item 前、
//!   item 内不受限」口径)。
//!
//! 三类流量(中继节点角色):
//! - `TrafficClass::Serve`:投递下游 = 复制口对下服务字节
//!   (extent-data 响应、快照导出页,repl.rs);
//! - `TrafficClass::Backfill`:后台回填池自上游拉取
//!   (repl_backfill.rs process_record);
//! - `TrafficClass::OnDemand`:读路径命中 data_pending 的按需拉取
//!   (repl_backfill.rs fetch_object,C4)。
//!
//! 配置(M21 F3 收口):`[replication.traffic_weights]` 子表为准,形如
//! `{ serve = 100, backfill = 50, on_demand = 10 }`(缺省即此,字段缺席
//! = 该项取缺省);权重 ≥ 1(0 = 该类信用永不回充,等价配置死锁,启动
//! fail-fast 不静默)。**env `FS3D_REPL_TRAFFIC_WEIGHTS` 保留为测试钩子**
//! (`serve=100,backfill=50,on_demand=10` 形;三键必须同设),仅当配置
//! 子表缺席时回退。总速率 = 复制口限速(`[replication].export_rate`,
//! 缺省 64 MiB/s)——同一共享桶速率。

use std::sync::{Arc, Mutex};
use std::time::Instant;

/// 突发窗口(同 fs3-engine worker::Throttle 的 100ms grace 口径,
/// ADR-9 §6.4)。
const BURST: std::time::Duration = std::time::Duration::from_millis(100);

/// 流量类别(三类优先级由高到低 = 声明序)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficClass {
    /// 投递下游(复制口 serve 字节)。
    Serve = 0,
    /// 后台回填。
    Backfill = 1,
    /// 读路径按需拉取。
    OnDemand = 2,
}

impl TrafficClass {
    const ALL: [TrafficClass; 3] = [
        TrafficClass::Serve,
        TrafficClass::Backfill,
        TrafficClass::OnDemand,
    ];

    fn idx(self) -> usize {
        self as usize
    }
}

/// 三类流量权重(裁定 4;缺省 serve=100/backfill=50/on_demand=10,即
/// 缺省序 投递 > 回填 > 按需拉取)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrafficWeights {
    pub serve: u64,
    pub backfill: u64,
    pub on_demand: u64,
}

impl Default for TrafficWeights {
    fn default() -> Self {
        TrafficWeights {
            serve: 100,
            backfill: 50,
            on_demand: 10,
        }
    }
}

impl std::str::FromStr for TrafficWeights {
    type Err = String;

    /// 解析 `serve=100,backfill=50,on_demand=10` 形(键序无关,三键
    /// 必须同设;权重 ≥ 1)。
    fn from_str(s: &str) -> Result<Self, String> {
        let mut w = TrafficWeights {
            serve: 0,
            backfill: 0,
            on_demand: 0,
        };
        let mut seen = 0usize;
        for part in s.split(',') {
            let (k, v) = part.split_once('=').ok_or_else(|| {
                format!("bad FS3D_REPL_TRAFFIC_WEIGHTS entry {part:?} (expect class=weight)")
            })?;
            let v: u64 = v
                .trim()
                .parse()
                .map_err(|e| format!("bad weight in {part:?}: {e}"))?;
            if v == 0 {
                return Err(format!(
                    "weight in {part:?} must be >= 1 (0 = 该类流量永久制动,等价配置死锁)"
                ));
            }
            match k.trim() {
                "serve" => w.serve = v,
                "backfill" => w.backfill = v,
                "on_demand" => w.on_demand = v,
                other => {
                    return Err(format!(
                        "unknown traffic class {other:?} (expect serve|backfill|on_demand)"
                    ))
                }
            }
            seen += 1;
        }
        if seen != 3 {
            return Err(
                "FS3D_REPL_TRAFFIC_WEIGHTS must set all of serve=..,backfill=..,on_demand=.."
                    .into(),
            );
        }
        Ok(w)
    }
}

impl TrafficWeights {
    /// 配置段入口(M21 F3):`[replication.traffic_weights]` 为准(权重
    /// ≥1 校验同 env 口径);子表缺席回退 env 测试钩子
    /// `FS3D_REPL_TRAFFIC_WEIGHTS`,再缺席 = 缺省权重。
    pub fn from_config_or_env(
        c: Option<&crate::config::ReplicationTrafficWeights>,
    ) -> Result<TrafficWeights, String> {
        let Some(c) = c else {
            return Self::from_env();
        };
        let w = TrafficWeights {
            serve: c.serve,
            backfill: c.backfill,
            on_demand: c.on_demand,
        };
        w.validate()?;
        Ok(w)
    }

    /// 权重 ≥1 校验(0 = 该类信用永不回充,等价配置死锁,装配期
    /// fail-fast 不静默)。
    fn validate(&self) -> Result<(), String> {
        for (k, v) in [
            ("serve", self.serve),
            ("backfill", self.backfill),
            ("on_demand", self.on_demand),
        ] {
            if v == 0 {
                return Err(format!(
                    "[replication.traffic_weights] {k} must be >= 1 (0 = 该类流量永久制动,等价配置死锁)"
                ));
            }
        }
        Ok(())
    }

    /// env 测试钩子回退(F3 后仅配置子表缺席时走到):
    /// `FS3D_REPL_TRAFFIC_WEIGHTS` 缺席 = 缺省权重;
    /// 形状非法 = 显式报错(启动 fail-fast,照 FS3D_REPL_* 先例)。
    pub fn from_env() -> Result<TrafficWeights, String> {
        match std::env::var("FS3D_REPL_TRAFFIC_WEIGHTS") {
            Ok(s) => s.parse(),
            Err(_) => Ok(TrafficWeights::default()),
        }
    }
}

/// 带优先级的共享令牌桶(语义见模块注释)。返回 `Arc` 强调共享语义:
/// 同一 `Arc<ReplTraffic>` 克隆给复制口(serve)与回填池(backfill/
/// on_demand),即中继节点全局同一流量预算。
#[derive(Debug)]
pub struct ReplTraffic {
    inner: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    /// 共享桶补充速率(字节/秒)。
    rate: f64,
    /// 共享桶容量(rate × BURST)。
    burst: f64,
    /// 共享桶余额(可为负 = 透支,条目原子口径同 Throttle)。
    tokens: f64,
    /// 每类保底信用余额(可为负;回充速率 = rate × share[i],顶 =
    /// burst × share[i])。
    credit: [f64; 3],
    /// 每类权重份额(w_i/W,合计 1)。
    share: [f64; 3],
    last: Instant,
}

impl ReplTraffic {
    /// 建桶(初始满突发额度,含各类保底信用满额,同 Throttle 初始满
    /// 桶口径)。
    pub fn new(rate_bytes_per_sec: u64, weights: TrafficWeights) -> Arc<Self> {
        let rate = rate_bytes_per_sec.max(1) as f64;
        let burst = rate * BURST.as_secs_f64();
        let total = (weights.serve + weights.backfill + weights.on_demand) as f64;
        let share = [
            weights.serve as f64 / total,
            weights.backfill as f64 / total,
            weights.on_demand as f64 / total,
        ];
        Arc::new(ReplTraffic {
            inner: Mutex::new(Bucket {
                rate,
                burst,
                tokens: burst,
                credit: [burst * share[0], burst * share[1], burst * share[2]],
                share,
                last: Instant::now(),
            }),
        })
    }

    /// 事实上不限速的独立桶(未注入共享桶的单角色节点/测试用:保持
    /// E2 前回填/按需拉取无节流的既有行为;共享语义必须由装配处注入
    /// 同一 `Arc` 才成立,见 main.rs cmd_serve)。
    pub fn unlimited() -> Arc<Self> {
        Self::new(1 << 50, TrafficWeights::default())
    }

    /// 类 `class` 开启下一个业务条目前是否该制动:共享桶已透支且本类
    /// 保底信用也已透支 = true(等回充;信用回充速率 = 保底份额,饥饿
    /// 不可能)。共享桶有余量时任一类放行(work conserving)。
    pub fn overdrawn(&self, class: TrafficClass) -> bool {
        let mut b = self.inner.lock().unwrap();
        Self::refill(&mut b);
        b.tokens <= 0.0 && b.credit[class.idx()] <= 0.0
    }

    /// 记账 `bytes` 字节(共享桶与本类信用同扣;允许透支为负,条目
    /// 原子口径同 Throttle::consume)。
    pub fn consume(&self, class: TrafficClass, bytes: u64) {
        let mut b = self.inner.lock().unwrap();
        Self::refill(&mut b);
        b.tokens -= bytes as f64;
        b.credit[class.idx()] -= bytes as f64;
    }

    /// 按真实流逝时间匀速回充:共享桶以 rate、各类信用以保底份额
    /// 速率,均以各自容量为顶。
    fn refill(b: &mut Bucket) {
        let now = Instant::now();
        let dt = now.duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + b.rate * dt).min(b.burst);
        for c in TrafficClass::ALL {
            let cap = b.burst * b.share[c.idx()];
            b.credit[c.idx()] = (b.credit[c.idx()] + b.rate * b.share[c.idx()] * dt).min(cap);
        }
        b.last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// 权重解析:缺省/正序/乱序/非法形状。
    #[test]
    fn traffic_weights_parse() {
        assert_eq!(
            "serve=100,backfill=50,on_demand=10"
                .parse::<TrafficWeights>()
                .unwrap(),
            TrafficWeights::default()
        );
        assert_eq!(
            "on_demand=7, serve=3 ,backfill=5"
                .parse::<TrafficWeights>()
                .unwrap(),
            TrafficWeights {
                serve: 3,
                backfill: 5,
                on_demand: 7
            }
        );
        assert!("serve=100,backfill=50".parse::<TrafficWeights>().is_err());
        assert!("serve=0,backfill=50,on_demand=10"
            .parse::<TrafficWeights>()
            .is_err());
        assert!("serve=1,backfill=1,foo=1"
            .parse::<TrafficWeights>()
            .is_err());
        assert!("garbage".parse::<TrafficWeights>().is_err());
    }

    /// M21 E2(ADR-33 裁定 4;设计稿 §3.5;TODO M21/E2 具名用例):
    /// **饥饿防护**——三类流量同时打满(每类独立线程饱和申领)时:
    /// ① 三类都前进(计数 > 0,低优先级不被饿死;读路径按需拉取 =
    ///    on_demand 打满也饿不死回填与投递);
    /// ② 份额按权重序:serve > backfill > on_demand,且各类拿到不低于
    ///    保底份额一半的进度(高优先级不被低优先级拖停);
    /// ③ 中途进度单调:打满窗口内 serve 持续前进(不停滞)。
    #[test]
    fn relay_priority_prevents_read_starvation() {
        let rate = 64u64 << 20; // 64 MiB/s
        let t = ReplTraffic::new(rate, TrafficWeights::default());
        let stop = Arc::new(AtomicBool::new(false));
        const CHUNK: u64 = 256 * 1024;
        let spawn_class = |class: TrafficClass| {
            let t = t.clone();
            let stop = stop.clone();
            let bytes = Arc::new(AtomicU64::new(0));
            let bytes2 = bytes.clone();
            let h = std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if t.overdrawn(class) {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    } else {
                        t.consume(class, CHUNK);
                        bytes2.fetch_add(CHUNK, Ordering::Relaxed);
                    }
                }
            });
            (h, bytes)
        };
        let (hs, serve) = spawn_class(TrafficClass::Serve);
        let (hb, backfill) = spawn_class(TrafficClass::Backfill);
        let (ho, on_demand) = spawn_class(TrafficClass::OnDemand);

        // ③ 中途单调性采样:打满 600ms 后 serve 有进度,再 900ms 后更多
        std::thread::sleep(std::time::Duration::from_millis(600));
        let serve_mid = serve.load(Ordering::Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(900));
        stop.store(true, Ordering::Relaxed);
        hs.join().unwrap();
        hb.join().unwrap();
        ho.join().unwrap();

        let (s, b, o) = (
            serve.load(Ordering::Relaxed),
            backfill.load(Ordering::Relaxed),
            on_demand.load(Ordering::Relaxed),
        );
        let total = (s + b + o) as f64;
        // 权重 100/50/10 → 保底份额 62.5% / 31.25% / 6.25%
        assert!(
            s > 0 && b > 0 && o > 0,
            "三类流量都必须前进: s={s} b={b} o={o}"
        );
        assert!(
            s as f64 >= total * 0.625 * 0.5,
            "serve 保底份额: s={s} total={total}"
        );
        assert!(
            b as f64 >= total * 0.3125 * 0.5,
            "backfill 保底份额: b={b} total={total}"
        );
        assert!(
            o as f64 >= total * 0.0625 * 0.4,
            "on_demand 不被饿死: o={o} total={total}"
        );
        assert!(s > b && b > o, "份额按权重序: s={s} b={b} o={o}");
        assert!(
            serve.load(Ordering::Relaxed) > serve_mid,
            "打满窗口内 serve 持续前进(不被低优先级拖停): mid={serve_mid}"
        );
    }
}
