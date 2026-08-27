//! 指标注册表(DESIGN §8 / TODO M3 H2)。
//!
//! 热路径纪律:请求计数与延迟直方图只用原子操作(无锁);
//! 错误码计数仅在错误路径写(错误本身是少数路径,用短锁 HashMap)。
//! Prometheus 文本渲染由 `render_prometheus` 输出,admin API 直接使用。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// S3 操作分类(固定集合,直方图按操作分桶)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    Get,
    Put,
    Head,
    Delete,
    ListObjects,
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    Multipart,
    Copy,
    Presign,
    Other,
}

impl Op {
    pub const ALL: [Op; 12] = [
        Op::Get,
        Op::Put,
        Op::Head,
        Op::Delete,
        Op::ListObjects,
        Op::ListBuckets,
        Op::CreateBucket,
        Op::DeleteBucket,
        Op::Multipart,
        Op::Copy,
        Op::Presign,
        Op::Other,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::Put => "put",
            Op::Head => "head",
            Op::Delete => "delete",
            Op::ListObjects => "list_objects",
            Op::ListBuckets => "list_buckets",
            Op::CreateBucket => "create_bucket",
            Op::DeleteBucket => "delete_bucket",
            Op::Multipart => "multipart",
            Op::Copy => "copy",
            Op::Presign => "presign",
            Op::Other => "other",
        }
    }
}

/// 延迟直方图 bucket 上界(秒,指数分布:0.25ms → 32s)。
pub const LATENCY_BUCKETS: [f64; 18] = [
    0.000_25, 0.000_5, 0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.0,
    2.0, 4.0, 8.0, 16.0, 32.0,
];

/// 响应状态分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    Success, // 2xx
    Client,  // 4xx
    Server,  // 5xx
}

fn class_of(status: u16) -> StatusClass {
    match status {
        200..=299 => StatusClass::Success,
        400..=499 => StatusClass::Client,
        _ => StatusClass::Server,
    }
}

/// 指标注册表(进程级单例,通过 Arc 共享)。
#[derive(Debug)]
pub struct Metrics {
    /// 请求量:op × 状态类。
    requests: [[AtomicU64; 3]; 12],
    /// 延迟直方图:op × bucket(累计计数,Prometheus 语义)。
    latency: [[AtomicU64; 18]; 12],
    /// 错误码计数(错误路径写,短锁)。
    errors: Mutex<BTreeMap<String, u64>>,
    /// 总请求量(快照/聚合用)。
    total: AtomicU64,
    /// 总错误量(4xx/5xx)。
    total_errors: AtomicU64,
    /// 传输字节(读方向)。
    bytes_read: AtomicU64,
    /// 传输字节(写方向)。
    bytes_written: AtomicU64,
    /// 时钟回拨跳跃计数(M4 D4:预签名对时钟敏感,回拨 → 告警指标)。
    clock_jumps: AtomicU64,
    /// 启动时间(uptime 计算)。
    started: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let requests = std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0)));
        let latency = std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0)));
        Metrics {
            requests,
            latency,
            errors: Mutex::new(BTreeMap::new()),
            total: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            clock_jumps: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    /// 记录一次请求完成(op, 状态码, 延迟, 传输字节)。
    pub fn record(&self, op: Op, status: u16, elapsed: std::time::Duration, bytes: u64) {
        let op_i = op as usize;
        let class = class_of(status);
        self.requests[op_i][class as usize].fetch_add(1, Ordering::Relaxed);
        let secs = elapsed.as_secs_f64();
        for (i, bound) in LATENCY_BUCKETS.iter().enumerate() {
            if secs <= *bound {
                self.latency[op_i][i].fetch_add(1, Ordering::Relaxed);
            }
        }
        if status >= 400 {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.total.fetch_add(1, Ordering::Relaxed);
        if status < 300 {
            if matches!(op, Op::Get | Op::Head) {
                self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
            } else {
                self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// 记录错误码(错误路径调用;4xx/5xx)。
    /// 时钟回拨事件计数(服务层检测到 SystemTime 向后跳跃时调用)。
    pub fn record_clock_jump(&self) {
        self.clock_jumps.fetch_add(1, Ordering::Relaxed);
    }

    /// 时钟回拨次数(指标/告警)。
    pub fn clock_jumps(&self) -> u64 {
        self.clock_jumps.load(Ordering::Relaxed)
    }

    pub fn record_error(&self, code: &str) {
        let mut m = self.errors.lock().unwrap();
        *m.entry(code.to_string()).or_insert(0) += 1;
    }

    /// 请求量:op × 状态类。
    pub fn request_count(&self, op: Op, class: StatusClass) -> u64 {
        self.requests[op as usize][class as usize].load(Ordering::Relaxed)
    }

    /// 总请求量。
    pub fn total_requests(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// 总错误量(4xx+5xx;仪表盘可用性请用 5xx 计数)。
    pub fn total_errors(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }

    /// 5xx 请求量(与 Grafana FastS3High5xxRate 同口径)。
    pub fn total_5xx(&self) -> u64 {
        Op::ALL
            .iter()
            .map(|op| self.request_count(*op, StatusClass::Server))
            .sum()
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// 延迟分位(秒):遍历累计直方图求分位。`p` 为 0.0~1.0。
    pub fn latency_count_for_test(&self, op: Op, idx: usize) -> u64 {
        self.latency[op as usize][idx].load(Ordering::Relaxed)
    }

    pub fn latency_quantile(&self, op: Op, p: f64) -> f64 {
        let arr = &self.latency[op as usize];
        // 桶是累计计数:样本总数 = +Inf 桶(= 最后一个桶)。
        let total = arr[arr.len() - 1].load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let target = (total as f64 * p) as u64;
        for (i, c) in arr.iter().enumerate() {
            if c.load(Ordering::Relaxed) >= target {
                return LATENCY_BUCKETS[i];
            }
        }
        *LATENCY_BUCKETS.last().unwrap()
    }

    /// 错误码计数快照。
    pub fn error_counts(&self) -> BTreeMap<String, u64> {
        self.errors.lock().unwrap().clone()
    }

    /// 所有操作 × 所有 bucket 的延迟直方图快照(渲染用)。
    fn latency_snapshot(&self) -> Vec<(Op, Vec<u64>)> {
        Op::ALL
            .iter()
            .map(|&op| {
                let v = self.latency[op as usize]
                    .iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();
                (op, v)
            })
            .collect()
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Prometheus 文本格式(admin `GET /v1/admin/metrics`)。
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);
        out.push_str(
            "# HELP fasts3_clock_jumps_total wall-clock backward jumps detected (M4 D4)\n",
        );
        out.push_str("# TYPE fasts3_clock_jumps_total counter\n");
        out.push_str(&format!(
            "fasts3_clock_jumps_total {}\n",
            self.clock_jumps()
        ));
        out.push_str("# HELP fasts3_requests_total S3 requests by operation and status class\n");
        out.push_str("# TYPE fasts3_requests_total counter\n");
        for &op in &Op::ALL {
            for (ci, class) in ["2xx", "4xx", "5xx"].iter().enumerate() {
                let v = self.request_count(
                    op,
                    match ci {
                        0 => StatusClass::Success,
                        1 => StatusClass::Client,
                        _ => StatusClass::Server,
                    },
                );
                out.push_str(&format!(
                    "fasts3_requests_total{{op=\"{}\",class=\"{}\"}} {}\n",
                    op.as_str(),
                    class,
                    v
                ));
            }
        }
        out.push_str("# HELP fasts3_errors_total S3 errors by AWS error code\n");
        out.push_str("# TYPE fasts3_errors_total counter\n");
        for (code, v) in self.error_counts() {
            out.push_str(&format!("fasts3_errors_total{{code=\"{code}\"}} {v}\n"));
        }
        out.push_str("# HELP fasts3_latency_seconds Request latency histogram\n");
        out.push_str("# TYPE fasts3_latency_seconds histogram\n");
        for (op, buckets) in self.latency_snapshot() {
            for (i, bound) in LATENCY_BUCKETS.iter().enumerate() {
                out.push_str(&format!(
                    "fasts3_latency_seconds_bucket{{op=\"{}\",le=\"{}\"}} {}\n",
                    op.as_str(),
                    bound,
                    buckets[i]
                ));
            }
            let sum: u64 = buckets.iter().sum();
            out.push_str(&format!(
                "fasts3_latency_seconds_bucket{{op=\"{}\",le=\"+Inf\"}} {}\n",
                op.as_str(),
                sum
            ));
            out.push_str(&format!(
                "fasts3_latency_seconds_count{{op=\"{}\"}} {}\n",
                op.as_str(),
                sum
            ));
        }
        out.push_str("# HELP fasts3_bytes_transferred Bytes transferred by direction\n");
        out.push_str("# TYPE fasts3_bytes_transferred counter\n");
        out.push_str(&format!(
            "fasts3_bytes_transferred{{dir=\"read\"}} {}\n",
            self.bytes_read()
        ));
        out.push_str(&format!(
            "fasts3_bytes_transferred{{dir=\"write\"}} {}\n",
            self.bytes_written()
        ));
        out.push_str("# HELP fasts3_uptime_seconds Process uptime\n");
        out.push_str("# TYPE fasts3_uptime_seconds gauge\n");
        out.push_str(&format!("fasts3_uptime_seconds {}\n", self.uptime_secs()));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn record_and_quantile() {
        let m = Metrics::new();
        m.record(Op::Get, 200, Duration::from_millis(1), 4096);
        m.record(Op::Get, 200, Duration::from_millis(3), 4096);
        m.record(Op::Get, 200, Duration::from_millis(10), 4096);
        m.record(Op::Get, 404, Duration::from_millis(1), 0);
        assert_eq!(m.request_count(Op::Get, StatusClass::Success), 3);
        assert_eq!(m.request_count(Op::Get, StatusClass::Client), 1);
        assert_eq!(m.total_requests(), 4);
        assert_eq!(m.total_errors(), 1);
        assert_eq!(m.total_5xx(), 0);
        // p50 ≤ 4ms(两个 1ms/3ms 请求)
        assert!(m.latency_quantile(Op::Get, 0.5) <= 0.004);
        // p100 = 16ms 桶(10ms 落 16ms 桶)
        assert_eq!(m.latency_quantile(Op::Get, 1.0), 0.016);
        assert_eq!(m.bytes_read(), 4096 * 3);
    }

    #[test]
    fn error_counts_and_render() {
        let m = Metrics::new();
        m.record_error("NoSuchKey");
        m.record_error("NoSuchKey");
        m.record_error("AccessDenied");
        let counts = m.error_counts();
        assert_eq!(counts["NoSuchKey"], 2);
        assert_eq!(counts["AccessDenied"], 1);
        let text = m.render_prometheus();
        assert!(text.contains("fasts3_errors_total{code=\"NoSuchKey\"} 2"));
        assert!(text.contains("fasts3_requests_total{op=\"get\",class=\"2xx\"} 0"));
        assert!(text.contains("fasts3_uptime_seconds "));
    }
}
