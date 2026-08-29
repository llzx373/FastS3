//! fasts3_kms_* 指标(M20/F1;裸 AtomicU64 先例 = engine lib.rs:379)。
//!
//! 分账按 op+result;**key_id 不进标签**(防高基数)。

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct KmsMetrics {
    /// mint(写路径 wrap)累计:result=ok|error 两臂。
    pub mint_ok: AtomicU64,
    pub mint_err: AtomicU64,
    /// unwrap(读路径逐次在线解包)累计。
    pub unwrap_ok: AtomicU64,
    pub unwrap_err: AtomicU64,
    /// 任一错误累计(告警用单值)。
    pub error_total: AtomicU64,
    /// 延迟累计(微秒);延迟直方图后续版本再议,M20 只出均值口径。
    pub mint_micros: AtomicU64,
    pub unwrap_micros: AtomicU64,
}

impl KmsMetrics {
    pub fn record_mint(&self, ok: bool, micros: u64) {
        if ok {
            self.mint_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.mint_err.fetch_add(1, Ordering::Relaxed);
            self.error_total.fetch_add(1, Ordering::Relaxed);
        }
        self.mint_micros.fetch_add(micros, Ordering::Relaxed);
    }

    pub fn record_unwrap(&self, ok: bool, micros: u64) {
        if ok {
            self.unwrap_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.unwrap_err.fetch_add(1, Ordering::Relaxed);
            self.error_total.fetch_add(1, Ordering::Relaxed);
        }
        self.unwrap_micros.fetch_add(micros, Ordering::Relaxed);
    }

    /// Prometheus 文本(F1:key_id 不进任何标签)。
    pub fn render(&self) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        format!(
            "# HELP fasts3_kms_mint_total KMS mint (DEK wrap) calls.\n\
             # TYPE fasts3_kms_mint_total counter\n\
             fasts3_kms_mint_total{{result=\"ok\"}} {}\n\
             fasts3_kms_mint_total{{result=\"error\"}} {}\n\
             # HELP fasts3_kms_unwrap_total KMS unwrap (DEK unwrap, per-read online) calls.\n\
             # TYPE fasts3_kms_unwrap_total counter\n\
             fasts3_kms_unwrap_total{{result=\"ok\"}} {}\n\
             fasts3_kms_unwrap_total{{result=\"error\"}} {}\n\
             # HELP fasts3_kms_error_total KMS operation errors.\n\
             # TYPE fasts3_kms_error_total counter\n\
             fasts3_kms_error_total {}\n\
             # HELP fasts3_kms_duration_micros_total Cumulative KMS call duration in microseconds.\n\
             # TYPE fasts3_kms_duration_micros_total counter\n\
             fasts3_kms_duration_micros_total{{op=\"mint\"}} {}\n\
             fasts3_kms_duration_micros_total{{op=\"unwrap\"}} {}\n",
            g(&self.mint_ok),
            g(&self.mint_err),
            g(&self.unwrap_ok),
            g(&self.unwrap_err),
            g(&self.error_total),
            g(&self.mint_micros),
            g(&self.unwrap_micros),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_metrics_ops_attribution() {
        let m = KmsMetrics::default();
        m.record_mint(true, 100);
        m.record_mint(false, 50);
        m.record_unwrap(true, 200);
        let text = m.render();
        assert!(text.contains("fasts3_kms_mint_total{result=\"ok\"} 1"));
        assert!(text.contains("fasts3_kms_mint_total{result=\"error\"} 1"));
        assert!(text.contains("fasts3_kms_unwrap_total{result=\"ok\"} 1"));
        assert!(text.contains("fasts3_kms_error_total 1"));
        assert!(text.contains("fasts3_kms_duration_micros_total{op=\"mint\"} 150"));
        // key_id 不进标签
        assert!(!text.contains("key_id"));
    }
}
