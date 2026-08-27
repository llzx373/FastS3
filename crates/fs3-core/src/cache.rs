//! 用户态热对象缓存(M14 H1-2;DESIGN §4.12)。
//!
//! - LRU 语义:HashMap 索引 + VecDeque 访问序;插入时按 `max_bytes` 惰性
//!   淘汰最久未用条目;
//! - 仅缓存**小对象**(≤ `max_object_size`)的整对象字节;任意 Range 命中
//!   时按区间裁剪(高频 Range 头直接命中);
//! - **默认关**;开启的内存预算由用户配置(§9.2 内存基线冲突的明示);
//! - **SSE 对象不入缓存**(解密字节与客户密钥作用域绑定的安全红线;
//!   加密对象的读路径解压/解密 CPU 无法被缓存规避,与 §9.1 预算一致);
//! - 命中率可观测:get/put/hit/miss/bytes 计数(admin /metrics 导出)。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// 缓存配置(快照至 fasts3.toml `[cache]`;默认全关)。
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub enabled: bool,
    /// 内存额度上限(字节;默认 256MiB)。
    pub max_bytes: u64,
    /// 仅缓存 ≤ 该大小的对象整字节(默认 2MiB)。
    pub max_object_size: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            enabled: false,
            max_bytes: 256 * 1024 * 1024,
            max_object_size: 2 * 1024 * 1024,
        }
    }
}

/// 命中率指标(原子计数;/metrics 导出)。
#[derive(Debug, Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserted: AtomicU64,
    pub evicted: AtomicU64,
    pub cached_bytes: AtomicU64,
    pub served_bytes: AtomicU64,
}

pub type CacheMetricsSnapshot = (u64, u64, u64, u64, u64, u64);

impl CacheMetrics {
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.inserted.load(Ordering::Relaxed),
            self.evicted.load(Ordering::Relaxed),
            self.cached_bytes.load(Ordering::Relaxed),
            self.served_bytes.load(Ordering::Relaxed),
        )
    }
}

/// 用户态 LRU(线程安全;管理面低频访问 + 读路径命中检查)。
pub struct ObjectCache {
    inner: Mutex<Inner>,
    config: CacheConfig,
    pub metrics: CacheMetrics,
}

struct Inner {
    keys: HashMap<String, usize>,
    order: VecDeque<usize>,
    slots: Vec<Option<Slot>>,
    free: Vec<usize>,
    total_bytes: u64,
}

struct Slot {
    key: String,
    bytes: Vec<u8>,
}

fn obj_key(bucket: &str, key: &str, version: Option<&[u8; 16]>, size: u64) -> String {
    match version {
        Some(v) => format!("{bucket}\0{key}\0{}\0{size}", hex::encode(v)),
        None => format!("{bucket}\0{key}\0-\0{size}"),
    }
}

impl ObjectCache {
    pub fn new(config: CacheConfig) -> Arc<Self> {
        let cap = (config.max_bytes / 4096).clamp(16, 1 << 20) as usize;
        Arc::new(ObjectCache {
            inner: Mutex::new(Inner {
                keys: HashMap::new(),
                order: VecDeque::new(),
                slots: (0..cap).map(|_| None).collect(),
                free: (0..cap).collect(),
                total_bytes: 0,
            }),
            config,
            metrics: CacheMetrics::default(),
        })
    }

    /// 命中 → 返回缓存整对象字节(调用方按 Range 裁剪)。
    pub fn get(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        size: u64,
    ) -> Option<Vec<u8>> {
        let k = obj_key(bucket, key, version, size);
        let mut inner = self.inner.lock().unwrap();
        let idx = *inner.keys.get(&k)?;
        // 访问序前移(先 clone 出 idx;再改 order——只借用 inner.order)
        if let Some(pos) = inner.order.iter().position(|&i| i == idx) {
            inner.order.remove(pos);
        }
        inner.order.push_back(idx);
        let out = inner.slots[idx].as_ref().map(|s| s.bytes.clone());
        out
    }

    /// 可从缓存服务的判定:开关 + 大小门槛 + 非 SSE(调用方约定,
    /// SSE 对象不调用本函数——见 op_get_object 注释)。
    pub fn eligible(&self, size: u64) -> bool {
        self.config.enabled && size > 0 && size <= self.config.max_object_size
    }

    /// 插入(整对象字节);超出额度惰性淘汰最久未用。
    pub fn insert(&self, bucket: &str, key: &str, version: Option<&[u8; 16]>, bytes: Vec<u8>) {
        if bytes.is_empty()
            || bytes.len() as u64 > self.config.max_object_size
            || bytes.len() as u64 > self.config.max_bytes
        {
            return;
        }
        let k = obj_key(bucket, key, version, bytes.len() as u64);
        let len = bytes.len() as u64;
        let mut inner = self.inner.lock().unwrap();
        // 已存在 → 原位更新并前移(take 旧值避免 &mut slots/slots 双借用)
        if inner.keys.contains_key(&k) {
            let idx = inner.keys[&k];
            if let Some(mut slot) = inner.slots[idx].take() {
                inner.total_bytes -= slot.bytes.len() as u64;
                slot.bytes = bytes;
                inner.total_bytes += len;
                inner.slots[idx] = Some(slot);
                if let Some(pos) = inner.order.iter().position(|&i| i == idx) {
                    inner.order.remove(pos);
                }
                inner.order.push_back(idx);
                self.metrics.inserted.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // 新条目
        let idx = inner.free.pop().unwrap_or_else(|| {
            // 无空闲槽:淘汰最久未用(先 take 槽再删键,避免借用冲突)
            let victim = inner
                .order
                .pop_front()
                .expect("cache order empty with no free slot");
            if let Some(slot) = inner.slots[victim].take() {
                inner.keys.remove(slot.key.as_str());
                inner.total_bytes -= slot.bytes.len() as u64;
                self.metrics.evicted.fetch_add(1, Ordering::Relaxed);
            }
            victim
        });
        inner.slots[idx] = Some(Slot {
            key: k.clone(),
            bytes,
        });
        inner.keys.insert(k, idx);
        inner.order.push_back(idx);
        inner.total_bytes += len;
        // 超额度:继续淘汰最久未用
        while inner.total_bytes > self.config.max_bytes && !inner.order.is_empty() {
            let victim = inner.order.pop_front().expect("order");
            if let Some(slot) = inner.slots[victim].take() {
                inner.keys.remove(slot.key.as_str());
                inner.total_bytes -= slot.bytes.len() as u64;
                self.metrics.evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.metrics.inserted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_insert_get_evict() {
        let cfg = CacheConfig {
            enabled: true,
            max_bytes: 12 * 1024,
            max_object_size: 8 * 1024,
        };
        let c = ObjectCache::new(cfg);
        // 三条 4KiB → 共 12KiB = max
        for i in 0..3 {
            let b = vec![i as u8; 4096];
            c.insert("b", &format!("k{i}"), None, b);
        }
        assert_eq!(c.get("b", "k0", None, 4096).unwrap()[0], 0);
        assert_eq!(c.total_bytes(), 12 * 1024);
        // 第四条 4KiB → 淘汰最久未用(k0 刚被访问过 → k1 被淘汰)
        c.insert("b", "k3", None, vec![9u8; 4096]);
        assert!(c.get("b", "k1", None, 4096).is_none(), "LRU 淘汰最久未用");
        assert_eq!(c.get("b", "k0", None, 4096).unwrap()[0], 0);
        assert_eq!(c.get("b", "k3", None, 4096).unwrap()[0], 9);
        // 访问顺序:k3 最新;k0 次之;k2 最久未用 → 再插入触发淘汰 k2
        c.insert("b", "k4", None, vec![7u8; 4096]);
        assert!(c.get("b", "k2", None, 4096).is_none());
        assert!(c.get("b", "k0", None, 4096).is_some());
    }

    #[test]
    fn version_keyed_separately() {
        let cfg = CacheConfig {
            enabled: true,
            max_bytes: 1 << 20,
            max_object_size: 1 << 20,
        };
        let c = ObjectCache::new(cfg);
        let v1 = [1u8; 16];
        let v2 = [2u8; 16];
        c.insert("b", "k", Some(&v1), vec![1u8; 8]);
        c.insert("b", "k", Some(&v2), vec![2u8; 8]);
        assert_eq!(c.get("b", "k", Some(&v1), 8).unwrap()[0], 1);
        assert_eq!(c.get("b", "k", Some(&v2), 8).unwrap()[0], 2);
    }

    #[test]
    fn size_gates() {
        let cfg = CacheConfig {
            enabled: true,
            max_bytes: 1 << 20,
            max_object_size: 64,
        };
        let c = ObjectCache::new(cfg);
        assert!(c.eligible(64));
        assert!(!c.eligible(65));
        assert!(!c.eligible(0));
        // 超上限的对象直接不插入
        c.insert("b", "big", None, vec![0u8; 128]);
        assert!(c.get("b", "big", None, 128).is_none());
    }
}
