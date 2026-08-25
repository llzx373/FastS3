//! rocksdb 封装:打开/配置、键值编解码、事务与组提交(E1/E2)。
//!
//! backstore 为 [rust-rocksdb](https://crates.io/crates/rocksdb) 的乐观事务
//! (OptimisticTransactionDB):事务语义与 sled 一致(冲突自动重试、Abort 即
//! 回滚),组提交窗口由后台线程按 `flush_every_ms` 批量 `flush_wal` 实现
//! (ADR-8,替代 sled 内建 `flush_every_ms`)。

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use fs3_core::{AllocRecord, BucketMeta, Error, ObjectMeta, Result, Segment, MAX_OBJECT_SIZE};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, Direction, Error as RocksError, ErrorKind,
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options, Transaction,
    WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::keys::*;

pub mod keys;

/// M11 L3-1(ADR-12 DL5):`s:audit` 审计持久化环形。
pub mod audit;

pub use audit::AuditStore;

/// 元数据同步模式(DESIGN §4.4 / E2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// 组提交:flush_every_ms 窗口批量落盘(默认)。
    #[default]
    Group,
    /// 每个事务显式 fsync。
    Full,
    /// 不主动落盘(用户声明 HA 层可容忍单机丢失,如纯缓存集群)。
    None,
}

#[derive(Debug, Clone)]
pub struct MetaConfig {
    pub flush_every_ms: u64,
    pub sync_mode: SyncMode,
    /// rocksdb block cache 容量(字节);None = rocksdb 默认。
    pub cache_capacity: Option<u64>,
}

impl Default for MetaConfig {
    fn default() -> Self {
        MetaConfig {
            flush_every_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            sync_mode: SyncMode::Group,
            cache_capacity: None,
        }
    }
}

/// 分配器变更草稿(随事务写入 a:/t: 记录)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocDraft {
    pub alloc: Vec<(u64, u64)>,
    pub ref_inc: Vec<u64>,
    pub ref_dec: Vec<u64>,
}

impl AllocDraft {
    pub fn is_empty(&self) -> bool {
        self.alloc.is_empty() && self.ref_inc.is_empty() && self.ref_dec.is_empty()
    }
}

/// 桶统计增量。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsDelta {
    pub objects: i64,
    pub bytes: i64,
}

/// SSE-S3 KEK 代状态(M11 K1-1,ADR-12 DS1;键 `s:sse_kek_gen`,值 =
/// postcard 本结构)。**无密钥材料**:gen 是代数编号,KEK 本体由
/// `s:sse_kek_seed` 按代确定性派生,永不落盘/下发。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseKekGenState {
    /// 当前 KEK 代(从 1 起;当前代 = 最大代,新对象/新会话签发用)。
    pub gen: u32,
    /// 末次轮换 unix 秒(admin 状态端点展示;初始代 = 0 = 未轮换过)。
    pub last_rotated_at: i64,
    /// 重包裹已收敛到的代数(后台 rewrap 完成标记;`gen >
    /// rewrap_done_gen` = 存在待重包裹对象——重启后经本字段判定续跑,
    /// 重跑幂等:已重包裹条目 kek_id == gen 自然跳过)。
    pub rewrap_done_gen: u32,
}

/// SSE-S3 会话密钥材料(M11 K1-1,ADR-12 DS1;随 MultipartSession 尾部
/// 追加落盘):Create 携带 SSE-S3 意愿(显式 AES256 头或桶默认)时签发
/// 会话级 DEK,**只存 KEK 包裹值,DEK 明文零落盘**(UploadPart/
/// UploadPartCopy/Complete 按 kek_id 派生 KEK 现解现用,内存用完即擦)。
/// 会话级单 DEK(裁决写死,备选 = 每 part 随机 DEK):part nonce_base 照
/// D-E6 由会话 DEK 确定性派生,同 part 重传 ⇒ 同密文同 ETag,重传幂等
/// 与 SSE-C 同口径;每 part 随机 DEK 则重传 ETag 漂移 → Complete
/// InvalidPart,是正确性回退,否决。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSseS3 {
    /// 签发时的 KEK 代(轮换后旧代仍可由 seed 确定性派生,可读性不受影响;
    /// Complete 落对象时按当时当前代重签对象级 DEK)。
    pub kek_id: u32,
    /// AES-256-GCM(KEK, 会话 DEK)包裹值(nonce‖ct‖tag 60B)。
    pub wrapped_dek: Vec<u8>,
}

/// 分片上传会话(DESIGN §4.7;键 `u:{uploadId}`,桶索引 `m:{bucket}\0{uploadId}`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartSession {
    pub bucket: String,
    pub key: String,
    /// CreateMultipartUpload 携带的 Content-Type(Complete 时落到对象上)。
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// Create 时携带的回显头(M9 C3/D5:Content-Encoding/Cache-Control/Expires;
    /// Complete 时落到对象。序列化尾部追加字段,decode_session 双读兼容
    /// v1.0.0 存量会话值)。
    pub resp_headers: Vec<(String, String)>,
    pub created: i64,
    /// 已完成标记:二次 Complete 幂等返回;分片重传后清位(reactivate)。
    pub completed: bool,
    /// 完成结果快照(幂等重放:返回相同 ETag/Size/LastModified)。
    pub final_etag: [u8; 16],
    pub final_size: u64,
    pub final_mtime: i64,
    /// Create 时 x-amz-tagging 头携带的对象标签(M10 S1;Complete 时落到
    /// 对象。序列化尾部追加字段,decode_session 三读回退,存量会话按空表)。
    pub tags: Vec<(String, String)>,
    /// Create 时 x-amz-checksum-algorithm 声明的算法(M11 C1-4 门禁补强;
    /// Complete 按会话算法代算对象级 checksum——客户端未送复合头也落值,
    /// AWS 口径。类型恒为算法默认类型:非默认组合在协议层 Create 显式
    /// 拒绝,故无需落 ChecksumType。序列化尾部追加字段,decode_session
    /// 四读回退,存量会话按 None)。
    pub checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
    /// SSE-C 会话绑定(M11 E1-4,ADR-12 DE2):Create 携带 SSE-C 三头时落
    /// key-MD5(base64 请求原文,UploadPart/UploadPartCopy/Complete 一致性
    /// 校验与响应回显用)。**红线:只存 key-MD5,客户密钥本体零落盘**
    /// (DE1;密钥每次请求自带)。序列化尾部追加字段,decode_session 五读
    /// 回退,存量会话按 None(无 SSE)。
    pub sse_key_md5: Option<String>,
    /// SSE-S3 会话绑定(M11 K1-1,ADR-12 DS1):Create 的 SSE-S3 意愿
    /// (显式 AES256 头或桶默认)落本字段;与 sse_key_md5 互斥(Create 处
    /// 二选一,协议层判定)。序列化尾部追加字段,decode_session 六读回退,
    /// 存量会话按 None。
    pub sse_s3: Option<SessionSseS3>,
}

/// 当前 Unix 秒(会话时间戳用)。
fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl MultipartSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: &str,
        key: &str,
        content_type: &str,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<SessionSseS3>,
    ) -> Self {
        MultipartSession {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.to_string(),
            user_meta,
            resp_headers,
            created: now_ts(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags,
            checksum_alg,
            sse_key_md5,
            sse_s3,
        }
    }
}

/// 分片元数据(键 `p:{uploadId}\0{part_no be32}`;数据在 extents 或 inline)。
/// ADR-9:v2 起 `extents` 为段列表(Vec<Segment>)。
/// ADR-12 D-E3(M11 C1-4):尾部追加 `checksum` 字段;decode_part 双读
/// (旧值缺省 None),照 ObjectMeta v2→v3 / MultipartSession 先例,零迁移。
/// M11 E1-4(ADR-12 D-E4):尾部再追加 `sse` 字段(SSE-C 分片独立加密的
/// nonce_base + chunk_tags;Complete 重加密后对象级 SseInfo 落 ObjectMeta,
/// 分片记录随即删除);decode_part 三读回退,存量分片按 None(未加密)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartMeta {
    pub size: u64,
    pub etag: [u8; 16],
    pub mtime: i64,
    pub extents: Vec<Segment>,
    pub inline: Option<Vec<u8>>,
    /// 分片 checksum(UploadPart 客户端提供算法时引擎边写边算落值;
    /// None = 未提供)。Complete 逐分片比对与复合值重算的输入。
    pub checksum: Option<fs3_core::ChecksumInfo>,
    /// 分片 SSE-C 加密产物(M11 E1-4;DE2:每 part 独立加密,part 内
    /// 64KiB 网格,inline 密文同口径;nonce_base 由 D-E6 确定性派生,
    /// None = 未加密分片)。客户密钥零落盘——此处只有 nonce/tag/key_md5
    /// 校验子(D-E5),无密钥材料。
    pub sse: Option<fs3_core::SseInfo>,
}

impl PartMeta {
    pub fn etag_hex(&self) -> String {
        self.etag.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// D9 桶级配置文档类型(ADR-11 D9;M10 S1/S2/S3/S7):独立键前缀存储,不并入
/// BucketMeta 值(配置文档可达数 KB,避免桶记录膨胀;M8 `l:` location 先例)。
/// 值 = 协议层校验后的规范化文档(S3 桶策略为原始 JSON 文本;其余为规范化
/// XML);删桶时与同事务清理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketConf {
    /// CORS 配置(S2;键 `bc:{bucket}`)。
    Cors,
    /// 桶级标签(S1;键 `bt:{bucket}`;ADR-11 D8)。
    Tagging,
    /// Ownership Controls(S7;键 `bo:{bucket}`)。
    Ownership,
    /// 桶策略(S3;键 `bp:{bucket}`;值 = 原始 JSON 文本,逐字节回显)。
    Policy,
}

impl BucketConf {
    /// 全部配置文档前缀(delete_bucket 事务清理用;新增 D9 前缀须在此登记,
    /// 并同步 fs3d meta-export/import DTO——演进纪律 DESIGN-FUTURE §2.2;
    /// check 可达性扫描只读 `o:`/`p:` 段引用键,对配置键天然安全)。
    pub const ALL: [BucketConf; 4] = [
        BucketConf::Cors,
        BucketConf::Tagging,
        BucketConf::Ownership,
        BucketConf::Policy,
    ];

    pub fn key(self, bucket: &str) -> Vec<u8> {
        let prefix = match self {
            BucketConf::Cors => PREFIX_BUCKET_CORS,
            BucketConf::Tagging => PREFIX_BUCKET_TAGGING,
            BucketConf::Ownership => PREFIX_BUCKET_OWNERSHIP,
            BucketConf::Policy => PREFIX_BUCKET_POLICY,
        };
        bucket_conf_key(prefix, bucket)
    }
}

/// 元数据操作(单事务应用,顺序执行)。
#[derive(Debug, Clone)]
pub enum Op {
    BucketPut {
        name: String,
        meta: BucketMeta,
        /// 创建时 LocationConstraint(M8 回显语义;None/"" = us-east-1 默认)。
        /// Op 不落盘(瞬态事务指令),扩字段无版本兼容问题。
        location: Option<String>,
    },
    BucketDelete {
        name: String,
    },
    ObjectPut {
        bucket: String,
        key: String,
        meta: ObjectMeta,
    },
    /// 压缩迁移(ADR-9 §6.2 阶段 3):事务内读对象并校验旧段仍被引用,
    /// 把 `old_segments` 按序替换为 `new_segments`;被并发覆盖/删除 →
    /// 返回 Error::ObjectChanged(调用方放弃该对象,下轮再来)。
    ObjectMigrate {
        bucket: String,
        key: String,
        old_segments: Vec<Segment>,
        new_segments: Vec<Segment>,
    },
    /// 压缩迁移的分片变体(REVIEW §3.8:ADR-9 §6.2「o:+p: 双前缀」兑现;
    /// 事务内读分片并校验旧段仍被引用,替换 extents;分片被 abort/覆盖 →
    /// Error::ObjectChanged,调用方放弃该分片,下轮再来)。
    PartMigrate {
        upload_id: String,
        part_no: u32,
        old_segments: Vec<Segment>,
        new_segments: Vec<Segment>,
    },
    /// 桶版本化状态单事务更新(ADR-11 D1;V3 PutBucketVersioning):
    /// 只改写 BucketMeta.versioning,其余字段(含 l: location)原样保留;
    /// 桶不存在 → NotFound。状态机合法性(Enabled→Off 禁止等)由调用方
    /// (协议层)判定。
    BucketSetVersioning {
        name: String,
        state: fs3_core::VersioningState,
    },
    /// 桶默认加密单事务读改写(M11 K1-2/K1-3,ADR-12 DS2/DS3;
    /// Put/DeleteBucketEncryption 落地):只改写 BucketMeta.default_encryption
    /// (D0 预留字段填充,不改结构),其余字段原样保留;桶不存在 →
    /// NotFound。Delete 幂等(None → None 同样 Ok)。
    BucketSetEncryption {
        name: String,
        default: Option<fs3_core::SseAlgorithm>,
    },
    /// 桶 Object Lock 配置(M12 W2-2,ADR-13):enabled 恒 true(不可关闭);
    /// 开启时同时把 versioning 置 Enabled;只改这两处 + default_retention,
    /// 其余字段原样保留;桶不存在 → NotFound。
    BucketSetObjectLock {
        name: String,
        default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    },
    /// D9 桶级配置文档写入(M10 S1/S2/S7;值 = 规范化 XML;覆盖语义)。
    /// 桶不存在 → NotFound(与 BucketSetVersioning 同事务内校验)。
    BucketConfPut {
        bucket: String,
        conf: BucketConf,
        value: Vec<u8>,
    },
    /// D9 桶级配置文档删除(幂等:无配置同样 Ok;DeleteBucketTagging 等
    /// 的 AWS 幂等语义)。桶不存在 → NotFound。
    BucketConfDelete {
        bucket: String,
        conf: BucketConf,
    },
    /// 生命周期规则单事务整体替换(M11 L1;ADR-12 DL1:读旧写新——
    /// 事务内扫描 `r:{bucket}\0` 旧规则键全删,再逐条写入新规则;规则集
    /// 为空 = 纯清除)。桶不存在 → NotFound。
    LifecycleRulesReplace {
        bucket: String,
        rules: Vec<fs3_core::LifecycleRule>,
    },
    /// 生命周期规则整桶清除(幂等:无规则同样 Ok,DeleteBucketLifecycle-
    /// Configuration 的 AWS 204 幂等语义)。桶不存在 → NotFound。
    LifecycleRulesDelete {
        bucket: String,
    },
    /// 对象标签单事务读改写(M10 S1;PutObjectTagging/DeleteObjectTagging
    /// 落地):`vk = None` → 未版本化单键 `o:{b}\0{k}`;`Some(vk)` → 版本键
    /// (含 VK_NULL null 槽)。仅 tags 字段变更,不触碰数据段/统计;
    /// 目标条目不存在 → NotFound。删除标记判定在引擎层先于提交完成。
    ObjectSetTags {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        tags: Vec<(String, String)>,
    },
    ObjectDelete {
        bucket: String,
        key: String,
    },
    /// 删除当前版本(ADR-11 §3.4.3;版本化桶):写删除标记条目。
    /// `vk = Some`:版本键 `o:{bucket}\0{esc(key)}\0{vk16}`(Enabled = 调用方
    /// 生成的新 vk,Suspended = VK_NULL 槽位原地覆盖);`vk = None`:D1a-1
    /// ——Suspended 桶存在 Off 时代遗留未版本化单键时**原地覆盖该单键**
    /// (对外 VersionId 恒 "null",遗留单键与 null 槽不共存)。
    /// marker 由调用方构造(is_delete_marker=true、size=0、extents/inline
    /// 空,事务臂校验);不触碰数据段。未版本化桶仍走 ObjectDelete 物理删除。
    ObjectDeleteCurrent {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        marker: ObjectMeta,
    },
    /// 物理删除指定版本(DELETE ?versionId 语义):删除版本键条目;
    /// 段释放与统计扣减由同事务的 Alloc/Stats 携带(调用方计算)。
    ObjectDeleteVersion {
        bucket: String,
        key: String,
        vk: [u8; 16],
    },
    /// 版本化对象 PUT(ADR-11 D1;V2 引擎分叉):写版本键
    /// `o:{bucket}\0{esc(key)}\0{vk16}`(Enabled = 调用方生成的新 vk;
    /// Suspended = VK_NULL 槽原地覆盖,同事务读改写)。不触碰旧版本条目;
    /// 覆盖写的新段记账与旧 null 版本段释放由同事务 Alloc/Stats 携带。
    ObjectPutVersion {
        bucket: String,
        key: String,
        vk: [u8; 16],
        meta: ObjectMeta,
    },
    /// 值格式在线重写(M10 V5-3;ADR-11 D0):按**原始键**单事务重编码
    /// 对象值(写入恒 v3),不改统计/分配、不校验桶存在与删除标记契约
    /// —— 值内容原样往返,仅值格式版本字节 +1。键必须已存在(否则
    /// NotFound);供 `fasts3d rewrite-values` 离线/维护窗口使用。
    ObjectMetaRewrite {
        key: Vec<u8>,
        meta: ObjectMeta,
    },
    /// 分配器变更(写入 a:{seq} + t:{seq};seq 由事务内部分配)
    Alloc {
        draft: AllocDraft,
    },
    /// 桶统计增量(与对象操作同事务记账,E4 最小形态)
    Stats {
        bucket: String,
        delta: StatsDelta,
    },
    /// 创建分片会话:写 `u:{uploadId}` + 桶索引 `m:{bucket}\0{uploadId}`。
    MultipartCreate {
        upload_id: String,
        session: MultipartSession,
    },
    /// 更新会话标志(completed/final 快照;读改写保证冲突集)。
    MultipartUpdate {
        upload_id: String,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    },
    /// 删除会话 + 桶索引。
    MultipartDelete {
        upload_id: String,
    },
    /// 写分片(覆盖已存在分片)。
    PartPut {
        upload_id: String,
        part_no: u32,
        meta: PartMeta,
    },
    /// 按原始键删除分片(键在事务外枚举,事务内先读建立冲突集)。
    PartDelete {
        key: Vec<u8>,
    },
    /// 写访问密钥(`k:{access_key}` → KeyRecord;M3 密钥 CRUD)。
    KeyPut {
        access_key: String,
        record: fs3_core::KeyRecord,
    },
    /// 删除访问密钥。
    KeyDelete {
        access_key: String,
    },
}

/// 元数据层运行统计(H2 指标:WAL 组提交)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetaStats {
    /// WAL 组提交 flush 次数。
    pub wal_flush_count: u64,
    /// 累计 WAL flush 字节数。
    pub wal_flush_bytes: u64,
}

/// 组提交刷盘线程(sled `flush_every_ms` 语义的 rocksdb 等价物,ADR-8)。
///
/// rocksdb 无内建"每 N ms 刷 WAL"定时器;开启 `manual_wal_flush` 后 WAL 写入
/// 停留在内存缓冲,由本线程每 `flush_every_ms` 调用一次 `flush_wal(true)`
/// (write + fsync)批量落盘。窗口内 kill -9 的数据丢失语义与 sled 一致。
struct Flusher {
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    join: Option<JoinHandle<()>>,
    /// 组提交统计(H2)。
    stats: Arc<MetaStatsAtomic>,
}

#[derive(Debug, Default)]
struct MetaStatsAtomic {
    flush_count: AtomicU64,
    flush_bytes: AtomicU64,
}

impl Flusher {
    fn spawn(db: Arc<OptimisticTransactionDB>, every_ms: u64) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(()), Condvar::new()));
        let stats = Arc::new(MetaStatsAtomic::default());
        let (s, w) = (stop.clone(), wake.clone());
        let st = stats.clone();
        let join = std::thread::Builder::new()
            .name("fs3-meta-flusher".to_string())
            .spawn(move || {
                let (m, cv) = &*w;
                loop {
                    let guard = m.lock().unwrap();
                    let (guard, _) = cv
                        .wait_timeout(guard, Duration::from_millis(every_ms))
                        .unwrap();
                    drop(guard);
                    if s.load(Ordering::Acquire) {
                        break;
                    }
                    // 组提交窗口到期:WAL 批量 write + fsync
                    match db.flush_wal(true) {
                        Ok(()) => {
                            st.flush_count.fetch_add(1, Ordering::Relaxed);
                            // rocksdb.wal-bytes 属性:当前 WAL 大小(近似累计)
                            if let Ok(Some(sz)) = db.property_value("rocksdb.wal-bytes") {
                                if let Ok(n) = sz.trim().parse::<u64>() {
                                    st.flush_bytes.fetch_add(n, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            // 刷盘失败不 panic:下一窗口重试,调用方仍可显式 flush
                            eprintln!("fs3-meta: flush_wal failed: {e}");
                        }
                    }
                }
            })
            .map_err(|e| Error::Meta(format!("spawn flusher thread: {e}")))?;
        Ok(Flusher {
            stop,
            wake,
            join: Some(join),
            stats,
        })
    }

    fn stats(&self) -> MetaStats {
        MetaStats {
            wal_flush_count: self.stats.flush_count.load(Ordering::Relaxed),
            wal_flush_bytes: self.stats.flush_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (m, cv) = &*self.wake;
        let _g = m.lock().unwrap();
        cv.notify_all();
        drop(_g);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

pub struct MetaStore {
    db: Arc<OptimisticTransactionDB>,
    sync_mode: SyncMode,
    /// 事务提交写选项(None 模式 disable WAL)。
    write_opts: WriteOptions,
    /// 乐观事务选项:事务开始即取快照,读集参与提交冲突检测。
    txn_opts: OptimisticTransactionOptions,
    /// 组提交刷盘线程句柄:仅靠 Drop 停止/回收线程(字段本身不读取)。
    #[allow(dead_code)]
    flusher: Option<Flusher>,
    /// 桶级生命周期规则缓存(GET/HEAD 每请求求 x-amz-expiration;
    /// 无规则桶避免反复 prefix scan。put/delete 规则随 commit 失效)。
    lifecycle_cache: Mutex<HashMap<String, Vec<fs3_core::LifecycleRule>>>,
}

/// rocksdb 错误 → fs3 Error。
fn rocks_err(e: RocksError) -> Error {
    Error::Meta(format!("rocksdb: {e}"))
}

/// postcard(serde 原生,积极维护)编码。
fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| Error::Meta(format!("postcard encode: {e}")))
}

/// postcard 解码。
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> {
    postcard::from_bytes(v).map_err(|e| Error::Corrupt(format!("postcard decode: {e}")))
}

/// M11/L5 双读:LifecycleRule 值格式尾部追加 `legacy_prefix` 字段;新格式
/// 优先,失败回退 L1 初版格式(legacy_prefix=false——存量规则均为 Filter
/// 归一渲染形态)。照 decode_session 先例,零迁移。
fn decode_lifecycle_rule(v: &[u8]) -> Result<fs3_core::LifecycleRule> {
    /// L1 初版规则格式(无 legacy_prefix;L5 回退用)。
    #[derive(serde::Deserialize)]
    struct RuleV12 {
        id: String,
        status: fs3_core::LifecycleStatus,
        filter: fs3_core::LifecycleFilter,
        expiration: Option<fs3_core::LifecycleExpiration>,
        noncurrent_expiration: Option<fs3_core::NoncurrentVersionExpiration>,
        abort_incomplete_multipart: Option<fs3_core::AbortIncompleteMultipartUpload>,
    }
    match postcard::from_bytes::<fs3_core::LifecycleRule>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            let old: RuleV12 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode lifecycle rule: {e}")))?;
            Ok(fs3_core::LifecycleRule {
                id: old.id,
                status: old.status,
                filter: old.filter,
                expiration: old.expiration,
                noncurrent_expiration: old.noncurrent_expiration,
                abort_incomplete_multipart: old.abort_incomplete_multipart,
                legacy_prefix: false,
            })
        }
    }
}

/// M9/C3 双读:MultipartSession 值格式尾部追加 `resp_headers` 字段;
/// 新格式优先,失败回退 v1.0.0 格式(空表),存量会话保持可读。
/// M10/S1:尾部再追加 `tags` 字段,回退链扩为三层(含 resp_headers 无
/// tags 的 v1.1.0 格式 → tags 空表)。
/// M11/C1-4 门禁补强:尾部再追加 `checksum_alg` 字段,回退链扩为四层
/// (含 tags 无 checksum_alg 的 M11 初版格式 → checksum_alg None)。
/// M11/E1-4:尾部再追加 `sse_key_md5` 字段,回退链扩为五层(含
/// checksum_alg 无 sse_key_md5 的格式 → sse_key_md5 None = 无 SSE 会话)。
/// M11/K1-1:尾部再追加 `sse_s3` 字段,回退链扩为六层(含 sse_key_md5 无
/// sse_s3 的格式 → sse_s3 None = 无 SSE-S3 会话)。
fn decode_session(v: &[u8]) -> Result<MultipartSession> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacySession {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    }
    /// v1.1.0 会话格式(含 resp_headers,无 tags;M10 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV11 {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    }
    /// M11 初版会话格式(含 tags,无 checksum_alg;门禁补强回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12 {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
    }
    /// M11 E1-4 前会话格式(含 tags + checksum_alg,无 sse_key_md5;
    /// E1-4 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12b {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
    }
    /// M11 K1-1 前会话格式(含 sse_key_md5,无 sse_s3;K1-1 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12c {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
    }
    #[allow(clippy::too_many_arguments)]
    fn into_session(
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
    ) -> MultipartSession {
        MultipartSession {
            bucket,
            key,
            content_type,
            user_meta,
            resp_headers,
            created,
            completed,
            final_etag,
            final_size,
            final_mtime,
            tags,
            checksum_alg,
            sse_key_md5,
            sse_s3: None,
        }
    }
    match postcard::from_bytes::<MultipartSession>(v) {
        Ok(s) => Ok(s),
        Err(_) => match postcard::from_bytes::<SessionV12c>(v) {
            Ok(s) => Ok(into_session(
                s.bucket,
                s.key,
                s.content_type,
                s.user_meta,
                s.resp_headers,
                s.created,
                s.completed,
                s.final_etag,
                s.final_size,
                s.final_mtime,
                s.tags,
                s.checksum_alg,
                s.sse_key_md5,
            )),
            Err(_) => match postcard::from_bytes::<SessionV12b>(v) {
                Ok(s) => Ok(into_session(
                    s.bucket,
                    s.key,
                    s.content_type,
                    s.user_meta,
                    s.resp_headers,
                    s.created,
                    s.completed,
                    s.final_etag,
                    s.final_size,
                    s.final_mtime,
                    s.tags,
                    s.checksum_alg,
                    None,
                )),
                Err(_) => match postcard::from_bytes::<SessionV12>(v) {
                    Ok(s) => Ok(into_session(
                        s.bucket,
                        s.key,
                        s.content_type,
                        s.user_meta,
                        s.resp_headers,
                        s.created,
                        s.completed,
                        s.final_etag,
                        s.final_size,
                        s.final_mtime,
                        s.tags,
                        None,
                        None,
                    )),
                    Err(_) => match postcard::from_bytes::<SessionV11>(v) {
                        Ok(s) => Ok(into_session(
                            s.bucket,
                            s.key,
                            s.content_type,
                            s.user_meta,
                            s.resp_headers,
                            s.created,
                            s.completed,
                            s.final_etag,
                            s.final_size,
                            s.final_mtime,
                            Vec::new(),
                            None,
                            None,
                        )),
                        Err(_) => {
                            let legacy: LegacySession = postcard::from_bytes(v).map_err(|e| {
                                Error::Corrupt(format!("postcard decode session: {e}"))
                            })?;
                            Ok(into_session(
                                legacy.bucket,
                                legacy.key,
                                legacy.content_type,
                                legacy.user_meta,
                                Vec::new(),
                                legacy.created,
                                legacy.completed,
                                legacy.final_etag,
                                legacy.final_size,
                                legacy.final_mtime,
                                Vec::new(),
                                None,
                                None,
                            ))
                        }
                    },
                },
            },
        },
    }
}

/// M11 C1-4 双读(ADR-12 D-E3):PartMeta 值尾部追加 `checksum` 字段;
/// 新格式优先,失败回退无 checksum 旧格式(补 None),存量分片零迁移
/// 读取(回退仅发生在尾部字段缺失——旧值解码新结构恒因字节不足失败,
/// 不会误判;照 decode_session / ObjectMeta v2→v3 先例)。
/// M11 E1-4(ADR-12 D-E4):尾部再追加 `sse` 字段,回退链扩为三层(含
/// checksum 无 sse 的格式 → sse None = 未加密分片)。
fn decode_part(v: &[u8]) -> Result<PartMeta> {
    /// 旧格式(v1.1.0;无 checksum/sse 尾部字段;双读回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacyPartMeta {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        inline: Option<Vec<u8>>,
    }
    /// M11 E1-4 前分片格式(含 checksum,无 sse;E1-4 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct PartMetaV12 {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        inline: Option<Vec<u8>>,
        checksum: Option<fs3_core::ChecksumInfo>,
    }
    match postcard::from_bytes::<PartMeta>(v) {
        Ok(p) => Ok(p),
        Err(_) => match postcard::from_bytes::<PartMetaV12>(v) {
            Ok(p) => Ok(PartMeta {
                size: p.size,
                etag: p.etag,
                mtime: p.mtime,
                extents: p.extents,
                inline: p.inline,
                checksum: p.checksum,
                sse: None,
            }),
            Err(_) => {
                let l: LegacyPartMeta = postcard::from_bytes(v)
                    .map_err(|e| Error::Corrupt(format!("postcard decode part meta: {e}")))?;
                Ok(PartMeta {
                    size: l.size,
                    etag: l.etag,
                    mtime: l.mtime,
                    extents: l.extents,
                    inline: l.inline,
                    checksum: None,
                    sse: None,
                })
            }
        },
    }
}

fn decode_bucket(v: &[u8]) -> Result<BucketMeta> {
    // M10/ADR-11:值 = [BUCKET_META_VERSION] + postcard(BucketMeta);
    // 存量无版本字节值(v1.1.0/v1.0.0)由 fs3-core 双读回退,零迁移读取。
    BucketMeta::decode_value(v)
}

fn decode_object(v: &[u8]) -> Result<ObjectMeta> {
    // ADR-9 §13:值 = [版本字节] + postcard(ObjectMeta);旧值(无版本字节)拒绝
    ObjectMeta::decode_value(v)
}

fn decode_alloc(v: &[u8]) -> Result<AllocRecord> {
    decode(v).map_err(|e| Error::Corrupt(format!("alloc record: {e}")))
}

/// 前缀扫描(惰性迭代;调用方 break 即停止)。
fn scan_prefix<'a>(
    db: &'a OptimisticTransactionDB,
    prefix: &'a [u8],
) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
    db.iterator(IteratorMode::From(prefix, Direction::Forward))
        .map_while(|item| match item {
            Ok((k, v)) if k.starts_with(prefix) => Some(Ok((k.to_vec(), v.to_vec()))),
            Ok(_) => None,
            Err(e) => Some(Err(rocks_err(e))),
        })
}

/// 分页列举结果(delimiter 分组后;`last_scanned` 为游标,供续扫)。
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub items: Vec<(String, ObjectMeta)>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// 最后一个被扫描到的原始键(续扫游标,严格大于)。
    pub last_scanned: Option<String>,
}

/// ListObjectVersions 条目(ADR-11 §3.4.4 + D1a-3;V3)。
#[derive(Debug, Clone)]
pub struct VersionListEntry {
    pub key: String,
    /// 展示 vk(null 族——遗留单键/null 槽——恒为 VK_NULL,协议层渲染 "null")。
    pub vk: [u8; 16],
    pub meta: ObjectMeta,
    /// D1a 当前版本判定(键内 mtime 裁决,与列表位置独立)。
    pub is_latest: bool,
}

/// ListObjectVersions 页(版本/删除标记条目混合,is_delete_marker 区分)。
#[derive(Debug, Clone, Default)]
pub struct VersionListPage {
    pub entries: Vec<VersionListEntry>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// 续扫游标:(键, Some(vk)) = 版本条目;(公共前缀, None) = 前缀条目。
    pub last_scanned: Option<(String, Option<[u8; 16]>)>,
}

/// 桶内对象条目(双形态,ADR-11 D1):(key, Option<vk>, meta);
/// vk = None 为未版本化单键,Some 为版本条目(含 VK_NULL 槽与删除标记)。
pub type ObjectEntry = (String, Option<[u8; 16]>, ObjectMeta);

/// 全量对象快照条目(双形态):(bucket, key, Option<vk>, meta)。
pub type ObjectSnapshotEntry = (String, String, Option<[u8; 16]>, ObjectMeta);

/// 全量对象快照条目(字节层形态;M10 V5-3 值格式重写工具专用):
/// 在双形态解析之上额外携带**原始键**(重写按原键回写,避免键重编码)
/// 与**值版本字节**(双读归一后判别 v2/v3 的唯一途径)。常规扫描
/// (恢复可达性/压缩发现/导出)用 snapshot_all_objects,不暴露字节层。
#[derive(Debug, Clone)]
pub struct RawObjectEntry {
    /// o: 原始键(未版本化单键或版本键,原样)。
    pub raw_key: Vec<u8>,
    pub bucket: String,
    pub key: String,
    /// None = 未版本化单键;Some = 版本条目(含 VK_NULL 槽与删除标记)。
    pub vk: Option<[u8; 16]>,
    /// 值首字节(ADR-9 §13 版本字节;现存合法值 = 2/3/4)。
    pub value_version: u8,
    pub meta: ObjectMeta,
}

/// 版本前缀反扫「槽尖」(D1a 候选):(null 槽条目, 最大真实 vk 条目)。
type VersionTip = (Option<ObjectMeta>, Option<([u8; 16], ObjectMeta)>);

/// D1a 当前版本裁决(ADR-11 D1a-2;get_current_version / 版本化列表共用):
/// null 族(遗留单键/null 槽,不共存,防御性并存取 mtime 大者)与最大真实
/// vk 条目之间,null 族胜出 ⟺ 其 mtime **严格大于**最大真实 vk 的时间戳
/// 分量(微秒换算到秒);打平取真实版本。写侧保序(V6-1):null 族写入
/// 保证其 mtime > 既有最大真实 vk 秒分量(同秒写 +1s),新真实 vk 防回拨
/// 基址含 null 族 mtime(打平 ⇒ 真实版本确为后写)——双向同秒序均正确。
/// null 族胜出时返回 vk = VK_NULL(对外 VersionId "null")。
fn d1a_pick_current(
    legacy: Option<ObjectMeta>,
    null_slot: Option<ObjectMeta>,
    max_real: Option<([u8; 16], ObjectMeta)>,
) -> Option<([u8; 16], ObjectMeta)> {
    let null_family = match (legacy, null_slot) {
        (Some(l), Some(n)) => Some(if l.mtime >= n.mtime { l } else { n }),
        (Some(l), None) => Some(l),
        (None, n) => n,
    };
    match (null_family, max_real) {
        (Some(n), Some((rvk, r))) => {
            let real_secs = (fs3_core::vk_time_us(&rvk) / 1_000_000) as i64;
            if n.mtime > real_secs {
                Some((VK_NULL, n))
            } else {
                Some((rvk, r))
            }
        }
        (Some(n), None) => Some((VK_NULL, n)),
        (None, r) => r,
    }
}

impl MetaStore {
    pub fn open(dir: &Path, cfg: &MetaConfig) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // 元数据值已由 postcard 编码且含大量内联小对象,压缩收益有限;
        // 保持确定性,不依赖可选压缩库(依赖最小化,ADR-8)。
        opts.set_compression_type(DBCompressionType::None);
        if let Some(cap) = cfg.cache_capacity {
            let cache = Cache::new_lru_cache(cap as usize);
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_cache(&cache);
            opts.set_block_based_table_factory(&block_opts);
        }
        let mut write_opts = WriteOptions::new();
        let mut txn_opts = OptimisticTransactionOptions::new();
        // 快照读:事务内读集参与提交冲突检测,等价 sled 事务冲突集。
        txn_opts.set_snapshot(true);
        match cfg.sync_mode {
            SyncMode::Group => {
                // WAL 写入缓冲在内存,由后台线程按窗口批量落盘(ADR-8)
                opts.set_manual_wal_flush(true);
            }
            SyncMode::None => {
                // 纯内存语义:跳过 WAL,数据仅存 memtable(崩溃即丢,HA 层兜底)
                write_opts.disable_wal(true);
            }
            SyncMode::Full => {}
        }
        let db = Arc::new(OptimisticTransactionDB::open(&opts, dir).map_err(rocks_err)?);
        let flusher = if cfg.sync_mode == SyncMode::Group && cfg.flush_every_ms > 0 {
            Some(Flusher::spawn(db.clone(), cfg.flush_every_ms)?)
        } else {
            None
        };
        Ok(MetaStore {
            db,
            sync_mode: cfg.sync_mode,
            write_opts,
            txn_opts,
            flusher,
            lifecycle_cache: Mutex::new(HashMap::new()),
        })
    }

    /// 显式落盘:WAL write + fsync(组提交窗口外的确定性刷盘)。
    pub fn flush(&self) -> Result<()> {
        self.db.flush_wal(true).map_err(rocks_err)
    }

    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    /// 元数据层运行统计(H2 指标)。
    pub fn stats(&self) -> MetaStats {
        match &self.flusher {
            Some(f) => f.stats(),
            None => MetaStats::default(),
        }
    }

    // —— 读路径 ——

    pub fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        match self.db.get(bucket_key(name)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_bucket(&v)?)),
            None => Ok(None),
        }
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        match self.db.get(object_key(bucket, key)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    // —— 版本化读路径(ADR-11 D1/D4;V2) ——

    /// 桶版本化状态(桶不存在 → Off;list 过滤分叉用)。
    fn bucket_versioning(&self, bucket: &str) -> Result<fs3_core::VersioningState> {
        Ok(self
            .get_bucket(bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default())
    }

    /// 指定版本精确读(DELETE/GET ?versionId 语义):版本键点读。
    pub fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
    ) -> Result<Option<ObjectMeta>> {
        match self
            .db
            .get(object_version_key(bucket, key, vk))
            .map_err(rocks_err)?
        {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    /// 版本前缀反扫取「槽尖」:一次迭代同时取出 null 槽条目与最大真实 vk
    /// 条目(D1a 候选解析用)。
    ///
    /// 反扫技巧:版本键全部落在 `[prefix, upper)` 区间(upper = prefix 末字
    /// 节 0x00→0x01;object_key_prefix 恒以 0x00 收尾,进位安全),从 upper
    /// 反向迭代;upper 本身可能是真实对象键(`o:{b}\0k\x01`)需跳过,首个
    /// 命中 prefix 的条目即键序最大。null 槽(VK_NULL)恒为键序最大:首条
    /// 命中若为 null 槽则记录后继续向下,再取首条真实 vk = 最大真实 vk;
    /// 首条命中即真实 vk 时 null 槽必不存在(它若存在必排最前)。
    fn version_scan_tip(&self, bucket: &str, key: &str) -> Result<VersionTip> {
        let prefix = object_key_prefix(bucket, key);
        let mut upper = prefix.clone();
        *upper.last_mut().unwrap() += 1;
        let mut null_slot = None;
        let mut max_real = None;
        for item in self
            .db
            .iterator(IteratorMode::From(upper.as_slice(), Direction::Reverse))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                if k.as_ref() < prefix.as_slice() {
                    break;
                }
                // k == upper(真实对象键):跳过继续向下
                continue;
            }
            let (_, _, vk) = parse_object_version_key(&k)?;
            let vk = vk.ok_or_else(|| Error::Corrupt("version entry missing vk".into()))?;
            if vk == VK_NULL {
                null_slot = Some(decode_object(&v)?);
                continue;
            }
            max_real = Some((vk, decode_object(&v)?));
            break;
        }
        Ok((null_slot, max_real))
    }

    /// 本 key 最大真实 vk(vk 防回拨比较基址,ADR-11 D1a-5:null 槽时间戳
    /// 分量恒 u64::MAX、遗留单键无 vk,均不纳入)。
    pub fn max_real_vk(&self, bucket: &str, key: &str) -> Result<Option<[u8; 16]>> {
        Ok(self.version_scan_tip(bucket, key)?.1.map(|(vk, _)| vk))
    }

    /// 本 key 的版本扫描哨兵(V6-1 D1a 写侧保序用):(null 槽条目, 最大真实
    /// vk 条目)——一次反扫同取;遗留单键不在内(get_object 单读)。
    pub fn version_tip(&self, bucket: &str, key: &str) -> Result<VersionTip> {
        self.version_scan_tip(bucket, key)
    }

    /// 当前版本解析(ADR-11 D4 + D1a 跨状态转换补遗):候选集 = {遗留
    /// 未版本化单键 / null 槽条目, 最大真实 vk 条目},取 **mtime 最大**者,
    /// mtime 相等取真实版本(重启用后的写必然后于挂起期写)。
    ///
    /// - 遗留单键与 null 槽不共存(引擎 Suspended 写路径保证;防御性并存时
    ///   取 mtime 大者);
    /// - 返回条目的 vk:null 族(遗留单键/null 槽)= VK_NULL(对外 VersionId
    ///   恒 "null",?versionId=null 寻址二者之一);
    /// - **删除标记亦为当前版本**,由调用方按 `is_delete_marker` 判定
    ///   404/列表隐藏。
    pub fn get_current_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<([u8; 16], ObjectMeta)>> {
        let legacy = self.get_object(bucket, key)?;
        let (null_slot, max_real) = self.version_scan_tip(bucket, key)?;
        Ok(d1a_pick_current(legacy, null_slot, max_real))
    }

    /// get_current_version 的桶状态感知形态(F-1 Off 快速路径):
    /// `versioning == Off` 时桶内**绝不可能存在版本键**(状态机:
    /// Enabled/Suspended → Off 被禁,版本键仅在 Enabled/Suspended 期间
    /// 写入;删桶全量清理),候选集退化为遗留单键——直接点读返回
    /// (vk 恒 VK_NULL,与 d1a_pick_current 对仅存单键的裁决一致),
    /// 跳过版本前缀反扫 seek,语义精确等价;Enabled/Suspended 保持
    /// 全量 D1a 不变。调用方须持有桶版本化状态(桶不存在按 Off 处理,
    /// 与 bucket_versioning 缺省口径一致)。
    pub fn get_current_version_for(
        &self,
        bucket: &str,
        key: &str,
        versioning: fs3_core::VersioningState,
    ) -> Result<Option<([u8; 16], ObjectMeta)>> {
        if versioning == fs3_core::VersioningState::Off {
            return Ok(self.get_object(bucket, key)?.map(|m| (VK_NULL, m)));
        }
        self.get_current_version(bucket, key)
    }

    /// 该 key 全版本列举(ListObjectVersions 用):vk 升序(= 创建时间序);
    /// null 槽条目 vk = VK_NULL(对外 VersionId = "null",协议层渲染)。
    pub fn list_key_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<([u8; 16], ObjectMeta)>> {
        let prefix = object_key_prefix(bucket, key);
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, &prefix) {
            let (k, v) = item?;
            let (_, _, vk) = parse_object_version_key(&k)?;
            let vk = vk.ok_or_else(|| Error::Corrupt("version entry missing vk".into()))?;
            out.push((vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 枚举桶全部对象条目(双形态;含历史版本与删除标记;delete_bucket
    /// 全量释放与运维扫描用)。返回 (key, Option<vk>, meta),键序升序。
    pub fn list_object_entries(&self, bucket: &str) -> Result<Vec<ObjectEntry>> {
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        for item in scan_prefix(&self.db, &start) {
            let (k, v) = item?;
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            out.push((key, vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 桶对象条目分页扫描(M11 L2-2 生命周期执行器底座;ADR-12 DL3 全量
    /// 扫描的分块形态,避免 list_object_entries 整桶物化):双形态条目键序
    /// 升序(同 key 组内:遗留单键 → 真实 vk 升序 → null 槽)。
    ///
    /// `cursor` = 上一页最后返回的条目(严格大于续扫;None = 桶首)。
    /// 游标条目被并发删除时从其后最近条目续扫,不重复不遗漏。返回
    /// (entries, done):done = 桶内已无更多条目。`limit` 下限钳为 1。
    pub fn scan_object_entries_page(
        &self,
        bucket: &str,
        cursor: Option<&(String, Option<[u8; 16]>)>,
        limit: usize,
    ) -> Result<(Vec<ObjectEntry>, bool)> {
        let limit = limit.max(1);
        let base = object_prefix(bucket);
        let start = match cursor {
            Some((key, Some(vk))) => object_version_key(bucket, key, vk),
            Some((key, None)) => object_key(bucket, key),
            None => base.clone(),
        };
        let mut out = Vec::new();
        let mut done = true;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&base) {
                break;
            }
            // 游标语义 = 严格大于:首条命中若即游标条目本身则跳过
            if cursor.is_some() && out.is_empty() && k.as_ref() == start.as_slice() {
                continue;
            }
            if out.len() >= limit {
                done = false;
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            out.push((key, vk, decode_object(&v)?));
        }
        Ok((out, done))
    }

    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_BUCKET) {
            let (k, v) = item?;
            let name = String::from_utf8(k.strip_prefix(PREFIX_BUCKET).unwrap_or(&k).to_vec())
                .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
            out.push((name, decode_bucket(&v)?));
        }
        Ok(out)
    }

    /// 前缀扫描某桶全部对象(`o:{bucket}\0` 前缀)。
    ///
    /// 版本化桶(ADR-11 §3.4.4 + D1a):每 key 只输出当前版本——候选 {遗留
    /// 单键/null 槽, 最大真实 vk} 取 mtime 最大(相等取真实版本);当前为
    /// 删除标记的 key 不出现。未版本化桶逐字节同旧路径。
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        if self.bucket_versioning(bucket)? == fs3_core::VersioningState::Off {
            return self.list_objects_plain(bucket, prefix);
        }
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        // 同 key 条目连续(未版本化条目在前,版本条目 vk 升序、null 槽收尾):
        // 组内按 D1a 裁决当前版本(组尾键序最大 ≠ 当前,mtime 才是序)
        let mut cur_key: Option<String> = None;
        let mut cur_legacy: Option<ObjectMeta> = None;
        let mut cur_null: Option<ObjectMeta> = None;
        let mut cur_real: Option<([u8; 16], ObjectMeta)> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&start) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                if let Some(ck) = cur_key.take() {
                    if let Some((_, cm)) =
                        d1a_pick_current(cur_legacy.take(), cur_null.take(), cur_real.take())
                    {
                        if !cm.is_delete_marker && (prefix.is_empty() || ck.starts_with(prefix)) {
                            out.push((ck, cm));
                        }
                    }
                }
                cur_key = Some(key);
            }
            match vk {
                None => cur_legacy = Some(decode_object(&v)?),
                Some(VK_NULL) => cur_null = Some(decode_object(&v)?),
                Some(vk) => cur_real = Some((vk, decode_object(&v)?)),
            }
        }
        if let Some(ck) = cur_key {
            if let Some((_, cm)) = d1a_pick_current(cur_legacy, cur_null, cur_real) {
                if !cm.is_delete_marker && (prefix.is_empty() || ck.starts_with(prefix)) {
                    out.push((ck, cm));
                }
            }
        }
        Ok(out)
    }

    /// 未版本化桶前缀扫描(list_objects 的 Off 分支,旧路径原样保留)。
    fn list_objects_plain(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&start) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if prefix.is_empty() || key.starts_with(prefix) {
                out.push((key, decode_object(&v)?));
            }
        }
        Ok(out)
    }

    /// 分页列举:前缀 + 可选 delimiter 分组 + after 游标 + max 条目数。
    ///
    /// 条目 = 对象 + 公共前缀,均计入 max;截断时 last_scanned 为最后
    /// **已发出**的条目(Contents 键或公共前缀串;严格大于它即可续扫
    /// 不重不漏,与 AWS NextMarker/NextContinuationToken 语义一致)。
    ///
    /// 版本化桶(ADR-11 §3.4.4):每 key 只输出当前版本(当前 = 删除标记
    /// 则该 key 不出现);游标/分组语义不变。未版本化桶走 Off 分支,逐字节
    /// 同旧路径。
    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        if self.bucket_versioning(bucket)? == fs3_core::VersioningState::Off {
            self.list_objects_page_plain(bucket, prefix, delimiter, after, max)
        } else {
            self.list_objects_page_versioned(bucket, prefix, delimiter, after, max)
        }
    }

    /// 未版本化桶分页列举(list_objects_page 的 Off 分支,旧路径原样保留)。
    fn list_objects_page_plain(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        let base = object_prefix(bucket);
        let after_esc = after.map(|a| escape(a.as_bytes()));
        let start: Vec<u8> = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k
            }
            None => base,
        };
        // 注意:游标过滤在条目空间进行(见下),range 起点仅作扫描优化;
        // 裸键与完整键的字节比较不一致会导致游标失效,故不再直接比较 k。
        let mut entries = 0usize;
        // 本页最后"已发出"的条目(Contents 键或公共前缀串)。注意必须在
        // max 检查之后才记录:截断时 last_scanned 若记录到首个未发键,
        // 续页会跳过一条(s3-tests: test_bucket_listv2_continuationtoken)。
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if !prefix.is_empty() && !key.starts_with(prefix) {
                continue;
            }
            // 条目化:键 → 输出条目。带 delimiter 时,键在 prefix 之后首个
            // delimiter 之前的段归组为公共前缀条目;键自身等于公共前缀
            // (如 "0/")时也按 **公共前缀** 输出——AWS/RGW 不把以 delimiter
            // 结尾的键单独列为 Contents(s3-tests
            // test_bucket_list_delimiter_not_skip_special)。
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.clone(), false),
                    }
                }
                _ => (key.clone(), false),
            };
            // 条目级严格大于游标:游标为公共前缀(如 "boo/")时,该组全部
            // 键(boo/bar、boo/baz/…)的条目 ≤ 游标,整组跳过 —— 与
            // AWS NextMarker 语义一致(s3-tests: test_bucket_list_delimiter_prefix)。
            if let Some(m) = after {
                if entry.as_str() <= m {
                    continue;
                }
            }
            // 分组去重:同一条目只发一次
            if last_entry.as_deref() == Some(entry.as_str()) {
                continue;
            }
            if entries >= max {
                page.truncated = true;
                break;
            }
            if is_prefix_entry || entry != key {
                page.common_prefixes.push(entry.clone());
            } else {
                page.items.push((key, decode_object(&v)?));
            }
            last_entry = Some(entry.clone());
            last_emitted = Some(entry);
            entries += 1;
        }
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// 版本化桶分页列举(ADR-11 §3.4.4 + D1a):同 key 条目连续(未版本化
    /// 条目在前,版本条目 vk 升序、null 槽收尾),组内按 D1a 裁决当前版本
    /// (mtime 最大,相等取真实版本);当前为删除标记的 key 不出现。
    /// 游标/delimiter 分组/截断语义与 Off 分支一致(条目空间严格大于游标,
    /// 截断游标 = 最后已发出条目)。
    fn list_objects_page_versioned(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        let base = object_prefix(bucket);
        let after_esc = after.map(|a| escape(a.as_bytes()));
        let start: Vec<u8> = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k
            }
            None => base,
        };
        let mut entries = 0usize;
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        // 发出一个 key 的当前版本(D1a 裁决);返回 false = 已截断,停止扫描
        let mut emit = |key: &str, meta: &ObjectMeta, page: &mut ListPage| -> bool {
            // 当前 = 删除标记 → 该 key 不出现(§3.4.4)
            if meta.is_delete_marker {
                return true;
            }
            if !prefix.is_empty() && !key.starts_with(prefix) {
                return true;
            }
            // 条目化(与 Off 分支同一规则):delimiter 归组为公共前缀条目
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.to_string(), false),
                    }
                }
                _ => (key.to_string(), false),
            };
            if let Some(m) = after {
                if entry.as_str() <= m {
                    return true;
                }
            }
            if last_entry.as_deref() == Some(entry.as_str()) {
                return true;
            }
            if entries >= max {
                return false;
            }
            if is_prefix_entry || entry != key {
                page.common_prefixes.push(entry.clone());
            } else {
                page.items.push((key.to_string(), meta.clone()));
            }
            last_entry = Some(entry.clone());
            last_emitted = Some(entry);
            entries += 1;
            true
        };
        let mut truncated = false;
        let mut cur_key: Option<String> = None;
        let mut cur_legacy: Option<ObjectMeta> = None;
        let mut cur_null: Option<ObjectMeta> = None;
        let mut cur_real: Option<([u8; 16], ObjectMeta)> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                // 组边界:D1a 裁决上一组的当前版本并发出
                if let Some(ck) = cur_key.take() {
                    if let Some((_, cm)) =
                        d1a_pick_current(cur_legacy.take(), cur_null.take(), cur_real.take())
                    {
                        if !emit(&ck, &cm, &mut page) {
                            truncated = true;
                            break;
                        }
                    }
                }
                cur_key = Some(key);
            }
            match vk {
                None => cur_legacy = Some(decode_object(&v)?),
                Some(VK_NULL) => cur_null = Some(decode_object(&v)?),
                Some(vk) => cur_real = Some((vk, decode_object(&v)?)),
            }
        }
        if !truncated {
            if let Some(ck) = cur_key {
                if let Some((_, cm)) = d1a_pick_current(cur_legacy, cur_null, cur_real) {
                    if !emit(&ck, &cm, &mut page) {
                        truncated = true;
                    }
                }
            }
        }
        page.truncated = truncated;
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// ListObjectVersions 分页列举(ADR-11 §3.4.4 + D1a-3;V3):
    /// 键字典序升序;**键内条目按 mtime 降序**(null 条目按 mtime 插入真实
    /// 版本序列,不按键位;mtime 相等时真实版本在前、真实版本之间 vk 降序
    /// ——vk 单调,即创建序的逆序,与 AWS「最新在前」一致)。
    ///
    /// - 游标 = (key_marker, version_id_marker) 条目级严格大于(ADR-6);
    ///   key == key_marker 组内从 version_id_marker 之后续传(version_id_
    ///   marker 缺席 → 整组跳过,与未版本化桩语义一致;标记版本已被并发
    ///   删除而定位不到 → 跳过该组,分页继续);
    /// - delimiter:版本条目按 key 的公共前缀归组为 CommonPrefixes(键本身
    ///   等于公共前缀时同样归组,与 list_objects_page 同规则);
    /// - IsLatest = D1a 当前版本(mtime 裁决,与列表位置独立);
    /// - 未版本化桶天然兼容:每组仅遗留单键一条,VersionId="null"、
    ///   IsLatest=true(现状桩语义,s3-tests 清理依赖)。
    pub fn list_versions_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&[u8; 16]>,
        max: usize,
    ) -> Result<VersionListPage> {
        let mut page = VersionListPage::default();
        let base = object_prefix(bucket);
        let start: Vec<u8> = match key_marker {
            Some(m) => {
                let mut k = base.clone();
                k.extend_from_slice(&escape(m.as_bytes()));
                k
            }
            None => base.clone(),
        };
        let mut emitted = 0usize;
        let mut last_emitted: Option<(String, Option<[u8; 16]>)> = None;
        // 组缓冲:(展示 vk, meta);遗留单键与 null 槽的展示 vk 均 VK_NULL
        let mut cur_key: Option<String> = None;
        let mut group: Vec<([u8; 16], ObjectMeta)> = Vec::new();
        // 发出一个 key 组;返回 false = 已截断
        let mut flush = |key: &str,
                         group: &mut Vec<([u8; 16], ObjectMeta)>,
                         page: &mut VersionListPage|
         -> Result<bool> {
            if !prefix.is_empty() && !key.starts_with(prefix) {
                group.clear();
                return Ok(true);
            }
            // 条目化(delimiter 归组;与 list_objects_page 同一规则)
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.to_string(), false),
                    }
                }
                _ => (key.to_string(), false),
            };
            // 游标:条目字符串严格小于 key_marker → 整组跳过;等于时公共
            // 前缀组(已发出)或无 version_id_marker 的键组同样整组跳过
            if let Some(m) = key_marker {
                if entry.as_str() < m
                    || (entry.as_str() == m && (is_prefix_entry || version_id_marker.is_none()))
                {
                    group.clear();
                    return Ok(true);
                }
            }
            // 键内排序:mtime 降序;相等时真实版本在前(D1a 同值取真实版本),
            // 真实版本之间 vk 降序(vk 单调 ⇒ 创建逆序)
            group.sort_by(|(avk, a), (bvk, b)| {
                b.mtime
                    .cmp(&a.mtime)
                    .then_with(|| (*avk == VK_NULL).cmp(&(*bvk == VK_NULL)))
                    .then_with(|| bvk.cmp(avk))
            });
            // D1a 当前版本判定(IsLatest;与排序位置独立)
            let latest_vk = {
                let null_family = group
                    .iter()
                    .filter(|(vk, _)| *vk == VK_NULL)
                    .map(|(_, m)| m.clone())
                    .max_by_key(|m| m.mtime);
                let max_real = group
                    .iter()
                    .filter(|(vk, _)| *vk != VK_NULL)
                    .map(|(vk, m)| (*vk, m.clone()))
                    .max_by_key(|(vk, _)| *vk);
                d1a_pick_current(None, null_family, max_real).map(|(vk, _)| vk)
            };
            if is_prefix_entry || entry != key {
                // 分组去重:相邻键组折叠出的同一公共前缀只发一次(与
                // list_objects_page 的 last_entry 去重同语义)
                if page.common_prefixes.last() == Some(&entry) {
                    group.clear();
                    return Ok(true);
                }
                if emitted >= max {
                    group.clear();
                    return Ok(false);
                }
                page.common_prefixes.push(entry.clone());
                last_emitted = Some((entry, None));
                emitted += 1;
                group.clear();
                return Ok(true);
            }
            // 版本条目逐条发出;key == key_marker 时从 version_id_marker 后续传
            let mut skip_until_marker = match (key_marker, version_id_marker) {
                (Some(m), Some(vm)) if m == key => Some(*vm),
                _ => None,
            };
            for (vk, meta) in group.drain(..) {
                if let Some(vm) = skip_until_marker {
                    if vk == vm {
                        skip_until_marker = None;
                    }
                    continue;
                }
                if emitted >= max {
                    return Ok(false);
                }
                page.entries.push(VersionListEntry {
                    key: key.to_string(),
                    vk,
                    is_latest: Some(vk) == latest_vk,
                    meta,
                });
                last_emitted = Some((key.to_string(), Some(vk)));
                emitted += 1;
            }
            Ok(true)
        };
        let mut truncated = false;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&base) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                if let Some(ck) = cur_key.take() {
                    if !flush(&ck, &mut group, &mut page)? {
                        truncated = true;
                        break;
                    }
                }
                cur_key = Some(key);
            }
            // 遗留单键(None)与 null 槽(VK_NULL)对外展示 vk 均为 VK_NULL
            group.push((vk.unwrap_or(VK_NULL), decode_object(&v)?));
        }
        if !truncated {
            if let Some(ck) = cur_key {
                if !flush(&ck, &mut group, &mut page)? {
                    truncated = true;
                }
            }
        }
        page.truncated = truncated;
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// 列出 seq > after 的全部分配记录(恢复重放)。
    pub fn list_alloc_records(&self, after: u64) -> Result<Vec<AllocRecord>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_ALLOC) {
            let (k, v) = item?;
            let seq = parse_alloc_seq(&k)?;
            if seq > after {
                out.push(decode_alloc(&v)?);
            }
        }
        Ok(out)
    }
    /// 最新事务序号(s:seq)。
    pub fn last_seq(&self) -> Result<u64> {
        Ok(self
            .db
            .get(SYS_SEQ)
            .map_err(rocks_err)?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
            .unwrap_or(0))
    }

    // —— 写路径(全部走 rocksdb 乐观事务) ——

    /// 应用一组 Op(单个乐观事务,原子;提交冲突自动重试)。
    ///
    /// 返回本次事务序号(新 s:seq 值)。
    pub fn commit(&self, ops: &[Op]) -> Result<u64> {
        // 冲突重试上限:引擎写路径已由全局锁串行,此处主要覆盖测试/多引擎
        // 并发;上限仅为防御性,正常路径远达不到。
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            let seq = match apply_ops(&tx, ops) {
                Ok(seq) => seq,
                Err(e) => {
                    tx.rollback().map_err(rocks_err)?;
                    return Err(e);
                }
            };
            match tx.commit() {
                Ok(()) => {
                    if self.sync_mode == SyncMode::Full {
                        // Full:每事务显式 fsync
                        self.db.flush_wal(true).map_err(rocks_err)?;
                    }
                    for op in ops {
                        match op {
                            Op::LifecycleRulesReplace { bucket, .. }
                            | Op::LifecycleRulesDelete { bucket } => {
                                self.lifecycle_cache.lock().unwrap().remove(bucket);
                            }
                            _ => {}
                        }
                    }
                    return Ok(seq);
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    /// 桶 PUT(创建/更新;location 不写,保持旧语义)。
    pub fn commit_bucket_put(&self, name: &str, meta: &BucketMeta) -> Result<u64> {
        self.commit(&[Op::BucketPut {
            name: name.to_string(),
            meta: meta.clone(),
            location: None,
        }])
    }

    /// 桶 PUT 并记录 LocationConstraint(同事务,M8 回显语义)。
    pub fn commit_bucket_put_with_location(
        &self,
        name: &str,
        meta: &BucketMeta,
        location: &str,
    ) -> Result<u64> {
        self.commit(&[Op::BucketPut {
            name: name.to_string(),
            meta: meta.clone(),
            location: Some(location.to_string()),
        }])
    }

    /// 读桶 LocationConstraint(未设置/旧桶 → "")。
    pub fn bucket_location(&self, name: &str) -> Result<String> {
        let v = self.db.get(bucket_location_key(name)).map_err(rocks_err)?;
        Ok(v.map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default())
    }

    // —— 密钥加密种子盐(M3 密钥 CRUD;首次启动生成,持久化) ——

    /// 读种子盐(不存在 → 生成 64 字节随机并持久化,返回)。
    pub fn seed_salt(&self) -> Result<Vec<u8>> {
        if let Some(v) = self.db.get(SYS_KEY_SEED_SALT).map_err(rocks_err)? {
            return Ok(v);
        }
        let mut salt = [0u8; 64];
        fs3_core::random_bytes(&mut salt)?;
        // 直接写(不走事务;无并发竞争——引擎单例 + 启动时调用)
        self.db.put(SYS_KEY_SEED_SALT, salt).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)?;
        Ok(salt.to_vec())
    }

    // —— SSE-S3 KEK 体系(M11 K1-1,ADR-12 DS1) ——

    /// 读 KEK 种子(不存在 → 生成 64 字节随机并持久化,返回;幂等,
    /// 与 key_seed_salt 相互独立)。**红线:seed 及其派生 KEK/DEK 明文
    /// 零导出、零日志、永不下发**;本访问器是唯一出口,返回值不出引擎域。
    pub fn sse_kek_seed(&self) -> Result<[u8; 64]> {
        if let Some(v) = self.db.get(SYS_SSE_KEK_SEED).map_err(rocks_err)? {
            return v.as_slice().try_into().map_err(|_| {
                Error::Corrupt(format!("sse kek seed must be 64 bytes, got {}", v.len()))
            });
        }
        let mut seed = [0u8; 64];
        fs3_core::random_bytes(&mut seed)?;
        // 直写 + fsync(照 seed_salt 先例;调用点均持引擎写锁/单点,幂等)
        self.db.put(SYS_SSE_KEK_SEED, seed).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)?;
        Ok(seed)
    }

    /// 当前 KEK 代状态(键缺席 = 初始代 1,惰性不落盘;rewrap_done_gen
    /// 缺席时 = gen,即「无需重包裹」)。
    pub fn sse_kek_gen_state(&self) -> Result<SseKekGenState> {
        match self.db.get(SYS_SSE_KEK_GEN).map_err(rocks_err)? {
            Some(v) => decode(&v),
            None => Ok(SseKekGenState {
                gen: 1,
                last_rotated_at: 0,
                rewrap_done_gen: 1,
            }),
        }
    }

    /// 写 KEK 代状态(直写 + fsync,同 seed_salt 先例;调用方持引擎写锁)。
    fn put_sse_kek_gen_state(&self, st: &SseKekGenState) -> Result<()> {
        self.db
            .put(SYS_SSE_KEK_GEN, encode(st)?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// KEK 轮换(DS1):gen+1 落盘(last_rotated_at = 当前秒;
    /// rewrap_done_gen 不动 —— 与 gen 拉开差距即「重包裹待办」标记,
    /// 后台重包裹完成后由 mark_sse_rewrap_done 收敛)。返回新状态。
    pub fn rotate_sse_kek(&self) -> Result<SseKekGenState> {
        let cur = self.sse_kek_gen_state()?;
        let next = SseKekGenState {
            gen: cur
                .gen
                .checked_add(1)
                .ok_or_else(|| Error::InvalidArgument("sse kek generation overflow".into()))?,
            last_rotated_at: now_ts(),
            rewrap_done_gen: cur.rewrap_done_gen,
        };
        self.put_sse_kek_gen_state(&next)?;
        Ok(next)
    }

    /// 重包裹完成收敛(rewrap_done_gen = gen;幂等)。
    pub fn mark_sse_rewrap_done(&self, gen: u32) -> Result<()> {
        let cur = self.sse_kek_gen_state()?;
        self.put_sse_kek_gen_state(&SseKekGenState {
            rewrap_done_gen: gen,
            ..cur
        })
    }

    // —— 可信时钟(M12 W1-1,ADR-13 DL6) ——

    /// 读持久化可信时钟(键缺席 → None)。
    pub fn load_trusted_clock(&self) -> Result<Option<fs3_core::TrustedClockState>> {
        match self.db.get(SYS_TRUSTED_CLOCK).map_err(rocks_err)? {
            Some(v) => decode(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 写可信时钟(直写 + fsync,同 seed_salt 先例;调用方持引擎写锁/启动单点)。
    pub fn put_trusted_clock(&self, st: &fs3_core::TrustedClockState) -> Result<()> {
        self.db
            .put(SYS_TRUSTED_CLOCK, encode(st)?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 桶版本化状态更新(单事务读改写;l: location 等其余字段不动;
    /// V3 PutBucketVersioning 落地路径)。
    pub fn commit_bucket_set_versioning(
        &self,
        name: &str,
        state: fs3_core::VersioningState,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetVersioning {
            name: name.to_string(),
            state,
        }])
    }

    /// 桶默认加密更新(M11 K1-2;单事务读改写,其余字段不动;
    /// Put/DeleteBucketEncryption 落地路径)。
    pub fn commit_bucket_set_encryption(
        &self,
        name: &str,
        default: Option<fs3_core::SseAlgorithm>,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetEncryption {
            name: name.to_string(),
            default,
        }])
    }

    /// 桶 Object Lock 启用 + 默认保留(单事务;开启连带 versioning=Enabled;
    /// PutObjectLockConfiguration 落地路径)。
    pub fn commit_bucket_set_object_lock(
        &self,
        name: &str,
        default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetObjectLock {
            name: name.to_string(),
            default_retention,
        }])
    }

    // —— D9 桶级配置文档(M10 S1/S2/S7;ADR-11 D9) ——

    /// 读桶级配置文档(无配置 → None;值 = 规范化 XML 字节)。
    pub fn bucket_conf(&self, bucket: &str, conf: BucketConf) -> Result<Option<Vec<u8>>> {
        self.db.get(conf.key(bucket)).map_err(rocks_err)
    }

    /// 写桶级配置文档(覆盖语义;桶不存在 → NotFound)。
    pub fn commit_bucket_conf_put(
        &self,
        bucket: &str,
        conf: BucketConf,
        value: &[u8],
    ) -> Result<u64> {
        self.commit(&[Op::BucketConfPut {
            bucket: bucket.to_string(),
            conf,
            value: value.to_vec(),
        }])
    }

    /// 删桶级配置文档(幂等;桶不存在 → NotFound)。
    pub fn commit_bucket_conf_delete(&self, bucket: &str, conf: BucketConf) -> Result<u64> {
        self.commit(&[Op::BucketConfDelete {
            bucket: bucket.to_string(),
            conf,
        }])
    }

    // —— 生命周期规则(M11 L1;ADR-12 DL1 `r:` 键) ——

    /// 读桶生命周期规则集(按 `r:{bucket}\0` 前缀扫描;规则序 = rule_id
    /// 字典序——每规则一键的存储形态不保留提交序,执行器 L2-2 对顺序
    /// 不敏感;无规则/桶不存在 → 空表,桶存在性判定归协议层)。
    pub fn get_lifecycle_rules(&self, bucket: &str) -> Result<Vec<fs3_core::LifecycleRule>> {
        if let Some(hit) = self.lifecycle_cache.lock().unwrap().get(bucket) {
            return Ok(hit.clone());
        }
        let mut rules = Vec::new();
        for item in scan_prefix(&self.db, &lifecycle_rules_prefix(bucket)) {
            let (_k, v) = item?;
            rules.push(decode_lifecycle_rule(&v)?);
        }
        self.lifecycle_cache
            .lock()
            .unwrap()
            .insert(bucket.to_string(), rules.clone());
        Ok(rules)
    }

    #[cfg(test)]
    pub(crate) fn debug_clear_lifecycle_cache(&self) {
        self.lifecycle_cache.lock().unwrap().clear();
    }

    /// 生命周期规则整体替换(单事务读旧写新;桶不存在 → NotFound)。
    pub fn put_lifecycle_rules(
        &self,
        bucket: &str,
        rules: &[fs3_core::LifecycleRule],
    ) -> Result<u64> {
        self.commit(&[Op::LifecycleRulesReplace {
            bucket: bucket.to_string(),
            rules: rules.to_vec(),
        }])
    }

    /// 生命周期规则整桶清除(幂等:无规则同样 Ok;桶不存在 → NotFound)。
    pub fn delete_lifecycle_rules(&self, bucket: &str) -> Result<u64> {
        self.commit(&[Op::LifecycleRulesDelete {
            bucket: bucket.to_string(),
        }])
    }

    /// 对象标签单事务读改写(M10 S1):`vk = None` → 未版本化单键;
    /// `Some(vk)` → 版本键(含 VK_NULL null 槽)。目标不存在 → NotFound。
    pub fn commit_object_set_tags(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<[u8; 16]>,
        tags: Vec<(String, String)>,
    ) -> Result<u64> {
        self.commit(&[Op::ObjectSetTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
            tags,
        }])
    }

    /// 桶删除。
    pub fn commit_bucket_delete(&self, name: &str) -> Result<u64> {
        self.commit(&[Op::BucketDelete {
            name: name.to_string(),
        }])
    }

    // —— 恢复/导入(M7 E5 meta-import;仅离线工具使用) ——

    /// 将事务序号计数器设为 `base`(meta-import 用:导入的元数据从事务序号
    /// 继续,保证 `seq > 检查点序号` 的所有导入记录在引擎打开时全部重放,
    /// 位图恢复与崩溃重放语义完全一致)。
    pub fn reset_seq(&self, base: u64) -> Result<()> {
        self.db
            .put(SYS_SEQ, base.to_be_bytes())
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 覆写密钥种子盐(meta-import 用:密钥密文依赖种子盐派生,AES-GCM
    /// 密钥 = SHA-256(seed_salt);恢复时必须与导出时一致)。
    pub fn set_seed_salt(&self, salt: &[u8]) -> Result<()> {
        self.db.put(SYS_KEY_SEED_SALT, salt).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    // —— 访问密钥(M3 密钥 CRUD;secret 哈希存储) ——

    /// 写/更新访问密钥。
    pub fn commit_key_put(&self, record: &fs3_core::KeyRecord) -> Result<u64> {
        self.commit(&[Op::KeyPut {
            access_key: record.access_key.clone(),
            record: record.clone(),
        }])
    }

    /// 删除访问密钥(不存在 → NotFound)。
    pub fn commit_key_delete(&self, access_key: &str) -> Result<u64> {
        self.commit(&[Op::KeyDelete {
            access_key: access_key.to_string(),
        }])
    }

    /// 读访问密钥。
    pub fn get_key(&self, access_key: &str) -> Result<Option<fs3_core::KeyRecord>> {
        let k = key_key(access_key);
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => {
                Ok(Some(decode(&v).map_err(|e| {
                    Error::Corrupt(format!("key {access_key}: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    /// 列全部访问密钥(按 access_key 排序)。
    pub fn list_keys(&self) -> Result<Vec<fs3_core::KeyRecord>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_KEY, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_KEY) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("key record: {e}")))?);
        }
        Ok(out)
    }

    /// 对象 PUT + 分配记录 + 桶统计(ADR-4 同事务)。
    pub fn commit_object_put(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        if meta.size > MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument(format!(
                "object size {} exceeds max {}",
                meta.size, MAX_OBJECT_SIZE
            )));
        }
        self.commit(&[
            Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 对象删除 + 分配记录 + 桶统计。
    pub fn commit_object_delete(
        &self,
        bucket: &str,
        key: &str,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 写删除标记 + 分配记录 + 桶统计(ADR-11 D5 口径:删除标记本身零
    /// delta;`vk = Some` 写版本键(Enabled 新 vk / Suspended VK_NULL 槽),
    /// `vk = None` 原地覆盖遗留未版本化单键(D1a-1);覆盖旧 null 族数据
    /// 版本时,旧版本的段释放(ref_dec)与扣减由调用方计算后经
    /// `draft`/`delta` 同事务入账;Enabled 路径不触碰数据段,draft 为空
    /// 则不写 a: 记录)。
    pub fn commit_object_delete_current(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        marker: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectDeleteCurrent {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: vk.copied(),
                marker: marker.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 版本化对象 PUT + 分配记录 + 桶统计(ADR-11 D1;Enabled 新 vk /
    /// Suspended 覆盖 null 槽;不触碰旧版本条目)。
    pub fn commit_object_put_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        if meta.size > MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument(format!(
                "object size {} exceeds max {}",
                meta.size, MAX_OBJECT_SIZE
            )));
        }
        self.commit(&[
            Op::ObjectPutVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
                meta: meta.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 值格式在线重写(M10 V5-3):按原始键单事务重编码对象值为 v3,
    /// **不改统计/分配**(无 Alloc/Stats 伴随 op;经 s:seq 单点序列化,
    /// 与全部写路径同一冲突域)。键不存在 → NotFound。
    /// 供 `fasts3d rewrite-values` 使用;raw_key 来自
    /// snapshot_all_objects_raw(原样回写,避免键重编码)。
    pub fn commit_object_meta_update(&self, raw_key: &[u8], meta: &ObjectMeta) -> Result<u64> {
        self.commit(&[Op::ObjectMetaRewrite {
            key: raw_key.to_vec(),
            meta: meta.clone(),
        }])
    }

    /// 物理删除指定版本 + 分配记录 + 桶统计(扣减由调用方按该版本
    /// size 计算;删除标记版本零 delta)。
    pub fn commit_object_delete_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectDeleteVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 压缩迁移事务(ADR-9 §6.2 阶段 3):单对象段列表更新(旧段→新段)+
    /// 分配/释放记录,同事务;**不触碰桶统计**(数据量不变)。
    ///
    /// 事务内校验旧段仍按序被引用;对象被并发覆盖/删除 → `Error::ObjectChanged`
    /// (调用方放弃该对象,下轮再来;乐观事务冲突自动重试)。
    pub fn commit_object_migrate(
        &self,
        bucket: &str,
        key: &str,
        old_segments: &[Segment],
        new_segments: &[Segment],
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectMigrate {
                bucket: bucket.to_string(),
                key: key.to_string(),
                old_segments: old_segments.to_vec(),
                new_segments: new_segments.to_vec(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 压缩迁移事务的分片变体(REVIEW §3.8:p: 前缀分片段迁移与对象同语义)。
    pub fn commit_part_migrate(
        &self,
        upload_id: &str,
        part_no: u32,
        old_segments: &[Segment],
        new_segments: &[Segment],
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::PartMigrate {
                upload_id: upload_id.to_string(),
                part_no,
                old_segments: old_segments.to_vec(),
                new_segments: new_segments.to_vec(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 全量对象快照扫描(o: 前缀;rocksdb MVCC 快照,与并发写完全隔离)。
    /// 压缩发现阶段用(ADR-9 §6.2 阶段 1)。
    ///
    /// ADR-11 D1 双形态:版本化条目(`o:{b}\0{esc}\0{vk16}`)逐条返回,
    /// 元组第三位 = Some(vk)(同 key 多条;含删除标记);未版本化条目 =
    /// None。恢复可达性扫描需要**全部**版本条目的段引用(§3.4.6)。
    pub fn snapshot_all_objects(&self) -> Result<Vec<ObjectSnapshotEntry>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            let (bucket, key, vk) = parse_object_version_key(&k)?;
            out.push((bucket, key, vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 全量对象快照(字节层形态;M10 V5-3 值格式重写用):与
    /// snapshot_all_objects 同一 MVCC 快照语义,逐条携带原始键与值版本
    /// 字节。值损坏(双读均失败)时整体报错(维护工具fail-fast,不静默跳过)。
    pub fn snapshot_all_objects_raw(&self) -> Result<Vec<RawObjectEntry>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            let (bucket, key, vk) = parse_object_version_key(&k)?;
            let value_version = *v
                .first()
                .ok_or_else(|| Error::Corrupt("object meta value too short".into()))?;
            out.push(RawObjectEntry {
                raw_key: k.to_vec(),
                bucket,
                key,
                vk,
                value_version,
                meta: decode_object(&v)?,
            });
        }
        Ok(out)
    }

    /// 值版本字节只读探测(M10 V5-3):统计 o: 前缀下 v2 与当前可读格式
    /// (v3/v4)值数量,返回 (v2, current)。只读首字节、不解码(供重写
    /// 前后断言与引擎启动警告);首字节非 2/3/4 的值(无版本字节的旧布局
    /// 值,ADR-9 已放弃前置兼容)→ Corrupt。
    pub fn count_object_value_versions(&self) -> Result<(u64, u64)> {
        let snap = self.db.snapshot();
        let (mut v2, mut cur) = (0u64, 0u64);
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            match v.first() {
                Some(&fs3_core::OBJECT_META_VERSION) => cur += 1,
                // v3 存量值双读可读(无需重写即合法;重写工具会顺手归一到
                // 当前版本),与当前版本同桶计数
                Some(&fs3_core::OBJECT_META_VERSION_V3) => cur += 1,
                Some(&2) => v2 += 1,
                other => {
                    return Err(Error::Corrupt(format!(
                        "object value version byte {other:?} unsupported"
                    )))
                }
            }
        }
        Ok((v2, cur))
    }

    /// 值格式 v2→v3 重写完成标记(DESIGN-FUTURE §2.4:重写完成前禁回滚)。
    pub fn value_rewrite_v3_done(&self) -> Result<bool> {
        Ok(self
            .db
            .get(SYS_KEY_VALUE_REWRITE_V3_DONE)
            .map_err(rocks_err)?
            .is_some())
    }

    /// 落重写完成标记(直写 + fsync,同 seed_salt 先例:工具离线/单点
    /// 执行,不经事务;幂等)。
    pub fn mark_value_rewrite_v3_done(&self) -> Result<()> {
        self.db
            .put(SYS_KEY_VALUE_REWRITE_V3_DONE, env!("CARGO_PKG_VERSION"))
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 测试/演练专用:以原始字节直写对象值(构造 v2 存量值夹具,
    /// V5-3 重写与 V6-5 升级演练用)。生产路径恒走 commit_object_put*,
    /// 勿用 —— 本入口绕过版本字节/契约校验与统计/分配记账。
    #[doc(hidden)]
    pub fn put_object_value_raw(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        value: &[u8],
    ) -> Result<()> {
        let k = match vk {
            Some(vk) => object_version_key(bucket, key, vk),
            None => object_key(bucket, key),
        };
        self.db.put(k, value).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 全量分片快照扫描(p: 前缀;MVCC 快照)。压缩发现 + 恢复可达性扫描用。
    pub fn snapshot_all_parts(&self) -> Result<Vec<(String, u32, PartMeta)>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_PART, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_PART) {
                break;
            }
            let body = k
                .strip_prefix(PREFIX_PART)
                .ok_or_else(|| Error::Corrupt("part key missing prefix".into()))?;
            let sep = body
                .iter()
                .position(|&b| b == 0x00)
                .ok_or_else(|| Error::Corrupt("part key missing separator".into()))?;
            let uid = String::from_utf8(body[..sep].to_vec())
                .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
            let no = &body[sep + 1..];
            if no.len() != 4 {
                return Err(Error::Corrupt("part key malformed".into()));
            }
            let part_no = u32::from_be_bytes(no.try_into().unwrap());
            out.push((uid, part_no, decode_part(&v)?));
        }
        Ok(out)
    }

    // ─────────────────────────── multipart ───────────────────────────

    /// 创建分片上传会话(桶必须存在)。
    pub fn create_multipart(&self, upload_id: &str, session: &MultipartSession) -> Result<u64> {
        self.commit(&[Op::MultipartCreate {
            upload_id: upload_id.to_string(),
            session: session.clone(),
        }])
    }

    pub fn get_multipart(&self, upload_id: &str) -> Result<Option<MultipartSession>> {
        match self.db.get(session_key(upload_id)).map_err(rocks_err)? {
            // M9/C3 双读(存量会话值无 resp_headers 尾部字段)
            Some(v) => Ok(Some(decode_session(&v)?)),
            None => Ok(None),
        }
    }

    /// 写分片(覆盖同号旧分片;会话不存在 → NotFound)。
    pub fn put_part(
        &self,
        upload_id: &str,
        part_no: u32,
        meta: &PartMeta,
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::PartPut {
                upload_id: upload_id.to_string(),
                part_no,
                meta: meta.clone(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 分片重传/reactivate:清 completed 标记(读改写)。
    pub fn touch_multipart(&self, upload_id: &str) -> Result<u64> {
        self.commit(&[Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
        }])
    }

    pub fn get_part(&self, upload_id: &str, part_no: u32) -> Result<Option<PartMeta>> {
        match self
            .db
            .get(part_key(upload_id, part_no))
            .map_err(rocks_err)?
        {
            // M11 C1-4 双读(存量分片值无 checksum 尾部字段)
            Some(v) => Ok(Some(decode_part(&v)?)),
            None => Ok(None),
        }
    }

    /// 按分片号升序列出全部已上传分片。
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartMeta)>> {
        let mut out = Vec::new();
        let prefix = part_prefix(upload_id);
        for item in scan_prefix(&self.db, &prefix) {
            let (k, v) = item?;
            let part_no = parse_part_key(&k)?;
            out.push((part_no, decode_part(&v)?));
        }
        Ok(out)
    }

    /// 会话过期清理辅助:列出全部会话(u: 前缀扫描)。
    pub fn list_all_sessions(&self) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_UPLOAD) {
            let (k, v) = item?;
            let uid = String::from_utf8(
                k.strip_prefix(PREFIX_UPLOAD)
                    .ok_or_else(|| Error::Corrupt("upload key missing prefix".into()))?
                    .to_vec(),
            )
            .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
            out.push((uid, decode_session(&v)?));
        }
        Ok(out)
    }

    /// 桶内会话(按创建时间升序;ListMultipartUploads)。
    pub fn list_bucket_sessions(
        &self,
        bucket: &str,
        prefix: &str,
        after_key: Option<(&str, &str)>,
        max: usize,
    ) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        let mut scanned = 0usize;
        let index_prefix = session_index_prefix(bucket);
        'outer: for item in scan_prefix(&self.db, &index_prefix) {
            let (k, _) = item?;
            let uid = parse_session_index_key(&k)?;
            let sess = match self.get_multipart(&uid)? {
                Some(s) => s,
                None => continue,
            };
            // AWS:已 Complete/Abort 的会话不再出现在 ListMultipartUploads
            if sess.completed {
                continue;
            }
            // 前缀 + 游标过滤(游标 = (key, upload_id),字典序)
            if !prefix.is_empty() && !sess.key.starts_with(prefix) {
                continue;
            }
            if let Some((mk, mu)) = after_key {
                let (a, b) = (sess.key.as_str(), uid.as_str());
                if a < mk || (a == mk && b <= mu) {
                    continue;
                }
            }
            if scanned >= max {
                break 'outer;
            }
            scanned += 1;
            out.push((uid, sess));
        }
        Ok(out)
    }

    /// Abort:删除会话 + 桶索引 + 全部已枚举分片键 + 分配释放记录。
    /// 分片键在事务外枚举(引擎持全局锁,无并发竞态)。
    pub fn abort_multipart(
        &self,
        upload_id: &str,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 2);
        ops.push(Op::MultipartDelete {
            upload_id: upload_id.to_string(),
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        self.commit(&ops)
    }

    /// Complete:对象写入 + 会话收尾 + 分片删除 + 分配/统计,单事务。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.complete_multipart_version(bucket, key, upload_id, None, meta, part_keys, draft, delta)
    }

    /// Complete 的版本化变体(ADR-11 §3.4.5;V2):`vk = Some` 时最终对象
    /// 落版本键(Enabled 新 vk / Suspended VK_NULL 槽),`None` 退化为
    /// 未版本化单键(与 complete_multipart 逐字节一致);会话/分片键不变。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart_version(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        vk: Option<&[u8; 16]>,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 4);
        match vk {
            Some(vk) => ops.push(Op::ObjectPutVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
                meta: meta.clone(),
            }),
            None => ops.push(Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            }),
        }
        ops.push(Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: true,
            final_etag: meta.etag,
            final_size: meta.size,
            final_mtime: meta.mtime,
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        ops.push(Op::Stats {
            bucket: bucket.to_string(),
            delta,
        });
        self.commit(&ops)
    }
}

fn apply_ops(tx: &Transaction<OptimisticTransactionDB>, ops: &[Op]) -> Result<u64> {
    // rocksdb 事务闭包内操作失败 → 整体 Err → 回滚(调用方 commit 不执行)。
    fn tget(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        tx.get(key).map_err(rocks_err)
    }
    fn tinsert(
        tx: &Transaction<OptimisticTransactionDB>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        tx.put(key, value).map_err(rocks_err)
    }
    fn tremove(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<()> {
        tx.delete(key).map_err(rocks_err)
    }
    /// 事务内枚举桶生命周期规则键(M11 L1;`r:{bucket}\0` 前缀)。
    /// 冲突集纪律:r: 键域的全部写路径(本函数调用方各事务臂)都先
    /// tget(b:{bucket}) 锚定桶记录,乐观冲突经桶键检出重试;迭代器读
    /// 本身不入乐观事务冲突集,不得脱离桶键锚点使用。
    fn tscan_lifecycle_rule_keys(
        tx: &Transaction<OptimisticTransactionDB>,
        bucket: &str,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = lifecycle_rules_prefix(bucket);
        let mut keys = Vec::new();
        let mut it = tx.iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in &mut it {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys.push(k.to_vec());
        }
        Ok(keys)
    }

    // 单点序列化:读 s:seq → 写 s:seq+1;并发事务在提交时冲突并重试
    let cur = tget(tx, SYS_SEQ)?
        .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
        .unwrap_or(0);
    let seq = cur + 1;

    for op in ops {
        match op {
            Op::BucketPut {
                name,
                meta,
                location,
            } => {
                let k = bucket_key(name);
                // 读以建立冲突集(并发修改则重试)
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
                let lk = bucket_location_key(name);
                tget(tx, &lk)?;
                match location.as_deref() {
                    Some(loc) => tinsert(tx, lk, loc.as_bytes().to_vec())?,
                    None => tremove(tx, &lk)?,
                }
            }
            Op::BucketDelete { name } => {
                let k = bucket_key(name);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("bucket {name}")));
                }
                tremove(tx, &k)?;
                tremove(tx, &bucket_location_key(name))?;
                // D9 桶级配置文档同事务清理(S1 bt:/S2 bc:/S7 bo:)
                for conf in BucketConf::ALL {
                    tremove(tx, &conf.key(name))?;
                }
                // M11 L1(ADR-12 DL1):生命周期规则两段式键,前缀扫描清理
                // (三处联动之二;BucketConf::ALL 仅覆盖单段式配置键)
                for rk in tscan_lifecycle_rule_keys(tx, name)? {
                    tremove(tx, &rk)?;
                }
            }
            Op::BucketSetVersioning { name, state } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.versioning = *state;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketSetEncryption { name, default } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.default_encryption = *default;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketSetObjectLock {
                name,
                default_retention,
            } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.object_lock = true;
                meta.versioning = fs3_core::VersioningState::Enabled;
                meta.default_retention = default_retention.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketConfPut {
                bucket,
                conf,
                value,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                let k = conf.key(bucket);
                tget(tx, &k)?;
                tinsert(tx, k, value.clone())?;
            }
            Op::BucketConfDelete { bucket, conf } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                tremove(tx, &conf.key(bucket))?;
            }
            Op::LifecycleRulesReplace { bucket, rules } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // DL1 单事务整体替换:旧规则键(前缀枚举)全删 → 新规则逐条写
                for old in tscan_lifecycle_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
                for r in rules {
                    tinsert(
                        tx,
                        lifecycle_rule_key(bucket, &r.id),
                        encode(r).map_err(|e| {
                            Error::Meta(format!("lifecycle rule {} encode: {e}", r.id))
                        })?,
                    )?;
                }
            }
            Op::LifecycleRulesDelete { bucket } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                for old in tscan_lifecycle_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
            }
            Op::ObjectSetTags {
                bucket,
                key,
                vk,
                tags,
            } => {
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                let cur = tget(tx, &k)?
                    .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
                let mut meta = decode_object(&cur)?;
                meta.tags = tags.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectPut { bucket, key, meta } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                let k = object_key(bucket, key);
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectMigrate {
                bucket,
                key,
                old_segments,
                new_segments,
            } => {
                let k = object_key(bucket, key);
                let cur = tget(tx, &k)?.ok_or_else(|| {
                    Error::ObjectChanged(format!("{bucket}/{key} deleted during compaction"))
                })?;
                let mut meta = decode_object(&cur)?;
                // 快照隔离 + 乐观重试:旧段必须仍按序被引用,否则放弃该对象
                // (ADR-9 §6.2 阶段 3:对象被并发覆盖/删除 → 下轮再来)。
                if old_segments.len() != new_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "{bucket}/{key} segment mapping mismatch"
                    )));
                }
                let mut ptr = 0usize;
                let mut out: Vec<Segment> = Vec::with_capacity(meta.extents.len());
                for s in &meta.extents {
                    if ptr < old_segments.len() && *s == old_segments[ptr] {
                        out.push(new_segments[ptr].clone());
                        ptr += 1;
                    } else {
                        out.push(s.clone());
                    }
                }
                if ptr != old_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "{bucket}/{key} segments changed during compaction"
                    )));
                }
                meta.extents = out;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::PartMigrate {
                upload_id,
                part_no,
                old_segments,
                new_segments,
            } => {
                let k = part_key(upload_id, *part_no);
                let cur = tget(tx, &k)?.ok_or_else(|| {
                    Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} deleted during compaction"
                    ))
                })?;
                let mut meta: PartMeta = decode_part(&cur)?;
                if old_segments.len() != new_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} segment mapping mismatch"
                    )));
                }
                let mut ptr = 0usize;
                let mut out: Vec<Segment> = Vec::with_capacity(meta.extents.len());
                for s in &meta.extents {
                    if ptr < old_segments.len() && *s == old_segments[ptr] {
                        out.push(new_segments[ptr].clone());
                        ptr += 1;
                    } else {
                        out.push(s.clone());
                    }
                }
                if ptr != old_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} segments changed during compaction"
                    )));
                }
                meta.extents = out;
                tinsert(tx, k, encode(&meta)?)?;
            }
            Op::ObjectDelete { bucket, key } => {
                let k = object_key(bucket, key);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("object {bucket}/{key}")));
                }
                tremove(tx, &k)?;
            }
            Op::ObjectDeleteCurrent {
                bucket,
                key,
                vk,
                marker,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // 删除标记契约(ADR-11 D3):size=0、extents/inline 为空
                if !marker.is_delete_marker
                    || marker.size != 0
                    || !marker.extents.is_empty()
                    || marker.inline.is_some()
                {
                    return Err(Error::InvalidArgument(format!(
                        "delete marker meta malformed for {bucket}/{key}"
                    )));
                }
                // Some(vk) = 版本键;None = 遗留未版本化单键原地覆盖(D1a-1)
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                // 读以建立冲突集(Suspended 覆盖 null 族 = 同事务读改写)
                tget(tx, &k)?;
                tinsert(tx, k, marker.encode_value()?)?;
            }
            Op::ObjectDeleteVersion { bucket, key, vk } => {
                let k = object_version_key(bucket, key, vk);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("object version {bucket}/{key}")));
                }
                tremove(tx, &k)?;
            }
            Op::ObjectPutVersion {
                bucket,
                key,
                vk,
                meta,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // 版本数据条目契约:删除标记须经 ObjectDeleteCurrent 写入
                if meta.is_delete_marker {
                    return Err(Error::InvalidArgument(format!(
                        "delete marker must use ObjectDeleteCurrent for {bucket}/{key}"
                    )));
                }
                let k = object_version_key(bucket, key, vk);
                // 读以建立冲突集(Suspended 覆盖 null 槽 = 同事务读改写)
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectMetaRewrite { key, meta } => {
                // 读以建立冲突集并要求键存在(防误写游离键)
                if tget(tx, key)?.is_none() {
                    return Err(Error::NotFound(format!(
                        "object raw key {} bytes",
                        key.len()
                    )));
                }
                tinsert(tx, key.clone(), meta.encode_value()?)?;
            }
            Op::Alloc { draft } => {
                if !draft.is_empty() {
                    let rec = AllocRecord {
                        seq,
                        txn: seq,
                        alloc: draft.alloc.clone(),
                        ref_inc: draft.ref_inc.clone(),
                        ref_dec: draft.ref_dec.clone(),
                    };
                    tinsert(tx, alloc_key(seq), encode(&rec)?)?;
                    tinsert(tx, txn_key(seq), seq.to_be_bytes().to_vec())?;
                }
            }
            Op::Stats { bucket, delta } => {
                let k = bucket_key(bucket);
                let mut meta = match tget(tx, &k)? {
                    Some(v) => decode_bucket(&v)?,
                    None => {
                        return Err(Error::NotFound(format!("bucket {bucket}")));
                    }
                };
                meta.stats.objects =
                    (meta.stats.objects as i128 + delta.objects as i128).max(0) as u64;
                meta.stats.bytes = (meta.stats.bytes as i128 + delta.bytes as i128).max(0) as u64;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::MultipartCreate { upload_id, session } => {
                // 桶必须存在
                if tget(tx, &bucket_key(&session.bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {}", session.bucket)));
                }
                let uk = session_key(upload_id);
                tget(tx, &uk)?;
                tinsert(tx, uk, encode(session)?)?;
                let mk = session_index_key(&session.bucket, upload_id);
                tget(tx, &mk)?;
                tinsert(tx, mk, Vec::<u8>::new())?;
            }
            Op::MultipartUpdate {
                upload_id,
                completed,
                final_etag,
                final_size,
                final_mtime,
            } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let mut sess: MultipartSession = decode(&cur)?;
                sess.completed = *completed;
                sess.final_etag = *final_etag;
                sess.final_size = *final_size;
                sess.final_mtime = *final_mtime;
                tinsert(tx, uk, encode(&sess)?)?;
            }
            Op::MultipartDelete { upload_id } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let sess: MultipartSession = decode(&cur)?;
                tremove(tx, &uk)?;
                let mk = session_index_key(&sess.bucket, upload_id);
                tget(tx, &mk)?;
                tremove(tx, &mk)?;
            }
            Op::PartPut {
                upload_id,
                part_no,
                meta,
            } => {
                // 会话必须存在(NoSuchUpload 语义)
                if tget(tx, &session_key(upload_id))?.is_none() {
                    return Err(Error::NotFound(format!("upload {upload_id}")));
                }
                let pk = part_key(upload_id, *part_no);
                tget(tx, &pk)?;
                tinsert(tx, pk, encode(meta)?)?;
            }
            Op::PartDelete { key } => {
                if tget(tx, key)?.is_some() {
                    tremove(tx, key)?;
                }
            }
            Op::KeyPut { access_key, record } => {
                let k = key_key(access_key);
                tget(tx, &k)?;
                tinsert(tx, k, encode(record)?)?;
            }
            Op::KeyDelete { access_key } => {
                let k = key_key(access_key);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("key {access_key}")));
                }
                tremove(tx, &k)?;
            }
        }
    }

    tinsert(tx, SYS_SEQ.to_vec(), seq.to_be_bytes().to_vec())?;
    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    use fs3_core::{BucketStats, Segment};

    fn open_tmp() -> (tempfile::TempDir, MetaStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        (dir, store)
    }

    fn bucket_meta(name: &str) -> BucketMeta {
        BucketMeta {
            created: 1,
            owner: name.to_string(),
            stats: BucketStats::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
        }
    }

    fn object_meta(size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: [0u8; 16],
            mtime: 1,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: Vec::new(),
        }
    }

    /// 删除标记条目(ADR-11 D3:size=0、extents/inline 空;vk 落入 version_id,
    /// null 槽调用方传 None)。
    fn delete_marker(vk: Option<[u8; 16]>) -> ObjectMeta {
        ObjectMeta {
            is_delete_marker: true,
            version_id: vk,
            ..object_meta(0)
        }
    }

    #[test]
    fn bucket_crud() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.list_buckets().unwrap().len(), 1);
        s.commit_bucket_delete("b1").unwrap();
        assert!(s.get_bucket("b1").unwrap().is_none());
    }

    #[test]
    fn trusted_clock_persist_roundtrip() {
        // M12 W1-1:s:trusted_clock 缺席/往返/重开可读
        let (d, s) = open_tmp();
        assert!(s.load_trusted_clock().unwrap().is_none());
        let st = fs3_core::TrustedClockState {
            last_wall: 1_700_000_000,
            last_mono_ns: 42_000_000_000,
        };
        s.put_trusted_clock(&st).unwrap();
        assert_eq!(s.load_trusted_clock().unwrap(), Some(st));
        drop(s);
        let s2 = MetaStore::open(d.path(), &MetaConfig::default()).unwrap();
        assert_eq!(s2.load_trusted_clock().unwrap(), Some(st));
    }

    #[test]
    fn bucket_location_roundtrip() {
        // M8:LocationConstraint 与桶同事务持久化 + 删除清理
        let (_d, s) = open_tmp();
        assert_eq!(s.bucket_location("b1").unwrap(), "", "无桶 → 默认空");
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s.bucket_location("b1").unwrap(),
            "",
            "旧语义 PUT → 无 location"
        );
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "s3")
            .unwrap();
        assert_eq!(s.bucket_location("b1").unwrap(), "s3", "回显约束");
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "")
            .unwrap();
        assert_eq!(s.bucket_location("b1").unwrap(), "", "清空约束");
        s.commit_bucket_delete("b1").unwrap();
        assert_eq!(
            s.bucket_location("b1").unwrap(),
            "",
            "删除桶 → location 清理"
        );
    }

    #[test]
    fn bucket_conf_roundtrip_and_delete_cleanup() {
        // M10 S1/S2/S7:D9 桶级配置文档(bc:/bt:/bo:)读写删 + 删桶清理
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for conf in BucketConf::ALL {
            assert_eq!(s.bucket_conf("b1", conf).unwrap(), None, "{conf:?} 无配置");
            s.commit_bucket_conf_put("b1", conf, b"<doc/>").unwrap();
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap().as_deref(),
                Some(b"<doc/>".as_slice()),
                "{conf:?} 写入可读"
            );
            // 覆盖写
            s.commit_bucket_conf_put("b1", conf, b"<doc2/>").unwrap();
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap().as_deref(),
                Some(b"<doc2/>".as_slice())
            );
            // 不存在桶 → NotFound
            assert!(s.commit_bucket_conf_put("ghost", conf, b"x").is_err());
        }
        // 删除单个配置(幂等):其余配置不动
        s.commit_bucket_conf_delete("b1", BucketConf::Cors).unwrap();
        assert_eq!(s.bucket_conf("b1", BucketConf::Cors).unwrap(), None);
        assert!(s.bucket_conf("b1", BucketConf::Tagging).unwrap().is_some());
        s.commit_bucket_conf_delete("b1", BucketConf::Cors).unwrap();
        // 删桶 → 全部配置键清理(BucketDelete 事务臂)
        s.commit_bucket_delete("b1").unwrap();
        for conf in BucketConf::ALL {
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap(),
                None,
                "{conf:?} 删桶后残留"
            );
        }
    }

    /// M11 L1(ADR-12 DL1):生命周期规则三原语——整体替换语义、删桶
    /// 事务清理、两桶前缀隔离。
    #[test]
    fn lifecycle_rules_replace_delete_and_bucket_cleanup() {
        use fs3_core::{
            AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter, LifecycleRule,
            LifecycleStatus,
        };
        let rule = |id: &str, days: u32| LifecycleRule {
            id: id.into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: Some(LifecycleExpiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            legacy_prefix: false,
        };
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // 无规则 → 空表;不存在桶写入 → NotFound
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        assert!(s.put_lifecycle_rules("ghost", &[rule("r1", 30)]).is_err());
        assert!(s.delete_lifecycle_rules("ghost").is_err());
        // 写入两条(b1),b2 一条:前缀隔离
        s.put_lifecycle_rules("b1", &[rule("r1", 30), rule("r2", 60)])
            .unwrap();
        s.put_lifecycle_rules(
            "b2",
            &[LifecycleRule {
                id: "x".into(),
                abort_incomplete_multipart: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                expiration: None,
                ..rule("x", 0)
            }],
        )
        .unwrap();
        let got = s.get_lifecycle_rules("b1").unwrap();
        assert_eq!(got, vec![rule("r1", 30), rule("r2", 60)]);
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        assert_eq!(
            s.get_lifecycle_rules("b2").unwrap()[0]
                .abort_incomplete_multipart
                .unwrap()
                .days_after_initiation,
            7
        );
        // 整体替换:r1 删除、r2 改写、r3 新增,单事务完成
        s.put_lifecycle_rules("b1", &[rule("r2", 90), rule("r3", 1)])
            .unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![rule("r2", 90), rule("r3", 1)],
            "替换后不得残留 r1"
        );
        // b2 不受 b1 替换影响
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        // delete 幂等;不影响 b2
        s.delete_lifecycle_rules("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        s.delete_lifecycle_rules("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        // 删桶 → r: 键同事务清理;再建同名桶无残留
        s.put_lifecycle_rules("b1", &[rule("r1", 30)]).unwrap();
        s.commit_bucket_delete("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![],
            "删桶后规则必须随桶清理"
        );
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
    }

    /// M11 L5:LifecycleRule 尾部追加 `legacy_prefix`——新格式往返 +
    /// L1 初版格式字节直写回退(false,存量规则零迁移可读)。
    #[test]
    fn lifecycle_rule_legacy_prefix_dual_read() {
        use fs3_core::{
            AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter, LifecycleRule,
            LifecycleStatus, NoncurrentVersionExpiration,
        };
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let rule = LifecycleRule {
            id: "r1".into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter {
                prefix: "old/".into(),
                tags: vec![],
            },
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            legacy_prefix: true,
        };
        s.put_lifecycle_rules("b1", std::slice::from_ref(&rule))
            .unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![rule],
            "新格式往返(legacy_prefix 原样)"
        );
        // L1 初版格式字节(无 legacy_prefix 尾部)直写 → 回退 false
        #[derive(serde::Serialize)]
        struct RuleV12 {
            id: String,
            status: LifecycleStatus,
            filter: LifecycleFilter,
            expiration: Option<LifecycleExpiration>,
            noncurrent_expiration: Option<NoncurrentVersionExpiration>,
            abort_incomplete_multipart: Option<AbortIncompleteMultipartUpload>,
        }
        let old = RuleV12 {
            id: "r2".into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: Some(LifecycleExpiration {
                days: Some(7),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
        };
        let bytes = postcard::to_allocvec(&old).unwrap();
        s.db.put(lifecycle_rule_key("b1", "r2"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        s.debug_clear_lifecycle_cache();
        let got = s.get_lifecycle_rules("b1").unwrap();
        let r2 = got.iter().find(|r| r.id == "r2").unwrap();
        assert!(!r2.legacy_prefix, "初版格式值回退 legacy_prefix=false");
        assert_eq!(r2.expiration.as_ref().unwrap().days, Some(7));
    }

    #[test]
    fn object_set_tags_read_modify_write() {
        // M10 S1:ObjectSetTags 单事务读改写(仅 tags 变更;缺失目标 → NotFound)
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(3),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 3,
            },
        )
        .unwrap();
        let tags = vec![("a".to_string(), "b".to_string())];
        s.commit_object_set_tags("b1", "k", None, tags.clone())
            .unwrap();
        let m = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(m.tags, tags);
        assert_eq!(m.size, 3, "其余字段不动");
        // 清空
        s.commit_object_set_tags("b1", "k", None, vec![]).unwrap();
        assert!(s.get_object("b1", "k").unwrap().unwrap().tags.is_empty());
        // 缺失对象/缺失版本键 → NotFound
        assert!(s
            .commit_object_set_tags("b1", "ghost", None, vec![])
            .is_err());
        assert!(s
            .commit_object_set_tags("b1", "k", Some([0x42u8; 16]), vec![])
            .is_err());
    }

    #[test]
    fn object_put_get_delete() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq = s
            .commit_object_put(
                "b1",
                "key\x00with\u{FF}bytes",
                &object_meta(100),
                AllocDraft {
                    alloc: vec![(3, 2)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 100,
                },
            )
            .unwrap();
        assert_eq!(seq, 2); // 建桶(1)+ 对象 PUT(2)
        let m = s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .unwrap();
        assert_eq!(m.size, 100);
        // 桶统计记账
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 1);
        assert_eq!(b.stats.bytes, 100);

        // 分配记录可见
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(3, 2)]);
        assert_eq!(s.last_seq().unwrap(), 2); // 建桶(1)+ 对象 PUT(2)

        // 删除:对象消失,统计回退,释放记录生成
        s.commit_object_delete(
            "b1",
            "key\x00with\u{FF}bytes",
            AllocDraft {
                ref_dec: vec![3, 4],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -100,
            },
        )
        .unwrap();
        assert!(s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .is_none());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 0);
        assert_eq!(b.stats.bytes, 0);
    }

    #[test]
    fn off_bucket_key_shape_byte_exact() {
        // V4-2 断言性回归:Off(未版本化)桶对象提交路径落键逐字节 =
        // `o:{bucket}\0{esc(key)}`,绝不产生 `\0{vk16}` 版本键形态
        // (ADR-11 D1 硬承诺;v1.0.x 行为逐字节保持)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let put = |key: &str, size: u64| {
            s.commit_object_put(
                "b1",
                key,
                &object_meta(size),
                AllocDraft::default(),
                StatsDelta {
                    objects: 1,
                    bytes: size as i64,
                },
            )
            .unwrap();
        };
        put("k", 5);
        put("dir/a\x00b", 7);
        // 原始扫描全库 `o:` 键:逐字节比对 + 双形态解析均无 vk 后缀
        let obj_keys: Vec<Vec<u8>> =
            s.db.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward))
                .map(|item| item.unwrap().0.to_vec())
                .take_while(|k| k.starts_with(PREFIX_OBJECT))
                .collect();
        assert_eq!(
            obj_keys,
            vec![
                b"o:b1\x00dir/a\xff\x00b".as_slice(), // esc(0x00) = FF 00
                b"o:b1\x00k".as_slice(),
            ]
        );
        for k in &obj_keys {
            let (_, _, vk) = parse_object_version_key(k).unwrap();
            assert!(vk.is_none(), "Off 桶不得产生版本键形态: {k:?}");
        }
        // 删除后 `o:` 键零残留(物理删除,无标记/版本键)
        for (key, size) in [("k", 5i64), ("dir/a\x00b", 7)] {
            s.commit_object_delete(
                "b1",
                key,
                AllocDraft::default(),
                StatsDelta {
                    objects: -1,
                    bytes: -size,
                },
            )
            .unwrap();
        }
        let rest: Vec<Vec<u8>> =
            s.db.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward))
                .map(|item| item.unwrap().0.to_vec())
                .take_while(|k| k.starts_with(PREFIX_OBJECT))
                .collect();
        assert!(rest.is_empty(), "删除后无 o: 键残留: {rest:?}");
    }

    #[test]
    fn object_in_missing_bucket_aborts() {
        let (_d, s) = open_tmp();
        let r = s.commit_object_put(
            "nope",
            "k",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        );
        assert!(matches!(r, Err(Error::NotFound(_))));
    }

    #[test]
    fn seq_serializes_transactions() {
        let (_d, s) = open_tmp();
        let seq1 = s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq2 = s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(s.last_seq().unwrap(), 2);
    }

    #[test]
    fn concurrent_commits_serialize() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut handles = vec![];
        for i in 0..8 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..50 {
                    s.commit_object_put(
                        "b1",
                        &format!("k{i}-{j}"),
                        &object_meta(j as u64),
                        AllocDraft {
                            alloc: vec![(i * 100 + j as u64, 1)],
                            ..Default::default()
                        },
                        StatsDelta {
                            objects: 1,
                            bytes: j,
                        },
                    )
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 400 个对象全部可见,seq 单调,无重复分配记录
        assert_eq!(s.list_objects("b1", "").unwrap().len(), 400);
        assert_eq!(s.last_seq().unwrap(), 401); // 建桶(1)+ 400 次 PUT
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 400);
        let mut seen = std::collections::HashSet::new();
        for r in &recs {
            assert!(seen.insert(r.seq), "dup seq {}", r.seq);
            assert_eq!(r.seq, r.txn);
        }
    }

    #[test]
    fn key_encoding_prevents_cross_bucket_scan() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b", &bucket_meta("b")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // b 桶的对象不得出现在 b2 桶的前缀扫描里
        s.commit_object_put(
            "b",
            "x",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert!(s.list_objects("b2", "").unwrap().is_empty());
        assert_eq!(s.list_objects("b", "").unwrap().len(), 1);
    }

    #[test]
    fn reset_seq_and_seed_salt_import_support() {
        // M7 E5:meta-import 的基础原语——序号可复位续写、种子盐可覆写。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(s.last_seq().unwrap(), 1);
        // 导出时的序号(如 42)复位后,后续事务从 base+1 继续
        s.reset_seq(42).unwrap();
        assert_eq!(s.last_seq().unwrap(), 42);
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        assert_eq!(s.last_seq().unwrap(), 43);
        // 种子盐覆写(导入备份时恢复原种子盐,AES-GCM 密文可解)
        let salt = s.seed_salt().unwrap();
        s.set_seed_salt(b"restored-seed-salt-000000000000000000000000000000000000")
            .unwrap();
        // 重开 DB 后仍是覆写值(种子盐不随 seed_salt() 默认生成逻辑变更)
        let dir = tempfile::tempdir().unwrap();
        {
            let st = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            st.set_seed_salt(b"x-000000000000000000000000000000000000000000000000")
                .unwrap();
        }
        let st = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert_eq!(
            st.seed_salt().unwrap(),
            b"x-000000000000000000000000000000000000000000000000"
        );
        assert_ne!(salt, st.seed_salt().unwrap());
    }

    #[test]
    fn sync_modes_full_and_none_work() {
        // Full:每事务 fsync;None:禁用 WAL(纯 memtable)。两条路径的基本
        // 读写、seq 推进、flush 幂等都必须正常。
        for mode in [SyncMode::Full, SyncMode::None] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = MetaConfig {
                flush_every_ms: 1,
                sync_mode: mode,
                cache_capacity: None,
            };
            let s = MetaStore::open(dir.path(), &cfg).unwrap();
            assert_eq!(s.sync_mode(), mode);
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(5),
                AllocDraft::default(),
                StatsDelta {
                    objects: 1,
                    bytes: 5,
                },
            )
            .unwrap();
            assert_eq!(s.last_seq().unwrap(), 2);
            assert!(s.get_object("b1", "k").unwrap().is_some());
            // None 模式 WAL 已禁用:flush 为空操作,不得报错
            s.flush().unwrap();
            assert_eq!(s.list_alloc_records(0).unwrap().len(), 0);
        }
    }

    #[test]
    fn reopen_persists_data() {
        // rocksdb WAL 恢复:重开目录后数据完整、seq 延续
        let dir = tempfile::tempdir().unwrap();
        {
            let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(7),
                AllocDraft {
                    alloc: vec![(1, 1)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 7,
                },
            )
            .unwrap();
        } // drop → rocksdb 关闭时刷 WAL
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.get_object("b1", "k").unwrap().unwrap().size, 7);
        assert_eq!(s.last_seq().unwrap(), 2);
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(1, 1)]);
    }

    #[test]
    fn object_value_version_byte_and_legacy_rejection() {
        // ADR-9 §13:值 = [版本字节] + postcard;旧值(无版本字节)拒绝解码
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut m = object_meta(100);
        m.extents = vec![Segment {
            extent_id: 3,
            offset: 0,
            len: 65536,
            crcs: vec![1, 2],
        }];
        s.commit_object_put("b1", "k", &m, AllocDraft::default(), StatsDelta::default())
            .unwrap();
        let got = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(got, m, "段列表往返一致");
        // 直接向 rocksdb 写入无版本字节的旧格式值 → 解码拒绝(Corrupt)
        let legacy = postcard::to_allocvec(&m).unwrap();
        let db = s.db.clone();
        db.put(object_key("b1", "legacy"), legacy).unwrap();
        assert!(matches!(
            s.get_object("b1", "legacy"),
            Err(Error::Corrupt(_))
        ));
    }

    /// v2 值格式夹具(M10 V5-3 测试用;字段与 fs3-core ObjectMetaV2
    /// 逐一对应,postcard 字段序即编码序)。
    #[derive(serde::Serialize)]
    struct ObjectMetaV2Fixture {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        content_type: String,
        user_meta: Vec<(String, String)>,
        inline: Option<Vec<u8>>,
        parts: Vec<u64>,
        resp_headers: Vec<(String, String)>,
    }

    /// 以 v2 版本字节编码 ObjectMeta(丢弃 v3 尾部字段)。
    fn encode_v2_value(m: &ObjectMeta) -> Vec<u8> {
        let f = ObjectMetaV2Fixture {
            size: m.size,
            etag: m.etag,
            mtime: m.mtime,
            extents: m.extents.clone(),
            content_type: m.content_type.clone(),
            user_meta: m.user_meta.clone(),
            inline: m.inline.clone(),
            parts: m.parts.clone(),
            resp_headers: m.resp_headers.clone(),
        };
        let mut v = vec![2u8];
        v.extend(postcard::to_allocvec(&f).unwrap());
        v
    }

    #[test]
    fn commit_object_meta_update_reencodes_in_place() {
        // M10 V5-3:单事务按原始键重编码(不改统计/分配;双形态键均覆盖);
        // 键不存在 → NotFound。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let m = object_meta(64);
        s.commit_object_put(
            "b1",
            "k",
            &m,
            AllocDraft {
                alloc: vec![(1, 1)],
                ..Default::default()
            },
            StatsDelta {
                objects: 1,
                bytes: 64,
            },
        )
        .unwrap();
        let vk = [7u8; 16];
        let mv = ObjectMeta {
            version_id: Some(vk),
            ..object_meta(32)
        };
        s.commit_object_put_version(
            "b1",
            "vk",
            &vk,
            &mv,
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 32,
            },
        )
        .unwrap();

        // v2 存量值(单键 + 版本键各一)
        s.put_object_value_raw("b1", "k", None, &encode_v2_value(&m))
            .unwrap();
        s.put_object_value_raw("b1", "vk", Some(&vk), &encode_v2_value(&mv))
            .unwrap();
        assert_eq!(s.count_object_value_versions().unwrap(), (2, 0));

        // 重写:值内容不变、版本字节 → 3、统计/分配零触碰
        for e in s.snapshot_all_objects_raw().unwrap() {
            assert_eq!(e.value_version, 2);
            s.commit_object_meta_update(&e.raw_key, &e.meta).unwrap();
        }
        assert_eq!(s.count_object_value_versions().unwrap(), (0, 2));
        let expect_m = ObjectMeta::decode_value(&encode_v2_value(&m)).unwrap();
        assert_eq!(s.get_object("b1", "k").unwrap().unwrap(), expect_m);
        // 重写 = 双读结果的原样重编码:v2 无 version_id 字段,双读后为
        // None,重写保持 None(commit 层不做引擎不变量归一;真实存量 v2
        // 值只可能位于未版本化单键 —— 版本键是 v1.1 新键,写入恒 v3)。
        let got_v = s.get_object_version("b1", "vk", &vk).unwrap().unwrap();
        assert_eq!(
            got_v,
            ObjectMeta::decode_value(&encode_v2_value(&mv)).unwrap()
        );
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (2, 96), "统计不变");
        // 重写事务只推 s:seq,不产生新 a: 记录
        assert_eq!(s.list_alloc_records(0).unwrap().len(), 1);

        // 键不存在 → NotFound
        assert!(matches!(
            s.commit_object_meta_update(&object_key("b1", "ghost"), &m),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn raw_snapshot_and_value_version_probe() {
        // M10 V5-3:snapshot_all_objects_raw 携带原始键/值版本字节;
        // count_object_value_versions 只读首字节;完成标记读写。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "a",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let vk = [9u8; 16];
        s.commit_object_put_version(
            "b1",
            "a",
            &vk,
            &ObjectMeta {
                version_id: Some(vk),
                ..object_meta(2)
            },
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.put_object_value_raw("b1", "old", None, &encode_v2_value(&object_meta(3)))
            .unwrap();

        let raw = s.snapshot_all_objects_raw().unwrap();
        assert_eq!(raw.len(), 3);
        for e in &raw {
            // 原始键与解析字段互洽(经 keys.rs 单入口往返)
            let (b, k, v) = parse_object_version_key(&e.raw_key).unwrap();
            assert_eq!((b, k, v), (e.bucket.clone(), e.key.clone(), e.vk));
        }
        let v2 = raw.iter().filter(|e| e.value_version == 2).count();
        let cur = raw
            .iter()
            .filter(|e| e.value_version == fs3_core::OBJECT_META_VERSION)
            .count();
        assert_eq!((v2, cur), (1, 2));
        assert_eq!(s.count_object_value_versions().unwrap(), (1, 2));

        // 完成标记:未落 → false;落 → true(幂等)
        assert!(!s.value_rewrite_v3_done().unwrap());
        s.mark_value_rewrite_v3_done().unwrap();
        assert!(s.value_rewrite_v3_done().unwrap());
        s.mark_value_rewrite_v3_done().unwrap();
        assert!(s.value_rewrite_v3_done().unwrap());
    }

    #[test]
    fn migrate_txn_replaces_segments_and_detects_stale() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut m = object_meta(200_000);
        let old = vec![
            Segment {
                extent_id: 1,
                offset: 0,
                len: 100_000,
                crcs: vec![11],
            },
            Segment {
                extent_id: 2,
                offset: 0,
                len: 100_000,
                crcs: vec![22],
            },
        ];
        m.extents = old.clone();
        s.commit_object_put("b1", "k", &m, AllocDraft::default(), StatsDelta::default())
            .unwrap();
        // 迁移:旧段 → 新段(按序替换)
        let new = vec![
            Segment {
                extent_id: 7,
                offset: 0,
                len: 100_000,
                crcs: vec![77],
            },
            Segment {
                extent_id: 8,
                offset: 0,
                len: 100_000,
                crcs: vec![88],
            },
        ];
        s.commit_object_migrate(
            "b1",
            "k",
            &old,
            &new,
            AllocDraft {
                alloc: vec![(7, 2)],
                ref_dec: vec![1, 2],
                ..Default::default()
            },
        )
        .unwrap();
        let got = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(got.extents, new);
        assert_eq!(got.size, 200_000, "元数据其余字段不变");
        // 旧段已不存在 → ObjectChanged(对象被并发覆盖/删除的模拟)
        let r = s.commit_object_migrate("b1", "k", &old, &new, AllocDraft::default());
        assert!(matches!(r, Err(Error::ObjectChanged(_))));
        // 对象已删除 → ObjectChanged(不得复活)
        s.commit_object_delete("b1", "k", AllocDraft::default(), StatsDelta::default())
            .unwrap();
        let r = s.commit_object_migrate("b1", "k", &new, &old, AllocDraft::default());
        assert!(matches!(r, Err(Error::ObjectChanged(_))));
    }

    #[test]
    fn snapshot_scan_all_objects_and_parts() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for i in 0..3 {
            s.commit_object_put(
                "b1",
                &format!("k{i}"),
                &object_meta(i),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // multipart 分片也进入快照扫描(恢复可达性 + 压缩发现)
        let uid = "upload-1";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "big",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let part = PartMeta {
            size: 100,
            etag: [1u8; 16],
            mtime: 1,
            extents: vec![Segment {
                extent_id: 9,
                offset: 0,
                len: 100,
                crcs: vec![],
            }],
            inline: None,
            checksum: None,
            sse: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        let objs = s.snapshot_all_objects().unwrap();
        assert_eq!(objs.len(), 3);
        assert!(objs.iter().all(|(b, _, _, _)| b == "b1"));
        let parts = s.snapshot_all_parts().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, uid);
        assert_eq!(parts[0].1, 1);
        assert_eq!(parts[0].2.extents, part.extents);
    }

    #[test]
    fn part_meta_checksum_dual_read() {
        // M11 C1-4(ADR-12 D-E3):PartMeta 尾部追加 checksum;新格式往返 +
        // 旧格式字节(无 checksum 字段)双读缺省 None。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let uid = "up-ck";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let ck = fs3_core::ChecksumInfo {
            algorithm: fs3_core::ChecksumAlgorithm::Crc32c,
            value: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let part = PartMeta {
            size: 5,
            etag: [7u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![1u8; 5]),
            checksum: Some(ck.clone()),
            sse: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        // 新格式往返(checksum 原样)
        let got = s.get_part(uid, 1).unwrap().unwrap();
        assert_eq!(got, part);
        assert_eq!(s.list_parts(uid).unwrap()[0].1.checksum, Some(ck));

        // 旧格式字节(无 checksum 尾部字段)直写 → 双读补 None
        #[derive(serde::Serialize)]
        struct LegacyPartMeta {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            inline: Option<Vec<u8>>,
        }
        let legacy = LegacyPartMeta {
            size: 9,
            etag: [2u8; 16],
            mtime: 3,
            extents: vec![],
            inline: Some(vec![0u8; 9]),
        };
        let bytes = postcard::to_allocvec(&legacy).unwrap();
        s.db.put(part_key(uid, 2), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_part(uid, 2).unwrap().unwrap();
        assert_eq!(got.size, 9);
        assert_eq!(got.checksum, None, "旧格式值双读缺省 None");
        // 截断损坏值 → Corrupt(两种格式都失败),不静默
        assert!(matches!(
            decode_part(&bytes[..bytes.len() - 2]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn part_meta_sse_dual_read() {
        // M11 E1-4(ADR-12 D-E4):PartMeta 尾部再追加 sse;新格式往返 +
        // 中间格式字节(含 checksum 无 sse)三读缺省 None。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let uid = "up-sse";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let sse = fs3_core::SseInfo::sse_c([0xAB; 12], vec![[0x11; 16], [0x22; 16]], [0x5C; 16]);
        let part = PartMeta {
            size: 5,
            etag: [7u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![1u8; 5]),
            checksum: None,
            sse: Some(sse.clone()),
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        // 新格式往返(sse 原样)
        let got = s.get_part(uid, 1).unwrap().unwrap();
        assert_eq!(got, part);
        assert_eq!(got.sse, Some(sse));

        // 中间格式字节(checksum 层,无 sse 尾部)直写 → 三读补 None
        #[derive(serde::Serialize)]
        struct PartMetaV12 {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            inline: Option<Vec<u8>>,
            checksum: Option<fs3_core::ChecksumInfo>,
        }
        let mid = PartMetaV12 {
            size: 7,
            etag: [3u8; 16],
            mtime: 2,
            extents: vec![],
            inline: Some(vec![0u8; 7]),
            checksum: None,
        };
        let bytes = postcard::to_allocvec(&mid).unwrap();
        s.db.put(part_key(uid, 2), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_part(uid, 2).unwrap().unwrap();
        assert_eq!(got.size, 7);
        assert_eq!(got.sse, None, "中间格式值三读缺省 None");
    }

    #[test]
    fn session_sse_key_md5_dual_read() {
        // M11 E1-4:MultipartSession 尾部再追加 sse_key_md5;新格式往返 +
        // 中间格式字节(含 checksum_alg 无 sse_key_md5)五读缺省 None;
        // 会话只存 key-MD5(客户密钥本体零落盘,DE1 红线)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let sess = MultipartSession::new(
            "b1",
            "m",
            "text/x",
            vec![],
            vec![],
            vec![],
            None,
            Some("1B2M2Y8AsgTpgAmY7PhCfg==".to_string()),
            None,
        );
        s.create_multipart("up-1", &sess).unwrap();
        let got = s.get_multipart("up-1").unwrap().unwrap();
        assert_eq!(got, sess, "新格式往返(sse_key_md5 原样)");

        // 中间格式字节(checksum_alg 层,无 sse_key_md5 尾部)直写 → 五读补 None
        #[derive(serde::Serialize)]
        struct SessionV12b {
            bucket: String,
            key: String,
            content_type: String,
            user_meta: Vec<(String, String)>,
            resp_headers: Vec<(String, String)>,
            created: i64,
            completed: bool,
            final_etag: [u8; 16],
            final_size: u64,
            final_mtime: i64,
            tags: Vec<(String, String)>,
            checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        }
        let mid = SessionV12b {
            bucket: "b1".into(),
            key: "m".into(),
            content_type: "text/x".into(),
            user_meta: vec![],
            resp_headers: vec![],
            created: 1,
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags: vec![],
            checksum_alg: None,
        };
        let bytes = postcard::to_allocvec(&mid).unwrap();
        s.db.put(session_key("up-2"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_multipart("up-2").unwrap().unwrap();
        assert_eq!(got.sse_key_md5, None, "中间格式值五读缺省 None");

        // M11 K1-1:sse_s3 尾部字段——新格式往返 + V12c 中间格式(含
        // sse_key_md5 无 sse_s3)六读缺省 None
        let sess_s3 = MultipartSession {
            sse_s3: Some(SessionSseS3 {
                kek_id: 2,
                wrapped_dek: vec![0xAB; 60],
            }),
            ..sess.clone()
        };
        s.create_multipart("up-3", &sess_s3).unwrap();
        let got = s.get_multipart("up-3").unwrap().unwrap();
        assert_eq!(got, sess_s3, "新格式往返(sse_s3 原样)");
        #[derive(serde::Serialize)]
        struct SessionV12c {
            bucket: String,
            key: String,
            content_type: String,
            user_meta: Vec<(String, String)>,
            resp_headers: Vec<(String, String)>,
            created: i64,
            completed: bool,
            final_etag: [u8; 16],
            final_size: u64,
            final_mtime: i64,
            tags: Vec<(String, String)>,
            checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
            sse_key_md5: Option<String>,
        }
        let midc = SessionV12c {
            bucket: "b1".into(),
            key: "m".into(),
            content_type: "text/x".into(),
            user_meta: vec![],
            resp_headers: vec![],
            created: 1,
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags: vec![],
            checksum_alg: None,
            sse_key_md5: Some("1B2M2Y8AsgTpgAmY7PhCfg==".into()),
        };
        let bytes = postcard::to_allocvec(&midc).unwrap();
        s.db.put(session_key("up-4"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_multipart("up-4").unwrap().unwrap();
        assert_eq!(
            got.sse_key_md5.as_deref(),
            Some("1B2M2Y8AsgTpgAmY7PhCfg=="),
            "V12c 层 sse_key_md5 保留"
        );
        assert_eq!(got.sse_s3, None, "V12c 中间格式值六读缺省 None");

        // M11 L5:list_all_sessions 同走 decode_session 回退链(生命周期
        // 执行器会话阶段路径)——混合格式存量会话全可读;修复前该路径误用
        // 裸 decode,任一旧格式会话值即令执行器周期永久卡死
        let all = s.list_all_sessions().unwrap();
        assert_eq!(all.len(), 4, "混合格式会话全部可列出: {all:?}");
    }

    /// M11 K1-1(ADR-12 DS1):KEK 种子幂等生成/持久化;代状态惰性默认 +
    /// 轮换 + 重包裹收敛标记;BucketSetEncryption 读改写。
    #[test]
    fn sse_kek_state_and_bucket_encryption() {
        let (_d, s) = open_tmp();
        // 种子:首次生成 64B,二次读同值(幂等持久化)
        let seed = s.sse_kek_seed().unwrap();
        assert_eq!(seed.len(), 64);
        assert_eq!(s.sse_kek_seed().unwrap(), seed, "幂等(不重新生成)");
        // 与访问密钥种子盐相互独立(DS1 红线)
        assert_ne!(seed[..], s.seed_salt().unwrap()[..]);
        // 代状态:缺席 = 初始代 1(惰性不落盘)
        let st = s.sse_kek_gen_state().unwrap();
        assert_eq!(
            st,
            SseKekGenState {
                gen: 1,
                last_rotated_at: 0,
                rewrap_done_gen: 1
            }
        );
        // 轮换:gen+1,rewrap_done_gen 不动 → 待办标记拉开
        let st = s.rotate_sse_kek().unwrap();
        assert_eq!(st.gen, 2);
        assert!(st.last_rotated_at > 0);
        assert_eq!(st.rewrap_done_gen, 1);
        // 重包裹收敛:done = gen → 无待办
        s.mark_sse_rewrap_done(2).unwrap();
        let st = s.sse_kek_gen_state().unwrap();
        assert_eq!(st.rewrap_done_gen, 2);
        // 持久化:重开同库状态仍在
        drop(s);
        let s2 = MetaStore::open(_d.path(), &MetaConfig::default()).unwrap();
        assert_eq!(s2.sse_kek_gen_state().unwrap().gen, 2);
        assert_eq!(s2.sse_kek_seed().unwrap(), seed);

        // 桶默认加密读改写(其余字段不动;桶不在 → NotFound)
        s2.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s2.get_bucket("b1").unwrap().unwrap().default_encryption,
            None
        );
        s2.commit_bucket_set_encryption("b1", Some(fs3_core::SseAlgorithm::Aes256))
            .unwrap();
        let m = s2.get_bucket("b1").unwrap().unwrap();
        assert_eq!(m.default_encryption, Some(fs3_core::SseAlgorithm::Aes256));
        assert_eq!(m.owner, bucket_meta("b1").owner, "其余字段原样");
        // Delete 幂等(None → None 同样 Ok)
        s2.commit_bucket_set_encryption("b1", None).unwrap();
        assert_eq!(
            s2.get_bucket("b1").unwrap().unwrap().default_encryption,
            None
        );
        assert!(s2.commit_bucket_set_encryption("ghost", None).is_err());
    }

    #[test]
    fn list_page_after_marker_is_exclusive() {
        // 回归:s3-tests test_bucket_list_many — 游标必须严格排除 marker 自身,
        // 且比较用完整键(base+escape),不能拿裸 marker 与完整键比较。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 无游标:前 2 个
        let p = s.list_objects_page("b1", "", None, None, 2).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz"]);
        assert!(p.truncated);
        // Marker=baz → 严格大于:foo,quxx(不得含 baz)
        let p = s
            .list_objects_page("b1", "", None, Some("baz"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        assert!(!p.truncated);
        // 不存在的 marker(blah)→ bar..foo 之间:foo,quxx
        let p = s
            .list_objects_page("b1", "", None, Some("blah"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        // marker 超出列表 → 空
        let p = s
            .list_objects_page("b1", "", None, Some("zzz"), 100)
            .unwrap();
        assert!(p.items.is_empty() && !p.truncated);
        // 含分隔符键的游标同样严格
        s.commit_object_put(
            "b1",
            "a/b",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let p = s
            .list_objects_page("b1", "", None, Some("a/b"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz", "foo", "quxx"]);
    }

    #[test]
    fn list_page_cursor_is_last_emitted() {
        // 回归:s3-tests test_bucket_listv2_continuationtoken — 截断页的
        // 游标必须是最后发出的条目,而非首个未发键(否则续页跳一条)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let p = s.list_objects_page("b1", "", None, None, 1).unwrap();
        assert_eq!(p.last_scanned.as_deref(), Some("bar"));
        assert!(p.truncated);
        // 续页:严格大于 bar → baz,foo,quxx 全量,无跳漏
        let p = s
            .list_objects_page("b1", "", None, Some("bar"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["baz", "foo", "quxx"]);
        assert!(!p.truncated);
    }

    #[test]
    fn list_page_cursor_with_delimiter_is_common_prefix() {
        // 回归:s3-tests test_bucket_list_delimiter_prefix — 截断页最后条目
        // 为公共前缀时,游标 = 公共前缀串(AWS NextMarker 语义)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in [
            "asdf",
            "boo/bar",
            "boo/baz/xyzzy",
            "cquux/thud",
            "cquux/bla",
        ] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 第一页:Contents [asdf],游标 asdf
        let p = s.list_objects_page("b1", "", Some("/"), None, 1).unwrap();
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].0, "asdf");
        assert_eq!(p.last_scanned.as_deref(), Some("asdf"));
        assert!(p.truncated);
        // 第二页:公共前缀 [boo/],游标 = "boo/"(而非键 boo/bar)
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("asdf"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["boo/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("boo/"));
        assert!(p.truncated);
        // 第三页:严格大于 "boo/" → 公共前缀 [cquux/],不再截断
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("boo/"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["cquux/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("cquux/"));
        assert!(!p.truncated);
    }

    #[test]
    fn delimiter_key_equals_common_prefix_is_prefix() {
        // M9/②组:test_bucket_list_delimiter_not_skip_special —— 键 "0/"
        // (以 delimiter 结尾)不作为 Contents,按公共前缀归组;
        // 键 "1999#"/"1999+" 等特殊字符键不得被跳过。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["0/", "0/1000", "0/1998", "1999", "1999#", "1999+", "2000"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let p = s
            .list_objects_page("b1", "", Some("/"), None, 1000)
            .unwrap();
        let keys: Vec<String> = p.items.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, ["1999", "1999#", "1999+", "2000"]);
        assert_eq!(p.common_prefixes, ["0/"]);
        assert!(!p.truncated);
    }

    #[test]
    fn bucket_legacy_value_dual_read() {
        // M9/C5:v1.0.0 桶值(无 created_with_acl)经 decode_bucket 双读
        // 解码为 false;新格式往返保持。
        let (_d, s) = open_tmp();
        #[derive(serde::Serialize)]
        struct LegacyBucket {
            created: i64,
            owner: String,
            stats: fs3_core::BucketStats,
            quota: Option<u64>,
        }
        let legacy = LegacyBucket {
            created: 1,
            owner: "u".into(),
            stats: fs3_core::BucketStats::default(),
            quota: None,
        };
        s.db.put(bucket_key("b1"), postcard::to_allocvec(&legacy).unwrap())
            .unwrap();
        let m = s.get_bucket("b1").unwrap().unwrap();
        assert!(!m.created_with_acl);
        assert_eq!(m.owner, "u");
        // M10/ADR-11:存量值回退补默认(未版本化)
        assert_eq!(m.versioning, fs3_core::VersioningState::Off);
        assert_eq!(m.default_encryption, None);
        assert!(!m.object_lock);
        // 新格式往返
        let mut m2 = m.clone();
        m2.created_with_acl = true;
        s.commit_bucket_put("b1", &m2).unwrap();
        assert!(s.get_bucket("b1").unwrap().unwrap().created_with_acl);
        assert_eq!(s.get_bucket("b1").unwrap().unwrap().owner, "u");
        // M10 起写入恒带版本字节
        let raw = s.db.get(bucket_key("b1")).unwrap().unwrap();
        assert_eq!(raw[0], fs3_core::BUCKET_META_VERSION);
    }

    #[test]
    fn bucket_v1_1_value_dual_read() {
        // M10/ADR-11:v1.1.0 桶值(五字段含 created_with_acl,无版本字节)
        // 双读保留该字段,v2 尾部字段补默认。
        let (_d, s) = open_tmp();
        #[derive(serde::Serialize)]
        struct BucketMetaV1 {
            created: i64,
            owner: String,
            stats: fs3_core::BucketStats,
            quota: Option<u64>,
            created_with_acl: bool,
        }
        let v11 = BucketMetaV1 {
            created: 1_724_155_200,
            owner: "u".into(),
            stats: fs3_core::BucketStats::default(),
            quota: Some(7),
            created_with_acl: true,
        };
        s.db.put(bucket_key("b1"), postcard::to_allocvec(&v11).unwrap())
            .unwrap();
        let m = s.get_bucket("b1").unwrap().unwrap();
        assert!(m.created_with_acl, "v1.1.0 尾部字段不得丢失");
        assert_eq!(m.quota, Some(7));
        assert_eq!(m.versioning, fs3_core::VersioningState::Off);
        assert_eq!(m.default_encryption, None);
        assert!(!m.object_lock);
    }

    #[test]
    fn object_delete_current_writes_delete_marker() {
        // ADR-11 D3/§3.4.3:删除当前版本 = 写删除标记条目(版本键),
        // 不触碰数据段,统计零 delta(未版本化键不受影响)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(100),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 100,
            },
        )
        .unwrap();
        let vk_dm = [0x11u8; 16];
        s.commit_object_delete_current(
            "b1",
            "k",
            Some(&vk_dm),
            &delete_marker(Some(vk_dm)),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // 标记条目落在版本键,原样可读;未版本化条目不动(引擎 V2 才分叉,
        // 此处只验证 meta 层机制)
        let raw =
            s.db.get(object_version_key("b1", "k", &vk_dm))
                .unwrap()
                .unwrap();
        let marker = decode_object(&raw).unwrap();
        assert!(marker.is_delete_marker);
        assert_eq!(marker.version_id, Some(vk_dm));
        assert_eq!(marker.size, 0);
        assert!(marker.extents.is_empty() && marker.inline.is_none());
        assert!(s.get_object("b1", "k").unwrap().is_some());
        // 零 delta:统计不变
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 100));
        // 契约校验:非删除标记/带数据 → InvalidArgument
        let mut not_marker = delete_marker(Some([0x22; 16]));
        not_marker.is_delete_marker = false;
        assert!(matches!(
            s.commit_object_delete_current(
                "b1",
                "k",
                Some(&[0x22; 16]),
                &not_marker,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        let mut with_data = delete_marker(Some([0x33; 16]));
        with_data.size = 1;
        assert!(matches!(
            s.commit_object_delete_current(
                "b1",
                "k",
                Some(&[0x33; 16]),
                &with_data,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        // 桶不存在 → NotFound
        assert!(matches!(
            s.commit_object_delete_current(
                "nope",
                "k",
                Some(&[0x44; 16]),
                &delete_marker(Some([0x44; 16])),
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn object_delete_version_removes_entry() {
        // ADR-11 §3.4.3:物理删除指定版本 = 删版本键 + 同事务 release/扣减。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let vk1 = [0x22u8; 16];
        // 预置一个版本条目 + 入账(模拟引擎版本写路径的 meta 侧形态)
        s.db.put(
            object_version_key("b1", "k", &vk1),
            object_meta(100).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 100,
            },
        }])
        .unwrap();
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk1,
            AllocDraft {
                ref_dec: vec![7],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -100,
            },
        )
        .unwrap();
        assert!(s
            .db
            .get(object_version_key("b1", "k", &vk1))
            .unwrap()
            .is_none());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (0, 0));
        // 释放记录同事务生成
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].ref_dec, vec![7]);
        // 版本不存在 → NotFound(幂等 204 由引擎/协议层映射)
        assert!(matches!(
            s.commit_object_delete_version(
                "b1",
                "k",
                &[0x99; 16],
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn versioned_stats_five_paths() {
        // ADR-11 D5:bytes/objects = 全部非删除标记版本;删除标记不计入。
        // 入账 5 路径(put/complete/copy/delete-version/delete-marker)逐条断言。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let stats = |s: &MetaStore| {
            let b = s.get_bucket("b1").unwrap().unwrap();
            (b.stats.objects, b.stats.bytes)
        };

        // 1) put:+1/+100
        s.commit_object_put(
            "b1",
            "a",
            &object_meta(100),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 100,
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (1, 100));

        // 2) multipart complete:+1/+200
        let uid = "up-1";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let part = PartMeta {
            size: 200,
            etag: [1u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![0u8; 200]),
            checksum: None,
            sse: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        let mut mm = object_meta(200);
        mm.parts = vec![200];
        s.complete_multipart(
            "b1",
            "m",
            uid,
            &mm,
            &[part_key(uid, 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 200,
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (2, 300));

        // 3) copy(与 put 同入账点 commit_object_put):+1/+50
        s.commit_object_put(
            "b1",
            "c",
            &object_meta(50),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 50,
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350));

        // 4) delete-marker:零 delta,统计不变;条目落在版本键
        let vk_dm = [0x11u8; 16];
        s.commit_object_delete_current(
            "b1",
            "a",
            Some(&vk_dm),
            &delete_marker(Some(vk_dm)),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "删除标记零 delta");

        // 5) delete-version:按版本 size 扣减;删除标记版本删除零 delta
        let vk1 = [0x22u8; 16];
        s.db.put(
            object_version_key("b1", "a", &vk1),
            object_meta(100).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 100,
            },
        }])
        .unwrap();
        assert_eq!(stats(&s), (4, 450));
        s.commit_object_delete_version(
            "b1",
            "a",
            &vk1,
            AllocDraft::default(),
            StatsDelta {
                objects: -1,
                bytes: -100,
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "delete-version 扣减后配额占用下降");
        s.commit_object_delete_version(
            "b1",
            "a",
            &vk_dm,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "删除标记版本不计 bytes/objects");

        // Suspended 覆盖 null 槽:删除标记原地覆盖旧 null 版本,
        // 旧 null 版本扣减由调用方(引擎)计算,同事务入账
        s.db.put(
            object_version_key("b1", "n", &VK_NULL),
            object_meta(80).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 80,
            },
        }])
        .unwrap();
        assert_eq!(stats(&s), (4, 430));
        s.commit_object_delete_current(
            "b1",
            "n",
            Some(&VK_NULL),
            &delete_marker(None),
            AllocDraft {
                ref_dec: vec![9],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -80,
            },
        )
        .unwrap();
        // null 槽覆盖的段释放记录(ref_dec)与扣减同事务落盘
        let recs = s.list_alloc_records(0).unwrap();
        assert!(recs.iter().any(|r| r.ref_dec == vec![9]));
        assert_eq!(
            stats(&s),
            (3, 350),
            "null 槽覆盖:旧 null 版本扣减、标记零 delta"
        );
        // 原槽位现为删除标记条目(原地覆盖,version_id = None)
        let raw =
            s.db.get(object_version_key("b1", "n", &VK_NULL))
                .unwrap()
                .unwrap();
        let marker = decode_object(&raw).unwrap();
        assert!(marker.is_delete_marker);
        assert_eq!(marker.version_id, None);
    }

    // ─────────────────── 版本化读路径(ADR-11 D1/D4;V2) ───────────────────

    /// 时间戳分量 vk 构造(测试用;随机分量固定 0x07)。
    fn vk_at(ts: u64) -> [u8; 16] {
        // 夹具 vk 时间戳分量按 **微秒** 编码(与 new_version_vk 布局一致):
        // ts 与 ObjectMeta.mtime(秒)同值可比(V6-1 起 D1a 裁决按 vk 秒分量)
        let mut v = [0x07u8; 16];
        v[..8].copy_from_slice(&(ts * 1_000_000).to_be_bytes());
        v
    }

    fn versioned_bucket(name: &str, state: fs3_core::VersioningState) -> BucketMeta {
        BucketMeta {
            versioning: state,
            ..bucket_meta(name)
        }
    }

    #[test]
    fn object_put_version_op_and_exact_read() {
        // ADR-11 D1:版本化 PUT 落版本键;精确读;契约校验。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let vk1 = vk_at(100);
        let mut m = object_meta(64);
        m.version_id = Some(vk1);
        s.commit_object_put_version(
            "b1",
            "k",
            &vk1,
            &m,
            AllocDraft {
                alloc: vec![(3, 1)],
                ..Default::default()
            },
            StatsDelta {
                objects: 1,
                bytes: 64,
            },
        )
        .unwrap();
        // 版本键可读;未版本化键无条目
        assert!(s.get_object("b1", "k").unwrap().is_none());
        let got = s.get_object_version("b1", "k", &vk1).unwrap().unwrap();
        assert_eq!(got.version_id, Some(vk1));
        assert_eq!(got.size, 64);
        // 分配记录同事务可见
        assert!(s
            .list_alloc_records(0)
            .unwrap()
            .iter()
            .any(|r| r.alloc == vec![(3, 1)]));
        // 桶不存在 → NotFound;删除标记须经 DeleteCurrent → InvalidArgument
        assert!(matches!(
            s.commit_object_put_version(
                "nope",
                "k",
                &vk1,
                &m,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            s.commit_object_put_version(
                "b1",
                "k",
                &vk_at(200),
                &delete_marker(Some(vk_at(200))),
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        // 同 vk 覆盖(Suspended null 槽原地覆盖形态):值被替换
        let mut m2 = object_meta(32);
        m2.version_id = Some(vk1);
        s.commit_object_put_version(
            "b1",
            "k",
            &vk1,
            &m2,
            AllocDraft::default(),
            StatsDelta {
                objects: 0,
                bytes: -32,
            },
        )
        .unwrap();
        assert_eq!(
            s.get_object_version("b1", "k", &vk1).unwrap().unwrap().size,
            32
        );
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 32));
    }

    #[test]
    fn current_version_resolution_and_versions_listing() {
        // ADR-11 D4:当前版本 = 最大 vk 条目(删除标记亦为当前版本);
        // 全版本列举 vk 升序;null 槽恒最大;反扫上界键不干扰。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // k:两个数据版本 + 一条新删除标记(最大 vk)
        for (t, sz) in [(100u64, 10u64), (200, 20)] {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                "k",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        s.commit_object_delete_current(
            "b1",
            "k",
            Some(&vk_at(300)),
            &delete_marker(Some(vk_at(300))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (cur_vk, cur) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(cur_vk, vk_at(300));
        assert!(cur.is_delete_marker, "最大 vk 条目原样返回,调用方判定标记");
        // 全版本列举:vk 升序(= 创建序)
        let all = s.list_key_versions("b1", "k").unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(all[1].1.size, 20);
        // 无版本 key → None
        assert!(s.get_current_version("b1", "ghost").unwrap().is_none());
        assert!(s.list_key_versions("b1", "ghost").unwrap().is_empty());
        // 反扫上界碰撞:真实对象键 `o:b1\0k\x01`(恰为上界值)不得干扰解析
        s.commit_object_put(
            "b1",
            "k\u{1}",
            &object_meta(5),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (cur_vk2, _) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(cur_vk2, vk_at(300), "上界键不得干扰最大 vk 反扫");
        // null 槽恒为当前(键序最大)
        s.commit_object_put_version(
            "b1",
            "n",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(
            s.get_current_version("b1", "n").unwrap().unwrap().0,
            VK_NULL
        );
        // 全条目枚举:双形态(3 版本 + null 槽 + 1 未版本化)
        let entries = s.list_object_entries("b1").unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries.iter().filter(|(_, vk, _)| vk.is_some()).count(), 4);
        assert!(entries
            .iter()
            .any(|(k, vk, _)| k == "k\u{1}" && vk.is_none()));
        // 快照扫描双形态:版本条目带 vk,恢复可达性可见(§3.4.6)
        let snap = s.snapshot_all_objects().unwrap();
        assert_eq!(snap.len(), 5);
        assert_eq!(snap.iter().filter(|(_, _, vk, _)| vk.is_some()).count(), 4);
    }

    /// M11 L2-2:分页条目扫描——游标严格大于续扫、双形态、跨页不重复不
    /// 遗漏;游标条目被并发删除时从其后最近条目续扫。
    #[test]
    fn scan_object_entries_page_cursor() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // k1:遗留单键;k2:两版本 + null 槽;k3:单版本(页边界跨组)
        s.commit_object_put(
            "b1",
            "k1",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        for t in [100u64, 200] {
            let mut m = object_meta(t);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                "k2",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        s.commit_object_put_version(
            "b1",
            "k2",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let mut m3 = object_meta(3);
        m3.version_id = Some(vk_at(300));
        s.commit_object_put_version(
            "b1",
            "k3",
            &vk_at(300),
            &m3,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let all = s.list_object_entries("b1").unwrap();
        assert_eq!(all.len(), 5);
        // limit=2 逐页扫:拼接结果必须与全量枚举逐条一致
        let mut cursor: Option<(String, Option<[u8; 16]>)> = None;
        let mut paged: Vec<ObjectEntry> = Vec::new();
        loop {
            let (page, done) = s
                .scan_object_entries_page("b1", cursor.as_ref(), 2)
                .unwrap();
            assert!(page.len() <= 2);
            cursor = page.last().map(|(k, vk, _)| (k.clone(), *vk));
            paged.extend(page);
            if done {
                break;
            }
        }
        assert_eq!(paged.len(), all.len());
        for (a, b) in paged.iter().zip(all.iter()) {
            assert_eq!((&a.0, a.1), (&b.0, b.1));
        }
        // 游标条目并发删除:从其后最近条目续扫(删除 k2 中间版本,
        // 以它为游标续扫不得重复/遗漏)
        s.commit_object_delete_version(
            "b1",
            "k2",
            &vk_at(200),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (page, done) = s
            .scan_object_entries_page("b1", Some(&("k2".to_string(), Some(vk_at(200)))), 8)
            .unwrap();
        assert!(done);
        let keys: Vec<(&str, Option<[u8; 16]>)> =
            page.iter().map(|(k, vk, _)| (k.as_str(), *vk)).collect();
        assert_eq!(keys, vec![("k2", Some(VK_NULL)), ("k3", Some(vk_at(300)))]);
    }

    #[test]
    fn current_version_for_off_fast_path_equivalence() {
        // F-1:Off 桶快速路径 = 未版本化单键点读(vk 恒 VK_NULL),与全量
        // D1a 裁决逐值等价(Off 桶绝不可能存在版本键);Enabled/Suspended
        // 的 _for 与全量同路,行为不变。
        let (_d, s) = open_tmp();
        // —— Off 桶:存在键 → (VK_NULL, meta);不存在键 → None(404 源)——
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(42),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let full = s.get_current_version("b1", "k").unwrap();
        let fast = s
            .get_current_version_for("b1", "k", fs3_core::VersioningState::Off)
            .unwrap();
        assert_eq!(fast, full);
        assert_eq!(fast.unwrap().0, VK_NULL);
        assert!(s
            .get_current_version_for("b1", "ghost", fs3_core::VersioningState::Off)
            .unwrap()
            .is_none());
        // —— Enabled 桶:_for 与全量 D1a 同路(真实版本裁决不变)——
        s.commit_bucket_put(
            "b2",
            &versioned_bucket("b2", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        for (t, sz) in [(100u64, 10u64), (200, 20)] {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b2",
                "k",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let full = s.get_current_version("b2", "k").unwrap();
        assert_eq!(
            s.get_current_version_for("b2", "k", fs3_core::VersioningState::Enabled)
                .unwrap(),
            full
        );
        assert_eq!(full.unwrap().0, vk_at(200));
        // —— Suspended 桶:null 槽裁决同路 ——
        s.commit_bucket_put(
            "b3",
            &versioned_bucket("b3", fs3_core::VersioningState::Suspended),
        )
        .unwrap();
        s.commit_object_put_version(
            "b3",
            "k",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let full = s.get_current_version("b3", "k").unwrap();
        assert_eq!(
            s.get_current_version_for("b3", "k", fs3_core::VersioningState::Suspended)
                .unwrap(),
            full
        );
        assert_eq!(full.unwrap().0, VK_NULL);
    }

    #[test]
    fn list_page_versioned_current_only_filter() {
        // ADR-11 §3.4.4:版本化桶 ListObjects 每 key 只出当前版本;当前 =
        // 删除标记则该 key 不出现;游标/delimiter 语义不变。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        let put_v = |s: &MetaStore, key: &str, t: u64, sz: u64| {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                key,
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        };
        // a:两个版本,当前 = vk(200) 数据版本
        put_v(&s, "a", 100, 1);
        put_v(&s, "a", 200, 2);
        // b:当前 = 删除标记 → 隐藏
        put_v(&s, "b", 100, 1);
        s.commit_object_delete_current(
            "b1",
            "b",
            Some(&vk_at(200)),
            &delete_marker(Some(vk_at(200))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // dir/x 当前数据;dir/y 当前 = 标记(delimiter 分组仍出 dir/)
        put_v(&s, "dir/x", 100, 3);
        put_v(&s, "dir/y", 100, 4);
        s.commit_object_delete_current(
            "b1",
            "dir/y",
            Some(&vk_at(200)),
            &delete_marker(Some(vk_at(200))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();

        // 全量:仅当前数据版本;a 输出的是 vk(200) 的值
        let p = s.list_objects_page("b1", "", None, None, 100).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a", "dir/x"]);
        assert_eq!(p.items[0].1.size, 2, "输出当前版本(最新)的 meta");
        assert!(!p.truncated);
        // delimiter:dir/ 归组(组内有 key 当前为标记不影响组的发出)
        let p = s.list_objects_page("b1", "", Some("/"), None, 100).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a"]);
        assert_eq!(p.common_prefixes, ["dir/"]);
        // 分页:max=1 逐页,游标续扫不重不漏
        let p1 = s.list_objects_page("b1", "", None, None, 1).unwrap();
        assert_eq!(p1.items.len(), 1);
        assert!(p1.truncated);
        let p2 = s
            .list_objects_page("b1", "", None, p1.last_scanned.as_deref(), 10)
            .unwrap();
        let mut all: Vec<String> = p1.items.iter().map(|(k, _)| k.clone()).collect();
        all.extend(p2.items.iter().map(|(k, _)| k.clone()));
        assert_eq!(all, ["a", "dir/x"]);
        assert!(!p2.truncated);
        // list_objects 同步过滤
        let all = s.list_objects("b1", "").unwrap();
        let keys: Vec<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a", "dir/x"]);
        // Off 桶对照:同内容全量输出(旧路径零改动)
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        s.commit_object_put(
            "b2",
            "a",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.commit_object_put(
            "b2",
            "b",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(s.list_objects("b2", "").unwrap().len(), 2);
        assert_eq!(
            s.list_objects_page("b2", "", None, None, 100)
                .unwrap()
                .items
                .len(),
            2
        );
    }

    #[test]
    fn complete_multipart_version_lands_version_key() {
        // ADR-11 §3.4.5:Complete 落版本键(vk = Some);None 退化为未版本化
        // 单键(与 complete_multipart 委托路径一致)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mk_part = |uid: &str| {
            s.create_multipart(
                uid,
                &MultipartSession::new(
                    "b1",
                    "m",
                    "text/x",
                    vec![],
                    vec![],
                    vec![],
                    None,
                    None,
                    None,
                ),
            )
            .unwrap();
            let part = PartMeta {
                size: 5,
                etag: [1u8; 16],
                mtime: 1,
                extents: vec![],
                inline: Some(vec![0u8; 5]),
                checksum: None,
                sse: None,
            };
            s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
            part
        };
        let mut mm = object_meta(5);
        mm.parts = vec![5];
        // vk = Some:版本键
        let vk1 = vk_at(100);
        let mut vm = mm.clone();
        vm.version_id = Some(vk1);
        s.complete_multipart_version(
            "b1",
            "m",
            "up-v",
            Some(&vk1),
            &vm,
            &[part_key("up-v", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
            },
        )
        .unwrap_err();
        // 会话未创建 → NotFound;先建会话再 Complete
        mk_part("up-v");
        s.complete_multipart_version(
            "b1",
            "m",
            "up-v",
            Some(&vk1),
            &vm,
            &[part_key("up-v", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
            },
        )
        .unwrap();
        assert!(s.get_object("b1", "m").unwrap().is_none());
        assert_eq!(
            s.get_object_version("b1", "m", &vk1).unwrap().unwrap().size,
            5
        );
        assert!(s.get_multipart("up-v").unwrap().unwrap().completed);
        assert!(s.list_parts("up-v").unwrap().is_empty());
        // vk = None:未版本化单键(等价 complete_multipart)
        mk_part("up-p");
        s.complete_multipart_version(
            "b1",
            "p",
            "up-p",
            None,
            &mm,
            &[part_key("up-p", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
            },
        )
        .unwrap();
        assert_eq!(s.get_object("b1", "p").unwrap().unwrap().size, 5);
    }

    // ─────────────── D1a 跨状态转换(ADR-11 D1a;V3-0) ───────────────

    /// 预置遗留未版本化单键(Off 时代形态):自定 mtime。
    fn put_legacy(s: &MetaStore, bucket: &str, key: &str, mtime: i64, size: u64) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        s.commit_object_put(
            bucket,
            key,
            &m,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
    }

    /// 预置真实版本条目:vk_at(ts),mtime 独立指定。
    fn put_real(s: &MetaStore, bucket: &str, key: &str, ts: u64, mtime: i64, size: u64) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        m.version_id = Some(vk_at(ts));
        s.commit_object_put_version(
            bucket,
            key,
            &vk_at(ts),
            &m,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
    }

    /// 预置 null 槽条目(数据版本;is_delete_marker 由参数定)。
    fn put_null_slot(s: &MetaStore, bucket: &str, key: &str, mtime: i64, size: u64, marker: bool) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        if marker {
            m.is_delete_marker = true;
            m.size = 0;
            s.commit_object_delete_current(
                bucket,
                key,
                Some(&VK_NULL),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        } else {
            s.commit_object_put_version(
                bucket,
                key,
                &VK_NULL,
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
    }

    #[test]
    fn d1a_current_version_legacy_vs_real() {
        // Off→Enabled 遗留键遮蔽回归(D1a-2):遗留单键与真实版本共存时
        // 当前 = mtime 最大;相等取真实版本;删掉真实版本后遗留单键回升。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_legacy(&s, "b1", "k", 100, 10);
        // 仅遗留单键:当前 = 遗留(VK_NULL 展示)
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 10));
        // 写入真实版本(mtime 更大)→ 真实版本为当前(遗留被遮蔽)
        put_real(&s, "b1", "k", 200, 200, 20);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(200), 20));
        // 候选 = 遗留 vs **最大真实 vk**(D1a 候选语义;非键内最大 mtime):
        // vk_at(300) 为最大真实 vk,与遗留同 mtime → tie 取真实版本
        put_real(&s, "b1", "k", 300, 100, 30);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "tie:最大真实 vk 胜出");
        // 真实版本全部删除 → 遗留单键回升为当前
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk_at(200),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk_at(300),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 10));
        // tie 专项:真实 vk 秒分量 == 遗留单键 mtime(100)→ 打平取真实版本
        // (V6-1 比较键 = vk 时间戳分量,写侧保序下打平 ⟺ 真实版本后写)
        put_real(&s, "b1", "k", 100, 100, 40);
        let (vk, _) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(vk, vk_at(100), "打平取真实版本");
    }

    #[test]
    fn d1a_current_version_null_slot_vs_real() {
        // Suspended→Enabled null 槽遮蔽回归(D1a-2):null 槽与真实版本共存,
        // mtime 裁决;重启用后的新真实版本(同秒)胜出。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_real(&s, "b1", "k", 100, 100, 10);
        // Suspended 时代 null 槽写入(mtime 更晚)→ null 族为当前
        put_null_slot(&s, "b1", "k", 200, 20, false);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 20));
        // 重启用后写入(vk 秒分量更大)→ 真实版本胜出
        put_real(&s, "b1", "k", 300, 200, 30);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "vk 更新:真实版本胜出");
        // tie 专项:null 槽 mtime == 最大真实 vk 秒分量 → 打平取真实版本
        // (写侧保序下打平 ⟺ 真实版本后写)
        put_null_slot(&s, "b1", "k", 300, 25, false);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "tie:真实版本仍为当前");
        // null 槽为删除标记:mtime 最大 → 当前 = 标记(调用方判 404)
        put_null_slot(&s, "b1", "k", 400, 0, true);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(vk, VK_NULL);
        assert!(m.is_delete_marker);
        // vk 防回拨比较基址(D1a-5):不含 null 槽/遗留单键
        assert_eq!(s.max_real_vk("b1", "k").unwrap(), Some(vk_at(300)));
        put_legacy(&s, "b1", "g", 500, 1);
        assert_eq!(s.max_real_vk("b1", "g").unwrap(), None);
    }

    #[test]
    fn d1a_list_objects_current_by_mtime() {
        // 版本化桶 ListObjects/ListPage 的当前版本同走 D1a(组内 mtime 裁决,
        // 非组尾键序):Suspended 覆盖遗留单键后,新真实版本同秒胜出。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_legacy(&s, "b1", "a", 100, 10);
        put_real(&s, "b1", "a", 200, 100, 20); // 同 mtime:真实版本为当前
        put_real(&s, "b1", "b", 100, 100, 30);
        put_null_slot(&s, "b1", "b", 200, 40, false); // null 槽更晚:当前
        let all = s.list_objects("b1", "").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1.size, 20, "key a:同秒真实版本胜出");
        assert_eq!(all[1].1.size, 40, "key b:null 槽 mtime 最大");
        let page = s.list_objects_page("b1", "", None, None, 10).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].1.size, 20);
        assert_eq!(page.items[1].1.size, 40);
    }

    #[test]
    fn object_delete_current_legacy_inplace() {
        // D1a-1 meta 机制:vk=None → 删除标记写未版本化单键(原地覆盖);
        // 契约校验同样生效。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        put_legacy(&s, "b1", "k", 100, 10);
        s.commit_object_delete_current(
            "b1",
            "k",
            None,
            &delete_marker(None),
            AllocDraft::default(),
            StatsDelta {
                objects: -1,
                bytes: -10,
            },
        )
        .unwrap();
        let m = s.get_object("b1", "k").unwrap().unwrap();
        assert!(m.is_delete_marker && m.version_id.is_none());
        assert!(s.list_key_versions("b1", "k").unwrap().is_empty());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (0, 0));
    }

    #[test]
    fn bucket_set_versioning_txn() {
        // V3-1:单事务读改写;location/统计等其余字段不动;桶不存在 → NotFound。
        let (_d, s) = open_tmp();
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "eu-west-1")
            .unwrap();
        s.commit_bucket_set_versioning("b1", fs3_core::VersioningState::Enabled)
            .unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.versioning, fs3_core::VersioningState::Enabled);
        assert_eq!(s.bucket_location("b1").unwrap(), "eu-west-1");
        s.commit_bucket_set_versioning("b1", fs3_core::VersioningState::Suspended)
            .unwrap();
        assert_eq!(
            s.get_bucket("b1").unwrap().unwrap().versioning,
            fs3_core::VersioningState::Suspended
        );
        assert!(matches!(
            s.commit_bucket_set_versioning("nope", fs3_core::VersioningState::Enabled),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn bucket_set_object_lock_enables_versioning() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let def = fs3_core::ObjectLockDefaultRetention {
            mode: fs3_core::RetentionMode::Governance,
            unit: fs3_core::RetentionPeriodUnit::Days,
            n: 7,
        };
        s.commit_bucket_set_object_lock("b1", Some(def.clone()))
            .unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert!(b.object_lock);
        assert_eq!(b.versioning, fs3_core::VersioningState::Enabled);
        assert_eq!(b.default_retention, Some(def));
        s.commit_bucket_set_object_lock("b1", None).unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert!(b.object_lock, "Enabled 不可关闭");
        assert_eq!(b.default_retention, None);
        assert!(matches!(
            s.commit_bucket_set_object_lock("nope", None),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn list_versions_page_full_semantics() {
        // ADR-11 §3.4.4 + D1a-3:键升序、键内 mtime 降序;IsLatest 按 D1a;
        // KeyMarker/VersionIdMarker 条目级续传;delimiter 归组。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // key "a":3 个真实版本(创建序 vk 100<200<300)+ 1 删除标记(最新)
        for (ts, sz) in [(100u64, 1u64), (200, 2), (300, 3)] {
            put_real(&s, "b1", "a", ts, ts as i64, sz);
        }
        let mut dm = delete_marker(Some(vk_at(400)));
        dm.mtime = 400;
        s.commit_object_delete_current(
            "b1",
            "a",
            Some(&vk_at(400)),
            &dm,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // key "b":真实版本 + null 槽(null 槽 mtime 居中 → 按 mtime 插入序列)
        put_real(&s, "b1", "b", 100, 100, 10);
        put_null_slot(&s, "b1", "b", 150, 20, false);
        put_real(&s, "b1", "b", 200, 200, 30);
        // key "d/x"、"d/y"(delimiter 归组用)
        put_real(&s, "b1", "d/x", 100, 100, 1);
        put_real(&s, "b1", "d/y", 100, 100, 1);

        let page = s
            .list_versions_page("b1", "", None, None, None, 100)
            .unwrap();
        assert!(!page.truncated);
        // 键内 mtime 降序:a = [标记(400), v300, v200, v100]
        let a: Vec<&VersionListEntry> = page.entries.iter().filter(|e| e.key == "a").collect();
        assert_eq!(a.len(), 4);
        assert!(
            a[0].meta.is_delete_marker && a[0].is_latest,
            "最新 = 删除标记"
        );
        assert_eq!(a[1].vk, vk_at(300));
        assert!(!a[1].is_latest);
        assert_eq!(a[3].vk, vk_at(100));
        // b = [v200, null(150), v100]:null 按 mtime 插入,不按键位
        let b: Vec<&VersionListEntry> = page.entries.iter().filter(|e| e.key == "b").collect();
        assert_eq!(b.len(), 3);
        assert_eq!(b[0].vk, vk_at(200));
        assert!(b[0].is_latest, "v200 mtime 最大 = 当前");
        assert_eq!(b[1].vk, VK_NULL, "null 条目按 mtime 插入真实版本序列");
        assert_eq!(b[2].vk, vk_at(100));

        // 分页:max=3 → 截断;a 组内 VersionIdMarker 续传不重不漏
        let p1 = s.list_versions_page("b1", "", None, None, None, 3).unwrap();
        assert!(p1.truncated);
        assert_eq!(p1.entries.len(), 3);
        let (lk, lvk) = p1.last_scanned.clone().unwrap();
        assert_eq!((lk.as_str(), lvk), ("a", Some(vk_at(200))));
        let p2 = s
            .list_versions_page("b1", "", None, Some("a"), lvk.as_ref(), 100)
            .unwrap();
        assert!(!p2.truncated);
        let mut resumed: Vec<([u8; 16], &str)> =
            p2.entries.iter().map(|e| (e.vk, e.key.as_str())).collect();
        // p2 首条 = a 的 v100(接着 p1 末条 v200),随后 b 组 3 条 + d 组 2 条
        assert_eq!(resumed.remove(0), (vk_at(100), "a"));
        assert_eq!(resumed.len(), 5);
        // 全量 = p1 + p2,无重叠无遗漏
        let total: Vec<([u8; 16], &str)> = p1
            .entries
            .iter()
            .chain(p2.entries.iter())
            .map(|e| (e.vk, e.key.as_str()))
            .collect();
        assert_eq!(total.len(), 4 + 3 + 2);

        // delimiter:d/ 组折叠为公共前缀;d 组版本条目不出现
        let pd = s
            .list_versions_page("b1", "", Some("/"), None, None, 100)
            .unwrap();
        assert_eq!(pd.common_prefixes, vec!["d/".to_string()]);
        assert!(pd.entries.iter().all(|e| !e.key.starts_with("d/")));
        // 公共前缀续传:key_marker = "d/" → d 组整组跳过
        let pd2 = s
            .list_versions_page("b1", "", Some("/"), Some("d/"), None, 100)
            .unwrap();
        assert!(pd2.entries.is_empty() && pd2.common_prefixes.is_empty());
    }

    #[test]
    fn list_versions_page_unversioned_stub_compat() {
        // 未版本化桶(Off)走同一实现:每对象一条 VersionId="null"、
        // IsLatest=true(现状桩语义,s3-tests nuke_bucket 依赖);key_marker
        // 分页与 max-keys 截断一致。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for (k, sz) in [("a", 1u64), ("b", 2), ("c", 3)] {
            put_legacy(&s, "b1", k, 100, sz);
        }
        let page = s
            .list_versions_page("b1", "", None, None, None, 100)
            .unwrap();
        assert_eq!(page.entries.len(), 3);
        assert!(page
            .entries
            .iter()
            .all(|e| e.vk == VK_NULL && e.is_latest && !e.meta.is_delete_marker));
        let p1 = s.list_versions_page("b1", "", None, None, None, 2).unwrap();
        assert!(p1.truncated && p1.entries.len() == 2);
        let (lk, lvk) = p1.last_scanned.clone().unwrap();
        assert_eq!((lk.as_str(), lvk), ("b", Some(VK_NULL)));
        let p2 = s
            .list_versions_page("b1", "", None, Some("b"), lvk.as_ref(), 100)
            .unwrap();
        assert!(!p2.truncated);
        assert_eq!(p2.entries.len(), 1);
        assert_eq!(p2.entries[0].key, "c");
    }
}
