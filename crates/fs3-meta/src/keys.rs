//! 键编码与转义(DESIGN §4.4)。
//!
//! 单树统一前缀 + 转义:
//! - `b:{bucket}` 桶元数据
//! - `o:{bucket}\0{key}` 对象元数据(key 转义后)
//! - `o:{bucket}\0{key}\0{vk16}` 对象版本元数据(ADR-11 D1;仅版本化桶)
//! - `u:{bucket}\0{key}\0{uploadId}` 分片上传会话(M2)
//! - `a:{seq be64}` 分配器变更记录
//! - `t:{seq be64}` 事务提交标记
//! - `s:seq` 系统单调计数器(ADR-5 新增内部键)
//! - `k:{access_key}` 访问密钥(M3 密钥 CRUD,secret 哈希存储)
//! - `bc:{bucket}` 桶级 CORS 配置文档(M10 S2;ADR-11 D9,值 = 规范化 XML)
//! - `bt:{bucket}` 桶级标签文档(M10 S1;ADR-11 D8,值 = 规范化 XML)
//! - `bo:{bucket}` 桶级 OwnershipControls 文档(M10 S7,值 = 规范化 XML)
//! - `bp:{bucket}` 桶策略文档(M10 S3;ADR-11 D9,值 = 原始 JSON 文本)
//! - `r:{bucket}\0{rule_id}` 生命周期规则(M11 L1;ADR-12 DL1,值 =
//!   postcard LifecycleRule;单事务整体替换;两段式同 `m:` 先例)
//! - `n:{bucket}\0{rule_id}` 事件通知规则(M15 N1;ADR-18 D-E4,值 =
//!   postcard NotificationRule;单事务整体替换;两段式同 `r:` 先例)
//! - `e:{seq be64}` 事件队列条目(M15 N2;ADR-18 D-E1;值 = postcard
//!   EventRecord;be64 字典序 = 写入序;有界环形批量截断;入队与数据
//!   操作同事务——崩溃零漂移)
//!
//! s: 前缀下的系统键:`s:seq`(单调计数器)、`s:key_seed_salt`(M3)、
//! `s:value_rewrite_v3_done`(M10 V5-3 值格式重写完成标记)、
//! `s:sse_kek_seed` / `s:sse_kek_gen`(M11 K1-1 SSE-S3 KEK 体系)、
//! `s:audit\0{seq be64}` 审计环形条目(M11 L3-1;ADR-12 DL5)、
//! `s:trusted_clock`(M12 W1-1;ADR-13 DL6 可信时钟 wall+mono 对)。
//!
//! 转义规则:0x00 → 0xFF 0x00;0xFF → 0xFF 0xFF;其余原样。
//! 保证 `o:{bucket}\0` 前缀扫描恰好覆盖该桶全部对象。
//!
//! ADR-11 D1 单入口原则:`o:` 键有/无 vk16 后缀的双形态分支**只允许出现
//! 在本文件**(`object_version_key`/`object_key_prefix`/`parse_object_version_key`),
//! 其余模块一律经此解析,不得自行拆键(V4-2 依赖)。

use fs3_core::{Error, Result};

pub const PREFIX_BUCKET: &[u8] = b"b:";
/// 桶 LocationConstraint(M8 回显语义;键 `l:{bucket}`,值 UTF-8 字符串)。
pub const PREFIX_BUCKET_LOC: &[u8] = b"l:";
/// 桶级 CORS 配置文档(M10 S2;ADR-11 D9:配置文档可达数 KB,不并入
/// BucketMeta 值避免桶记录膨胀,沿用 `l:` 独立键先例;值 = 规范化 XML)。
pub const PREFIX_BUCKET_CORS: &[u8] = b"bc:";
/// 桶级标签文档(M10 S1;ADR-11 D8:桶级标签落 D9 桶级配置键)。
pub const PREFIX_BUCKET_TAGGING: &[u8] = b"bt:";
/// 桶级 OwnershipControls 文档(M10 S7;单账号模型下配置存取 + 回显)。
pub const PREFIX_BUCKET_OWNERSHIP: &[u8] = b"bo:";
/// 桶策略文档(M10 S3;ADR-11 D9:桶级配置独立键前缀;值 = 客户端提交的
/// 原始 JSON 文本,GetBucketPolicy 逐字节回显——s3-tests 断言逐字节相等)。
pub const PREFIX_BUCKET_POLICY: &[u8] = b"bp:";
pub const PREFIX_OBJECT: &[u8] = b"o:";
/// 分片上传会话(主键:`u:{uploadId}`,值 = MultipartSession)。
pub const PREFIX_UPLOAD: &[u8] = b"u:";
/// 会话桶索引(`m:{bucket}\0{uploadId}` → 空;ListMultipartUploads 用)。
pub const PREFIX_UPLOAD_INDEX: &[u8] = b"m:";
/// 分片元数据(`p:{uploadId}\0{part_no be32}` → PartMeta)。
pub const PREFIX_PART: &[u8] = b"p:";
pub const PREFIX_ALLOC: &[u8] = b"a:";
pub const PREFIX_TXN: &[u8] = b"t:";
pub const PREFIX_SYS: &[u8] = b"s:";
/// 访问密钥(`k:{access_key}` → KeyRecord;M3)。
pub const PREFIX_KEY: &[u8] = b"k:";
/// 生命周期规则(M11 L1;ADR-12 DL1:`r:{bucket}\0{rule_id}` → postcard
/// LifecycleRule;每条规则一键,规则变更 = 单事务整体替换;删桶事务前缀
/// 扫描清理——两段式桶级键,故不在 BucketConf::ALL 单段式清理列表)。
pub const PREFIX_LIFECYCLE_RULE: &[u8] = b"r:";
/// 事件通知规则(M15 N1;ADR-18 D-E4:`n:{bucket}\0{rule_id}` → postcard
/// NotificationRule;每条规则一键,规则变更 = 单事务整体替换;删桶事务
/// 前缀扫描清理——两段式桶级键,同 `r:` 先例)。
pub const PREFIX_NOTIFICATION: &[u8] = b"n:";
/// 事件队列条目(M15 N2;ADR-18 D-E1:`e:{seq be64}` → postcard
/// EventRecord)。新一级前缀(非 `s:` 系统键):须三处同步——keys.rs
/// 前缀表(本处)、meta-export/import DTO(fs3d/meta.rs)、check 可达性
/// 扫描(只读 `o:`/`p:` 段引用键,对 `e:` 天然安全,登记于注释)。
/// be64 字典序 = 数值序 = 写入序;有界环形(上限可配),批量截断最旧;
/// 入队与触发它的数据操作同事务提交(崩溃零漂移,ADR-18 D-E1)。
pub const PREFIX_EVENT: &[u8] = b"e:";
/// S3 Inventory 配置(M15 I1;`iv:{bucket}\0{id}` → postcard
/// InventoryRule;每配置一键)。新一级前缀:同 `n:` 三处同步(keys.rs
/// 前缀表 + meta-export/import DTO + check 可达性扫描登记);两段式
/// 桶级键入删桶清理(同 `r:`/`n:` 先例)。
pub const PREFIX_INVENTORY: &[u8] = b"iv:";
/// 归档恢复作业队列(M16 A2;ADR-19 DA2.3:`x:{seq be64}` → postcard
/// RestoreJob;be64 字典序 = 入队序;入队与数据操作同事务提交——崩溃
/// 零漂移(挂起标记必在队、作业必可重放)。新一级前缀:三处同步——
/// keys.rs 前缀表(本处)、meta-export/import DTO(不导出:瞬态队列,同
/// `e:` 事件队列口径)、check 可达性扫描(只读 `o:`/`p:` 段引用键,对
/// `x:` 天然安全,登记于注释)。
pub const PREFIX_RESTORE_JOB: &[u8] = b"x:";

/// 系统单调计数器(每个事务 +1,单点序列化;ADR-5)。
pub const SYS_SEQ: &[u8] = b"s:seq";
/// 密钥加密种子盐(64 字节随机;首次启动生成,持久化;密钥派生 + 恢复用)。
pub const SYS_KEY_SEED_SALT: &[u8] = b"s:key_seed_salt";
/// 值格式 v2→v3 在线重写完成标记(M10 V5-3;ADR-11 D0 / DESIGN-FUTURE
/// §2.4:重写完成前禁回滚到 v1.0.x)。s: 既有前缀下的新系统键,不新增前缀,
/// 故 meta-export DTO 与 check 可达性扫描无需联动(§2.2 三处联动仅约束
/// 新前缀)。
pub const SYS_KEY_VALUE_REWRITE_V3_DONE: &[u8] = b"s:value_rewrite_v3_done";
/// 值格式 v6→v7 在线重写完成标记(M16 A1;ADR-19 DA4:重写完成前禁回滚到
/// v2.1.x 二进制——v7 值新二进制才可解,v2.1.x 拒绝解码;与 v3 标记同
/// 一 s: 前缀族,不新增前缀)。
pub const SYS_KEY_VALUE_REWRITE_V7_DONE: &[u8] = b"s:value_rewrite_v7_done";
/// SSE-S3 KEK 种子(M11 K1-1,ADR-12 DS1;64 字节随机,首次需要时生成,
/// 持久化;**与 s:key_seed_salt 访问密钥种子相互独立**)。红线:seed 及
/// 其派生的 KEK/DEK 明文零导出、零日志、永不下发——meta-export DTO 不
/// 含本键(DTO 只导桶/对象/会话类键,s: 系统键不入导出)。s: 既有前缀
/// 系统键,删桶清理列表不受影响(桶级键域不相交)。
pub const SYS_SSE_KEK_SEED: &[u8] = b"s:sse_kek_seed";
/// SSE-S3 当前 KEK 代状态(M11 K1-1;值 = postcard(SseKekGenState),
/// gen 从 1 起,当前代 = 最大代;键缺席 = 初始代 1,惰性不落盘)。
pub const SYS_SSE_KEK_GEN: &[u8] = b"s:sse_kek_gen";
/// 可信时钟状态(M12 W1-1,ADR-13 DL6;值 = postcard TrustedClockState
/// `{last_wall, last_mono_ns}`)。s: 既有前缀下的新系统键,不新增前缀,
/// 故 meta-export DTO 与 check 可达性扫描无需联动(同
/// SYS_KEY_VALUE_REWRITE_V3_DONE 注释口径)。
pub const SYS_TRUSTED_CLOCK: &[u8] = b"s:trusted_clock";
/// 池清单(M13 M1-1,ADR-15 DM1/DM1';值 = postcard(fs3_core::pool::PoolManifest),
/// 设备序 = 数组序,仅尾部增删)。s: 既有前缀下的新系统键,不新增前缀,
/// 故 meta-export DTO 与 check 可达性扫描无需联动(同
/// SYS_KEY_VALUE_REWRITE_V3_DONE 注释口径)。
pub const SYS_POOL: &[u8] = b"s:pool";
/// 审计环形条目前缀(M11 L3-1;ADR-12 DL5):`s:audit\0{seq be64}` →
/// postcard(fs3_core::audit::AuditEntry),每条目一键;be64 字典序 =
/// 写入序(回放取尾、截断删头的扫描边界)。s: 既有前缀下的系统键族,
/// 不新增前缀——meta-export DTO 不导出(s: 系统键不入导出)、check
/// 可达性扫描与删桶清理域不相交,三处联动无需改动(同
/// SYS_KEY_VALUE_REWRITE_V3_DONE 注释口径)。
pub const PREFIX_AUDIT: &[u8] = b"s:audit\x00";

/// STS 会话记录(M15 T1/T2;ADR-18 D-E2):`s:session\0{session_id}` →
/// postcard(fs3_core::SessionRecord)。s: 既有前缀下的系统键族(同
/// PREFIX_AUDIT 口径:不新增一级前缀,DTO/check/删桶三处无需联动);
/// 会话撤销 = 删键;过期由数据面按记录 expires_at 判定。
pub const PREFIX_SESSION: &[u8] = b"s:session\x00";

/// 转义:S3 对象键可含任意字节,0x00/0xFF 需转义以保持键内无分隔符。
pub fn escape(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + raw.len() / 8 + 4);
    for &b in raw {
        match b {
            0x00 => {
                out.push(0xFF);
                out.push(0x00);
            }
            0xFF => {
                out.push(0xFF);
                out.push(0xFF);
            }
            b => out.push(b),
        }
    }
    out
}

/// 反转义;孤立的 0xFF 视为损坏。
pub fn unescape(esc: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(esc.len());
    let mut i = 0;
    while i < esc.len() {
        let b = esc[i];
        if b == 0xFF {
            let nxt = *esc
                .get(i + 1)
                .ok_or_else(|| Error::Corrupt("lone 0xFF escape at end of key".into()))?;
            match nxt {
                0x00 => out.push(0x00),
                0xFF => out.push(0xFF),
                _ => {
                    return Err(Error::Corrupt(format!(
                        "invalid escape sequence 0xFF 0x{nxt:02x}"
                    )))
                }
            }
            i += 2;
        } else {
            out.push(b);
            i += 1;
        }
    }
    Ok(out)
}

pub fn bucket_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_BUCKET.len() + name.len());
    k.extend_from_slice(PREFIX_BUCKET);
    k.extend_from_slice(name.as_bytes());
    k
}

/// 桶 LocationConstraint 键(`l:{bucket}`;桶名不含 0x00,无需转义)。
pub fn bucket_location_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_BUCKET_LOC.len() + name.len());
    k.extend_from_slice(PREFIX_BUCKET_LOC);
    k.extend_from_slice(name.as_bytes());
    k
}

/// D9 桶级配置文档键(`{prefix}{bucket}`;桶名不含 0x00/0xFF,无需转义,
/// 同 `l:` 先例)。`prefix` 限 PREFIX_BUCKET_CORS / PREFIX_BUCKET_TAGGING /
/// PREFIX_BUCKET_OWNERSHIP / PREFIX_BUCKET_POLICY(调用方经 fs3-meta
/// BucketConf 枚举传入,不裸露)。
pub fn bucket_conf_key(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + name.len());
    k.extend_from_slice(prefix);
    k.extend_from_slice(name.as_bytes());
    k
}

pub fn object_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_OBJECT.len() + bucket.len() + 1 + key.len() + 8);
    k.extend_from_slice(PREFIX_OBJECT);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(&escape(key.as_bytes()));
    k
}

pub fn object_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_OBJECT.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_OBJECT);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// null 版本槽 vk(ADR-11 D1:Suspended 桶的写入/删除标记原地覆盖该槽;
/// 全 1,恒为该 key 前缀下键序最大 = 恒为「当前版本」)。
pub const VK_NULL: [u8; 16] = [0xFF; 16];

/// 版本化对象键:`o:{bucket}\0{esc(key)}\0{vk16}`(ADR-11 D1)。
///
/// vk16 = be64(微秒时间戳) ‖ be64(随机),字典序 = 时间序;vk 原样追加
/// (不转义):esc(key) 不含裸 0x00,后缀分隔符唯一可辨,vk 定长 16 字节
/// 收尾,内部 0x00/0xFF 不干扰解析与前缀边界。
pub fn object_version_key(bucket: &str, key: &str, vk: &[u8; 16]) -> Vec<u8> {
    let mut k = object_key(bucket, key);
    k.push(0x00);
    k.extend_from_slice(vk);
    k
}

/// 单 key 全版本前缀:`o:{bucket}\0{esc(key)}\0`(该 key 全部版本的前缀
/// 扫描;未版本化条目 `o:{bucket}\0{esc(key)}` 更短,天然不在其内)。
pub fn object_key_prefix(bucket: &str, key: &str) -> Vec<u8> {
    let mut k = object_key(bucket, key);
    k.push(0x00);
    k
}

/// 解析 `o:` 键为 (bucket, key)。
///
/// 仅限未版本化形态;esc(key) 不含裸 0x00,故键体出现 0x00 即版本键
/// 形态(`...\0{vk16}`)——不得误吞,须走 parse_object_version_key。
pub fn parse_object_key(raw: &[u8]) -> Result<(String, String)> {
    let (bucket, key, vk) = parse_object_version_key(raw)?;
    if vk.is_some() {
        return Err(Error::Corrupt(
            "versioned object key; use parse_object_version_key".into(),
        ));
    }
    Ok((bucket, key))
}

/// 解析 `o:` 键(双形态单入口,ADR-11 D1):
/// 未版本化 `o:{bucket}\0{esc(key)}` → (bucket, key, None);
/// 版本化 `o:{bucket}\0{esc(key)}\0{vk16}` → (bucket, key, Some(vk))。
pub fn parse_object_version_key(raw: &[u8]) -> Result<(String, String, Option<[u8; 16]>)> {
    let body = raw
        .strip_prefix(PREFIX_OBJECT)
        .ok_or_else(|| Error::Corrupt("object key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("object key missing separator".into()))?;
    let bucket = String::from_utf8(body[..sep].to_vec())
        .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
    let rest = &body[sep + 1..];
    // esc(key) 中 0x00 仅以转义对(FF 00)成员出现,单看「是否为 0x00」
    // 会把转义对误判为分隔符;须按转义对跳过扫描,首个新鲜位置上的
    // 0x00 才是版本分隔符,其后须恰为 vk16。
    let mut i = 0;
    let vsep = loop {
        match rest.get(i).copied() {
            None => break None,
            Some(0xFF) => {
                if rest.get(i + 1).is_none() {
                    // 孤立 0xFF:无版本后缀,交由 unescape 报 Corrupt
                    break None;
                }
                i += 2;
            }
            Some(0x00) => break Some(i),
            Some(_) => i += 1,
        }
    };
    match vsep {
        None => {
            let key = String::from_utf8(unescape(rest)?)
                .map_err(|_| Error::Corrupt("object key not utf8".into()))?;
            Ok((bucket, key, None))
        }
        Some(i) => {
            let key = String::from_utf8(unescape(&rest[..i])?)
                .map_err(|_| Error::Corrupt("object key not utf8".into()))?;
            let vk: [u8; 16] = rest[i + 1..]
                .try_into()
                .map_err(|_| Error::Corrupt("object version key malformed".into()))?;
            Ok((bucket, key, Some(vk)))
        }
    }
}

pub fn alloc_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_ALLOC.len() + 8);
    k.extend_from_slice(PREFIX_ALLOC);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

pub fn txn_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_TXN.len() + 8);
    k.extend_from_slice(PREFIX_TXN);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// 审计条目键:`s:audit\0{seq be64}`(M11 L3-1;字典序 = 数值序 =
/// 写入序,同 a:/t: be64 先例)。
pub fn audit_entry_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_AUDIT.len() + 8);
    k.extend_from_slice(PREFIX_AUDIT);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// 会话记录键:`s:session\0{session_id}`(M15 T1;ADR-18 D-E2)。
pub fn sts_session_key(session_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_SESSION.len() + session_id.len());
    k.extend_from_slice(PREFIX_SESSION);
    k.extend_from_slice(session_id.as_bytes());
    k
}

/// 解析 `s:session\0` 键 → session_id。
pub fn parse_sts_session_id(raw: &[u8]) -> Result<String> {
    let body = raw
        .strip_prefix(PREFIX_SESSION)
        .ok_or_else(|| Error::Corrupt("session key missing prefix".into()))?;
    String::from_utf8(body.to_vec()).map_err(|_| Error::Corrupt("session id not utf8".into()))
}

/// 解析 `s:audit\0` 键中的 seq(回放种子/截断边界用)。
pub fn parse_audit_seq(raw: &[u8]) -> Result<u64> {
    let body = raw
        .strip_prefix(PREFIX_AUDIT)
        .ok_or_else(|| Error::Corrupt("audit key missing prefix".into()))?;
    if body.len() != 8 {
        return Err(Error::Corrupt("audit key malformed".into()));
    }
    Ok(u64::from_be_bytes(body.try_into().unwrap()))
}

/// 会话主键:`u:{uploadId}`。
pub fn session_key(upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD.len() + upload_id.len());
    k.extend_from_slice(PREFIX_UPLOAD);
    k.extend_from_slice(upload_id.as_bytes());
    k
}

/// 会话桶索引键:`m:{bucket}\0{uploadId}`。
pub fn session_index_key(bucket: &str, upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD_INDEX.len() + bucket.len() + 1 + upload_id.len());
    k.extend_from_slice(PREFIX_UPLOAD_INDEX);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(upload_id.as_bytes());
    k
}

/// 会话桶索引前缀:`m:{bucket}\0`(ListMultipartUploads 扫描)。
pub fn session_index_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD_INDEX.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_UPLOAD_INDEX);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `m:` 索引键 → uploadId。
pub fn parse_session_index_key(raw: &[u8]) -> Result<String> {
    let body = raw
        .strip_prefix(PREFIX_UPLOAD_INDEX)
        .ok_or_else(|| Error::Corrupt("session index key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("session index key missing separator".into()))?;
    let uid = String::from_utf8(body[sep + 1..].to_vec())
        .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
    Ok(uid)
}

/// 生命周期规则键:`r:{bucket}\0{rule_id}`(M11 L1;ADR-12 DL1;两段式同
/// `m:` 先例——桶名/rule_id 均为 UTF-8 文本,桶名不含 0x00,分隔符唯一
/// 可辨;rule_id 协议层限非空 ≤255 字符)。
pub fn lifecycle_rule_key(bucket: &str, rule_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_LIFECYCLE_RULE.len() + bucket.len() + 1 + rule_id.len());
    k.extend_from_slice(PREFIX_LIFECYCLE_RULE);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(rule_id.as_bytes());
    k
}

/// 桶级生命周期规则前缀:`r:{bucket}\0`(规则整体替换/删桶清理的扫描
/// 边界;桶间天然隔离)。
pub fn lifecycle_rules_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_LIFECYCLE_RULE.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_LIFECYCLE_RULE);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 事件通知规则键:`n:{bucket}\0{rule_id}`(M15 N1;ADR-18 D-E4)。
pub fn notification_rule_key(bucket: &str, rule_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_NOTIFICATION.len() + bucket.len() + 1 + rule_id.len());
    k.extend_from_slice(PREFIX_NOTIFICATION);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(rule_id.as_bytes());
    k
}

/// 桶级通知规则前缀:`n:{bucket}\0`(规则整体替换/删桶清理的扫描
/// 边界;桶间天然隔离)。
pub fn notification_rules_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_NOTIFICATION.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_NOTIFICATION);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `n:` 键 → (bucket, rule_id)。
pub fn parse_notification_rule_key(raw: &[u8]) -> Result<(String, String)> {
    let body = raw
        .strip_prefix(PREFIX_NOTIFICATION)
        .ok_or_else(|| Error::Corrupt("notification key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("notification key missing separator".into()))?;

    let bucket = String::from_utf8(body[..sep].to_vec())
        .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
    let rid = String::from_utf8(body[sep + 1..].to_vec())
        .map_err(|_| Error::Corrupt("rule id not utf8".into()))?;
    Ok((bucket, rid))
}

/// S3 Inventory 配置键:`iv:{bucket}\0{id}`(M15 I1)。
pub fn inventory_config_key(bucket: &str, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_INVENTORY.len() + bucket.len() + 1 + id.len());
    k.extend_from_slice(PREFIX_INVENTORY);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(id.as_bytes());
    k
}

/// 桶级 Inventory 配置前缀:`iv:{bucket}\0`(List/删桶清理扫描边界)。
pub fn inventory_configs_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_INVENTORY.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_INVENTORY);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `iv:` 键 → (bucket, config_id)。
pub fn parse_inventory_config_key(raw: &[u8]) -> Result<(String, String)> {
    let body = raw
        .strip_prefix(PREFIX_INVENTORY)
        .ok_or_else(|| Error::Corrupt("inventory key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("inventory key missing separator".into()))?;
    let bucket = String::from_utf8(body[..sep].to_vec())
        .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
    let id = String::from_utf8(body[sep + 1..].to_vec())
        .map_err(|_| Error::Corrupt("inventory id not utf8".into()))?;
    Ok((bucket, id))
}

/// 事件队列条目键:`e:{seq be64}`(M15 N2;ADR-18 D-E1)。
/// 归档恢复作业键(`x:{seq be64}`;be64 字典序 = 数值序 = 入队序)。
pub fn restore_job_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_RESTORE_JOB.len() + 8);
    k.extend_from_slice(PREFIX_RESTORE_JOB);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

pub fn event_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_EVENT.len() + 8);
    k.extend_from_slice(PREFIX_EVENT);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// 解析 `e:` 键中的 seq(队首游标/截断边界用)。
/// 解析 `x:` 键中的 seq(队首游标用)。
pub fn parse_restore_job_seq(raw: &[u8]) -> Result<u64> {
    let body = raw
        .strip_prefix(PREFIX_RESTORE_JOB)
        .ok_or_else(|| Error::Corrupt("restore job key missing prefix".into()))?;
    if body.len() != 8 {
        return Err(Error::Corrupt("restore job key malformed".into()));
    }
    Ok(u64::from_be_bytes(body.try_into().unwrap()))
}

pub fn parse_event_seq(raw: &[u8]) -> Result<u64> {
    let body = raw
        .strip_prefix(PREFIX_EVENT)
        .ok_or_else(|| Error::Corrupt("event key missing prefix".into()))?;
    if body.len() != 8 {
        return Err(Error::Corrupt("event key malformed".into()));
    }
    Ok(u64::from_be_bytes(body.try_into().unwrap()))
}

/// 分片键:`p:{uploadId}\0{part_no be32}`。
pub fn part_key(upload_id: &str, part_no: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_PART.len() + upload_id.len() + 1 + 4);
    k.extend_from_slice(PREFIX_PART);
    k.extend_from_slice(upload_id.as_bytes());
    k.push(0x00);
    k.extend_from_slice(&part_no.to_be_bytes());
    k
}

/// 分片前缀:`p:{uploadId}\0`(ListParts 扫描)。
pub fn part_prefix(upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_PART.len() + upload_id.len() + 1);
    k.extend_from_slice(PREFIX_PART);
    k.extend_from_slice(upload_id.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `p:` 键 → part_no。
pub fn parse_part_key(raw: &[u8]) -> Result<u32> {
    let body = raw
        .strip_prefix(PREFIX_PART)
        .ok_or_else(|| Error::Corrupt("part key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("part key missing separator".into()))?;
    let no = &body[sep + 1..];
    if no.len() != 4 {
        return Err(Error::Corrupt("part key malformed".into()));
    }
    Ok(u32::from_be_bytes(no.try_into().unwrap()))
}

/// 解析 `a:` 键中的 seq。
pub fn parse_alloc_seq(raw: &[u8]) -> Result<u64> {
    let body = raw
        .strip_prefix(PREFIX_ALLOC)
        .ok_or_else(|| Error::Corrupt("alloc key missing prefix".into()))?;
    if body.len() != 8 {
        return Err(Error::Corrupt("alloc key malformed".into()));
    }
    Ok(u64::from_be_bytes(body.try_into().unwrap()))
}

/// 密钥主键:`k:{access_key}`。
pub fn key_key(access_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_KEY.len() + access_key.len());
    k.extend_from_slice(PREFIX_KEY);
    k.extend_from_slice(access_key.as_bytes());
    k
}

/// 解析 `k:` 键 → access_key。
pub fn parse_key_key(raw: &[u8]) -> Result<String> {
    let body = raw
        .strip_prefix(PREFIX_KEY)
        .ok_or_else(|| Error::Corrupt("key record missing prefix".into()))?;
    String::from_utf8(body.to_vec()).map_err(|_| Error::Corrupt("access key not utf8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn escape_roundtrip() {
        for raw in [
            b"".as_slice(),
            b"plain-key",
            b"a\x00b",
            b"\xFF\x00\xFF",
            &[0x00, 0x01, 0xFF, 0xFE, 0x00],
        ] {
            let esc = escape(raw);
            // 转义后的键不允许"孤立 0x00"(0x00 必须紧跟 0xFF 前缀),
            // 保证 `o:{bucket}\0` 分隔符唯一可辨
            assert!(esc
                .iter()
                .enumerate()
                .all(|(i, &b)| { b != 0x00 || (i > 0 && esc[i - 1] == 0xFF) }));
            assert_eq!(unescape(&esc).unwrap(), raw);
        }
    }

    #[test]
    fn escape_is_injective_and_prefix_safe() {
        // 转义后不存在 0x00,`o:{bucket}\0` 前缀扫描不会串桶
        let a = object_key("b1", "k\x00x");
        let b = object_key("b1", "k\u{FF}x");
        let c = object_key("b12", "kx");
        // 三者前缀各不相同
        assert!(object_prefix("b1") != object_prefix("b12"));
        assert!(a.starts_with(&object_prefix("b1")));
        assert!(b.starts_with(&object_prefix("b1")));
        assert!(!c.starts_with(&object_prefix("b1")));
        // 解析往返
        let (bucket, key) = parse_object_key(&a).unwrap();
        assert_eq!(bucket, "b1");
        assert_eq!(key, "k\x00x");
    }

    #[test]
    fn unescape_rejects_bad_sequences() {
        assert!(unescape(b"\xFF").is_err());
        assert!(unescape(b"a\xFF\x01").is_err());
        assert!(unescape(b"\xFF\xFF").unwrap() == vec![0xFF]);
    }

    #[test]
    fn object_key_byte_level_regression() {
        // ADR-11 D1 硬承诺:未版本化键编码逐字节不变(含转义与桶前缀)
        assert_eq!(object_key("b1", "k"), b"o:b1\x00k".as_slice());
        assert_eq!(object_key("b1", "a/b"), b"o:b1\x00a/b".as_slice());
        // U+00FF 的 UTF-8 编码为 C3 BF(非 0xFF 字节),0x00 → FF 00
        assert_eq!(
            object_key("b1", "k\x00y\u{FF}"),
            b"o:b1\x00k\xFF\x00y\xC3\xBF".as_slice()
        );
        assert_eq!(object_prefix("b1"), b"o:b1\x00".as_slice());
        assert_eq!(object_key("b1", ""), b"o:b1\x00".as_slice());
    }

    #[test]
    fn object_version_key_roundtrip() {
        let vk = [0x12u8; 16];
        let k = object_version_key("b1", "dir/a\x00b", &vk);
        // 形态:`o:{bucket}\0{esc(key)}\0{vk16}`,vk 原样追加不转义
        assert_eq!(
            k,
            [
                b"o:b1\x00".as_slice(),
                b"dir/a\xFF\x00b".as_slice(),
                &[0x00],
                &vk
            ]
            .concat()
            .as_slice()
        );
        let (b, key, v) = parse_object_version_key(&k).unwrap();
        assert_eq!(
            (b.as_str(), key.as_str(), v),
            ("b1", "dir/a\x00b", Some(vk))
        );
        // 键以 0x00 结尾(esc 以转义对 FF 00 收尾):分隔符仍唯一可辨
        let k2 = object_version_key("b1", "tail\x00", &vk);
        assert_eq!(parse_object_version_key(&k2).unwrap().1, "tail\x00");
        // 未版本化 parse 不得误吞版本键
        assert!(parse_object_key(&k).is_err());
        // 未版本化键经双形态入口 → None
        let (b, key, v) = parse_object_version_key(&object_key("b1", "plain")).unwrap();
        assert_eq!((b.as_str(), key.as_str(), v), ("b1", "plain", None));
        // vk 长度不合法(尾部垃圾/短缺)→ 拒绝
        let mut bad = object_version_key("b1", "k", &vk);
        bad.push(0x00);
        assert!(parse_object_version_key(&bad).is_err());
        let full = object_version_key("b1", "k", &vk);
        let short = &full[..full.len() - 1];
        assert!(parse_object_version_key(short).is_err());
    }

    #[test]
    fn object_version_key_ordering_and_prefixes() {
        // null 槽恒为该 key 前缀下键序最大(任意 vk < VK_NULL)
        for vk in [[0x00u8; 16], [0x42u8; 16], [0xFEu8; 16]] {
            assert!(object_version_key("b1", "k", &vk) < object_version_key("b1", "k", &VK_NULL));
        }
        assert_eq!(
            parse_object_version_key(&object_version_key("b1", "k", &VK_NULL))
                .unwrap()
                .2,
            Some(VK_NULL)
        );
        // 全版本前缀:同 key 全版本(含 null 槽)都在其内;未版本化条目
        // 与相邻 key 不在其内
        let kp = object_key_prefix("b1", "k");
        assert!(object_version_key("b1", "k", &[0x00; 16]).starts_with(&kp));
        assert!(object_version_key("b1", "k", &VK_NULL).starts_with(&kp));
        assert!(!object_key("b1", "k").starts_with(&kp));
        assert!(!object_key("b1", "ka").starts_with(&kp));
        assert!(!object_version_key("b1", "ka", &[0x00; 16]).starts_with(&kp));
        // vk 后缀不影响桶级前缀扫描:版本键仍在 `o:{bucket}\0` 内,
        // 且不串桶;版本键恒排在同 key 未版本化条目之后(前缀更长)
        assert!(object_version_key("b1", "k", &[0x00; 16]).starts_with(&object_prefix("b1")));
        assert!(!object_version_key("b2", "k", &[0x00; 16]).starts_with(&object_prefix("b1")));
        assert!(object_key("b1", "k") < object_version_key("b1", "k", &[0x00; 16]));
    }

    #[test]
    fn alloc_seq_keys_sort_numerically() {
        // be64 编码保证 a: 键按 seq 字典序 == 数值序
        let k1 = alloc_key(1);
        let k9 = alloc_key(9);
        let k10 = alloc_key(10);
        assert!(k1 < k9 && k9 < k10);
        assert_eq!(parse_alloc_seq(&k10).unwrap(), 10);
    }

    #[test]
    fn audit_entry_key_byte_level_and_ordering() {
        // M11 L3-1(ADR-12 DL5):`s:audit\0{seq be64}` 形态与排序不变量
        assert_eq!(
            audit_entry_key(1),
            b"s:audit\x00\x00\x00\x00\x00\x00\x00\x00\x01".as_slice()
        );
        let (k1, k9, k10) = (audit_entry_key(1), audit_entry_key(9), audit_entry_key(10));
        assert!(k1 < k9 && k9 < k10, "be64 字典序 == 数值序");
        assert_eq!(parse_audit_seq(&k10).unwrap(), 10);
        for k in [&k1, &k9, &k10] {
            assert!(k.starts_with(PREFIX_AUDIT));
        }
        // 与既有 s: 系统键不相交(s:seq/s:key_seed_salt 等不以 s:audit\0 开头)
        assert!(!SYS_SEQ.starts_with(PREFIX_AUDIT));
        assert!(!SYS_SSE_KEK_GEN.starts_with(PREFIX_AUDIT));
        assert!(!SYS_TRUSTED_CLOCK.starts_with(PREFIX_AUDIT));
        assert_eq!(SYS_TRUSTED_CLOCK, b"s:trusted_clock");
        assert!(parse_audit_seq(SYS_SEQ).is_err());
        assert!(parse_audit_seq(b"s:audit\x00\x01").is_err());
    }

    #[test]
    fn lifecycle_rule_key_byte_level_and_prefix_isolation() {
        // M11 L1(ADR-12 DL1):`r:{bucket}\0{rule_id}` 两段式形态
        assert_eq!(lifecycle_rule_key("b1", "r1"), b"r:b1\x00r1".as_slice());
        assert_eq!(lifecycle_rules_prefix("b1"), b"r:b1\x00".as_slice());
        // 规则键恒落在本桶前缀内;不串桶(含同头桶名 b1/b12)
        assert!(lifecycle_rule_key("b1", "r1").starts_with(&lifecycle_rules_prefix("b1")));
        assert!(!lifecycle_rule_key("b12", "r1").starts_with(&lifecycle_rules_prefix("b1")));
        assert!(!lifecycle_rule_key("b1", "r1").starts_with(&lifecycle_rules_prefix("b12")));
        // 与既有前缀域不相交(r: 独立前缀)
        assert!(!lifecycle_rule_key("b1", "r1").starts_with(PREFIX_OBJECT));
        assert!(!object_key("b1", "k").starts_with(&lifecycle_rules_prefix("b1")));
    }

    #[test]
    fn notification_rule_key_byte_level_and_prefix_isolation() {
        // M15 N1(ADR-18 D-E4):`n:{bucket}\0{rule_id}` 两段式形态
        assert_eq!(notification_rule_key("b1", "n1"), b"n:b1\x00n1".as_slice());
        assert_eq!(notification_rules_prefix("b1"), b"n:b1\x00".as_slice());
        // 规则键恒落在本桶前缀内;不串桶(含同头桶名 b1/b12)
        assert!(notification_rule_key("b1", "n1").starts_with(&notification_rules_prefix("b1")));
        assert!(!notification_rule_key("b12", "n1").starts_with(&notification_rules_prefix("b1")));
        assert!(!notification_rule_key("b1", "n1").starts_with(&notification_rules_prefix("b12")));
        // 解析往返
        assert_eq!(
            parse_notification_rule_key(&notification_rule_key("b1", "n1")).unwrap(),
            ("b1".to_string(), "n1".to_string())
        );
        assert!(parse_notification_rule_key(b"o:b1\x00k").is_err());
        assert!(parse_notification_rule_key(b"n:b1").is_err());
        // 与既有前缀域不相交(n: 独立前缀;r: 生命周期、o: 对象均不相交)
        assert!(!notification_rule_key("b1", "n1").starts_with(PREFIX_OBJECT));
        assert!(!notification_rule_key("b1", "n1").starts_with(PREFIX_LIFECYCLE_RULE));
        assert!(!lifecycle_rule_key("b1", "r1").starts_with(PREFIX_NOTIFICATION));
        assert!(!object_key("b1", "k").starts_with(&notification_rules_prefix("b1")));
    }

    #[test]
    fn event_key_byte_level_and_ordering() {
        // M15 N2(ADR-18 D-E1):`e:{seq be64}` be64 字典序 == 数值序
        assert_eq!(
            event_key(1),
            b"e:\x00\x00\x00\x00\x00\x00\x00\x01".as_slice()
        );
        let (k1, k9, k10) = (event_key(1), event_key(9), event_key(10));
        assert!(k1 < k9 && k9 < k10, "be64 字典序 == 数值序");
        assert_eq!(parse_event_seq(&k10).unwrap(), 10);
        for k in [&k1, &k9, &k10] {
            assert!(k.starts_with(PREFIX_EVENT));
        }
        // 与既有前缀域不相交(独立一级前缀 e:)
        assert!(!SYS_SEQ.starts_with(PREFIX_EVENT));
        assert!(!PREFIX_AUDIT.starts_with(PREFIX_EVENT));
        assert!(!event_key(1).starts_with(PREFIX_OBJECT));
        assert!(!object_key("b1", "k").starts_with(PREFIX_EVENT));
        assert!(parse_event_seq(b"e:\x01").is_err());
        assert!(parse_event_seq(SYS_SEQ).is_err());
    }

    proptest::proptest! {
        #[test]
        fn escape_roundtrip_prop(data: Vec<u8>) {
            let esc = escape(&data);
            prop_assert!(esc
                .iter()
                .enumerate()
                .all(|(i, &b)| b != 0x00 || (i > 0 && esc[i - 1] == 0xFF)));
            prop_assert_eq!(&unescape(&esc).unwrap(), &data);
            // 转义是单射
            let esc2 = escape(&data);
            prop_assert_eq!(esc, esc2);
        }

        #[test]
        fn object_key_prefix_prop(bucket: String, key: String) {
            // 桶名不含 0x00/0xFF 时,解析必须往返
            if bucket.bytes().all(|b| b != 0x00 && b != 0xFF) {
                let k = object_key(&bucket, &key);
                let (b2, k2) = parse_object_key(&k).unwrap();
                prop_assert_eq!(b2, bucket.clone());
                prop_assert_eq!(k2, key);
            }
        }

        #[test]
        fn object_version_key_prop(bucket: String, key: String, vk: [u8; 16]) {
            // 两种形态往返 + 前缀不变量(ADR-11 D1)
            if bucket.bytes().all(|b| b != 0x00 && b != 0xFF) && !bucket.is_empty() {
                let vk_key = object_version_key(&bucket, &key, &vk);
                let (b2, k2, v2) = parse_object_version_key(&vk_key).unwrap();
                prop_assert_eq!(b2, bucket.clone());
                prop_assert_eq!(k2, key.clone());
                prop_assert_eq!(v2, Some(vk));
                // 未版本化 parse 不得误吞版本键;未版本化键双形态 → None
                prop_assert!(parse_object_key(&vk_key).is_err());
                let plain = object_key(&bucket, &key);
                prop_assert_eq!(parse_object_version_key(&plain).unwrap().2, None);
                // vk 后缀不影响桶级/单 key 前缀扫描;版本键恒排在
                // 同 key 未版本化条目之后
                prop_assert!(vk_key.starts_with(&object_prefix(&bucket)));
                prop_assert!(vk_key.starts_with(&object_key_prefix(&bucket, &key)));
                prop_assert!(plain < vk_key);
                // null 槽恒为该 key 前缀下键序最大
                if vk != VK_NULL {
                    prop_assert!(vk_key < object_version_key(&bucket, &key, &VK_NULL));
                }
            }
        }
    }
}
