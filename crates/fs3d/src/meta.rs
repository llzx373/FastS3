//! M7 / E5 元数据快照:meta-export / meta-import。
//!
//! # meta-export
//!
//! 把全部元数据(桶、访问密钥、对象、multipart 会话与分片、种子盐)导出为
//! **可移植 JSON**(对象 `inline` 数据 base64、etag 十六进制)。配合底层卷快照
//! 构成完整备份(设计 §4.4 / TODO L5):
//!
//! ```text
//! 优雅停机(fasts3d serve 收尾写检查点)
//!   → fasts3d meta-export --output meta.json   # 元数据快照
//!   → 底层卷快照(cp/LVM/设备快照)             # 数据快照
//! ```
//!
//! > 导出文件含种子盐(密钥密文派生密钥)与密钥哈希,属敏感文件:落盘 0600,
//! > 应随卷快照一起加密保管。
//!
//! # meta-import
//!
//! 恢复到**同一布局**(extent_size/extent_count/layout_version 必须与导出一致)
//! 的目标设备:先恢复底层卷数据快照,再把元数据导入(meta 目录须为空,或
//! `--force` 重建)。导入的事务序号从导出时的 `last_seq` 继续,引擎打开时按
//! `seq > 检查点序号` 全量重放,位图/引用计数/共享段表由既有恢复路径重建,
//! 最后写新检查点收尾。

use std::path::Path;
use std::path::PathBuf;

use clap::Args;
use fs3_core::{BucketMeta, KeyRecord, ObjectMeta, Segment, SuperBlock, MAX_OBJECT_SIZE};
use fs3_meta::{AllocDraft, MetaConfig, MetaStore, PartMeta, StatsDelta};
use serde::{Deserialize, Serialize};

pub const META_EXPORT_FORMAT: &str = "fasts3-meta-export";
pub const META_EXPORT_VERSION: u32 = 1;

// ───────────────────────────── DTO(可移植 JSON) ─────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDto {
    pub extent_id: u32,
    pub offset: u32,
    pub len: u32,
    pub crcs: Vec<u32>,
}

impl From<&Segment> for SegmentDto {
    fn from(s: &Segment) -> Self {
        SegmentDto {
            extent_id: s.extent_id,
            offset: s.offset,
            len: s.len,
            crcs: s.crcs.clone(),
        }
    }
}

impl SegmentDto {
    fn to_segment(&self) -> Segment {
        Segment {
            extent_id: self.extent_id,
            offset: self.offset,
            len: self.len,
            crcs: self.crcs.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDto {
    pub size: u64,
    pub etag_hex: String,
    pub mtime: i64,
    pub extents: Vec<SegmentDto>,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// 内联小对象数据(base64;导出时 ≤ small_object_limit 才可能非空)。
    pub inline_b64: Option<String>,
    pub parts: Vec<u64>,
}

impl ObjectDto {
    fn from_meta(m: &ObjectMeta) -> Self {
        ObjectDto {
            size: m.size,
            etag_hex: m.etag_hex(),
            mtime: m.mtime,
            extents: m.extents.iter().map(SegmentDto::from).collect(),
            content_type: m.content_type.clone(),
            user_meta: m.user_meta.clone(),
            inline_b64: m
                .inline
                .as_ref()
                .map(|d| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)),
            parts: m.parts.clone(),
        }
    }

    fn to_meta(&self) -> fs3_core::Result<ObjectMeta> {
        // etag_hex → [u8;16];base64 → 字节
        let etag = hex_to_etag(&self.etag_hex)?;
        let inline = match &self.inline_b64 {
            Some(b) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b)
                    .map_err(|e| fs3_core::Error::InvalidArgument(format!("inline base64: {e}")))?,
            ),
            None => None,
        };
        Ok(ObjectMeta {
            size: self.size,
            etag,
            mtime: self.mtime,
            extents: self.extents.iter().map(SegmentDto::to_segment).collect(),
            content_type: self.content_type.clone(),
            user_meta: self.user_meta.clone(),
            inline,
            parts: self.parts.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartDto {
    pub part_no: u32,
    pub size: u64,
    pub etag_hex: String,
    pub mtime: i64,
    pub extents: Vec<SegmentDto>,
    pub inline_b64: Option<String>,
}

impl PartDto {
    fn from_part(part_no: u32, p: &PartMeta) -> Self {
        PartDto {
            part_no,
            size: p.size,
            etag_hex: p.etag_hex(),
            mtime: p.mtime,
            extents: p.extents.iter().map(SegmentDto::from).collect(),
            inline_b64: p
                .inline
                .as_ref()
                .map(|d| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)),
        }
    }

    fn to_part(&self) -> fs3_core::Result<PartMeta> {
        Ok(PartMeta {
            size: self.size,
            etag: hex_to_etag(&self.etag_hex)?,
            mtime: self.mtime,
            extents: self.extents.iter().map(SegmentDto::to_segment).collect(),
            inline: match &self.inline_b64 {
                Some(b) => Some(
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b).map_err(
                        |e| fs3_core::Error::InvalidArgument(format!("part inline base64: {e}")),
                    )?,
                ),
                None => None,
            },
        })
    }
}

/// multipart 会话(会话字段为纯文本;final_etag 转 hex)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadDto {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    pub created: i64,
    pub completed: bool,
    pub final_etag_hex: String,
    pub final_size: u64,
    pub final_mtime: i64,
    pub parts: Vec<PartDto>,
}

impl UploadDto {
    fn from_session(
        upload_id: &str,
        s: &fs3_meta::MultipartSession,
        parts: Vec<(u32, PartMeta)>,
    ) -> Self {
        UploadDto {
            upload_id: upload_id.to_string(),
            bucket: s.bucket.clone(),
            key: s.key.clone(),
            content_type: s.content_type.clone(),
            user_meta: s.user_meta.clone(),
            created: s.created,
            completed: s.completed,
            final_etag_hex: s.final_etag.iter().map(|b| format!("{b:02x}")).collect(),
            final_size: s.final_size,
            final_mtime: s.final_mtime,
            parts: parts
                .iter()
                .map(|(no, p)| PartDto::from_part(*no, p))
                .collect(),
        }
    }
}

/// 磁盘布局信息(导入校验用)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutInfoDto {
    pub extent_size: u64,
    pub extent_count: u64,
    pub layout_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketDto {
    pub name: String,
    pub created: i64,
    pub owner: String,
    pub objects: u64,
    pub bytes: u64,
    pub quota: Option<u64>,
}

/// 导出文件顶层结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaExportFile {
    pub format: String,
    pub format_version: u32,
    pub created: i64,
    pub fasts3_version: String,
    pub layout: LayoutInfoDto,
    /// 导出时元数据事务序号(导入从此继续,保证重放完整性)。
    pub last_seq: u64,
    /// 密钥种子盐(hex;密钥密文 AES-GCM 派生密钥;敏感)。
    pub seed_salt_hex: String,
    pub buckets: Vec<BucketDto>,
    pub keys: Vec<KeyRecord>,
    pub objects: Vec<ObjectEntryDto>,
    pub uploads: Vec<UploadDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntryDto {
    pub bucket: String,
    pub key: String,
    pub meta: ObjectDto,
}

// ───────────────────────────── CLI 参数 ─────────────────────────────

#[derive(Args, Debug)]
pub struct MetaExportArgs {
    /// 输出 JSON 文件(默认 fasts3-meta-export.json;落盘权限 0600)
    #[arg(long, default_value = "fasts3-meta-export.json")]
    pub output: PathBuf,
}

#[derive(Args, Debug)]
pub struct MetaImportArgs {
    /// 输入 JSON 文件
    #[arg(long)]
    pub input: PathBuf,
    /// meta 目录非空时强制清空重建(旧目录先改名备份,不删除)
    #[arg(long)]
    pub force: bool,
}

// ───────────────────────────── 导出 ─────────────────────────────

pub fn run_meta_export(
    device: &Path,
    meta_dir: &Path,
    args: &MetaExportArgs,
) -> fs3_core::Result<()> {
    // 布局来源:设备超级块(导入校验的基准)
    let sb = read_device_superblock(device)?;
    let layout = LayoutInfoDto {
        extent_size: sb.extent_size,
        extent_count: sb.extent_count(),
        layout_version: sb.layout_version,
    };

    // 元数据必须以独占方式打开(与运行中的 fasts3d 互斥:rocksdb 目录锁;
    // 在线备份请先优雅停机,或在维护窗口内执行)
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;

    let last_seq = store.last_seq()?;
    let seed_salt = store.seed_salt()?;

    let buckets: Vec<BucketDto> = store
        .list_buckets()?
        .into_iter()
        .map(|(name, m)| BucketDto {
            name,
            created: m.created,
            owner: m.owner,
            objects: m.stats.objects,
            bytes: m.stats.bytes,
            quota: m.quota,
        })
        .collect();

    let keys = store.list_keys()?;

    let objects: Vec<ObjectEntryDto> = store
        .snapshot_all_objects()?
        .into_iter()
        .map(|(bucket, key, m)| ObjectEntryDto {
            bucket,
            key,
            meta: ObjectDto::from_meta(&m),
        })
        .collect();

    // multipart:会话 + 分片(MVCC 快照)
    let sessions = store.list_all_sessions()?;
    let part_map: std::collections::HashMap<String, Vec<(u32, PartMeta)>> = store
        .snapshot_all_parts()?
        .into_iter()
        .fold(std::collections::HashMap::new(), |mut acc, (uid, no, p)| {
            acc.entry(uid).or_default().push((no, p));
            acc
        });
    let uploads: Vec<UploadDto> = sessions
        .into_iter()
        .map(|(uid, s)| {
            let parts = part_map.get(&uid).cloned().unwrap_or_default();
            UploadDto::from_session(&uid, &s, parts)
        })
        .collect();

    let file = MetaExportFile {
        format: META_EXPORT_FORMAT.into(),
        format_version: META_EXPORT_VERSION,
        created: now_ts(),
        fasts3_version: env!("CARGO_PKG_VERSION").into(),
        layout,
        last_seq,
        seed_salt_hex: hex::encode(&seed_salt),
        buckets,
        keys,
        objects,
        uploads,
    };
    let json = serde_json::to_string_pretty(&file)
        .map_err(|e| fs3_core::Error::InvalidArgument(format!("serialize export: {e}")))?;
    drop(store); // 先关库再写文件(避免导出期间元数据变更)

    write_private(&args.output, json.as_bytes())?;
    println!(
        "meta-export: {} buckets, {} keys, {} objects, {} uploads → {}",
        file.buckets.len(),
        file.keys.len(),
        file.objects.len(),
        file.uploads.len(),
        args.output.display()
    );
    println!(
        "  layout: extent_size={} extent_count={} layout_version={} last_seq={}",
        file.layout.extent_size,
        file.layout.extent_count,
        file.layout.layout_version,
        file.last_seq
    );
    Ok(())
}

// ───────────────────────────── 导入 ─────────────────────────────

pub fn run_meta_import(
    device: &Path,
    meta_dir: &Path,
    args: &MetaImportArgs,
) -> fs3_core::Result<()> {
    let text = std::fs::read_to_string(&args.input).map_err(|e| {
        fs3_core::Error::InvalidArgument(format!("read {}: {e}", args.input.display()))
    })?;
    let file: MetaExportFile = serde_json::from_str(&text).map_err(|e| {
        fs3_core::Error::InvalidArgument(format!(
            "parse {}: {e} (not a meta-export JSON?)",
            args.input.display()
        ))
    })?;
    if file.format != META_EXPORT_FORMAT || file.format_version != META_EXPORT_VERSION {
        return Err(fs3_core::Error::InvalidArgument(format!(
            "unsupported export format {} v{} (expect {} v{})",
            file.format, file.format_version, META_EXPORT_FORMAT, META_EXPORT_VERSION
        )));
    }

    // 1) 布局强校验:目标设备必须与导出时完全一致(同一设备规格才能按段引用恢复)
    let sb = read_device_superblock(device)?;
    let layout = LayoutInfoDto {
        extent_size: sb.extent_size,
        extent_count: sb.extent_count(),
        layout_version: sb.layout_version,
    };
    if layout != file.layout {
        return Err(fs3_core::Error::InvalidLayout(format!(
            "device layout mismatch: export {} (extent_size={} extent_count={} layout_version={}), \
             device {} (extent_size={} extent_count={} layout_version={}); \
             meta-import 只能恢复到同一布局的设备(先恢复底层卷快照)",
            args.input.display(),
            file.layout.extent_size,
            file.layout.extent_count,
            file.layout.layout_version,
            device.display(),
            layout.extent_size,
            layout.extent_count,
            layout.layout_version,
        )));
    }

    // 2) meta 目录:空/不存在直接使用;非空需 --force(旧目录改名备份)
    prepare_meta_dir(meta_dir, args.force)?;

    // 3) 目标库:种子盐 + 序号复位(导入事务从 last_seq+1 继续)
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;
    let seed_salt = hex::decode(&file.seed_salt_hex)
        .map_err(|e| fs3_core::Error::InvalidArgument(format!("seed_salt_hex: {e}")))?;
    store.set_seed_salt(&seed_salt)?;
    store.reset_seq(file.last_seq)?;

    // 4) 桶(含统计/配额/owner/created 原样恢复)
    for b in &file.buckets {
        if store.get_bucket(&b.name)?.is_some() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "bucket {} already exists (meta dir not empty?)",
                b.name
            )));
        }
        let meta = BucketMeta {
            created: b.created,
            owner: b.owner.clone(),
            stats: fs3_core::BucketStats {
                objects: b.objects,
                bytes: b.bytes,
            },
            quota: b.quota,
        };
        store.commit_bucket_put(&b.name, &meta)?;
    }

    // 5) 访问密钥(secret_hash/salt/密文原样;种子盐已恢复,可解密)
    for k in &file.keys {
        store.commit_key_put(k)?;
    }

    // 6) 对象:段校验(布局边界/对齐)+ 分配草稿 + 零统计增量
    //    (桶统计已含最终值,避免二次记账)
    let mut objects_restored = 0usize;
    for o in &file.objects {
        if store.get_bucket(&o.bucket)?.is_none() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "object {}/{} references missing bucket {}",
                o.bucket, o.key, o.bucket
            )));
        }
        let meta = o.meta.to_meta()?;
        if meta.size > MAX_OBJECT_SIZE {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "object {}/{} size {} exceeds max {}",
                o.bucket, o.key, meta.size, MAX_OBJECT_SIZE
            )));
        }
        validate_segments(&meta.extents, &layout)?;
        let draft = draft_for_segments(&meta.extents);
        store.commit_object_put(&o.bucket, &o.key, &meta, draft, StatsDelta::default())?;
        objects_restored += 1;
    }

    // 7) multipart 会话与分片(段同样校验)
    let mut parts_restored = 0usize;
    for u in &file.uploads {
        if store.get_bucket(&u.bucket)?.is_none() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "upload {} references missing bucket {}",
                u.upload_id, u.bucket
            )));
        }
        let session = fs3_meta::MultipartSession {
            bucket: u.bucket.clone(),
            key: u.key.clone(),
            content_type: u.content_type.clone(),
            user_meta: u.user_meta.clone(),
            created: u.created,
            completed: u.completed,
            final_etag: hex_to_etag(&u.final_etag_hex)?,
            final_size: u.final_size,
            final_mtime: u.final_mtime,
        };
        store.create_multipart(&u.upload_id, &session)?;
        for p in &u.parts {
            let part = p.to_part()?;
            validate_segments(&part.extents, &layout)?;
            let draft = draft_for_segments(&part.extents);
            store.put_part(&u.upload_id, p.part_no, &part, draft)?;
            parts_restored += 1;
        }
    }
    drop(store);

    // 8) 引擎打开:检查点加载 + 导入记录全量重放(seq > cp.seq)+
    //    段级可达性重建(引用计数/共享段表/泄漏报告)→ 收尾写新检查点
    let engine_cfg = fs3_engine::EngineConfig {
        device: device.to_path_buf(),
        meta_dir: meta_dir.to_path_buf(),
        ..Default::default()
    };
    let mut engine = fs3_engine::Engine::open(&engine_cfg)?;
    let report = engine.check_report()?;
    println!(
        "meta-import: {} buckets, {} keys, {} objects, {} uploads/{} parts restored",
        file.buckets.len(),
        file.keys.len(),
        objects_restored,
        file.uploads.len(),
        parts_restored
    );
    println!(
        "  engine open: leaks={} live_bytes={} (device {})",
        report.leaks.len(),
        report.live_bytes,
        report.device
    );
    if !report.leaks.is_empty() {
        tracing::warn!(
            "meta-import: {} leaked extents after restore (data snapshot newer than meta snapshot?)",
            report.leaks.len()
        );
    }
    engine.close()?; // 密封开放 extent + 写新检查点 + 元数据 flush
    println!("meta-import: engine closed, final checkpoint written");
    Ok(())
}

// ───────────────────────────── 辅助 ─────────────────────────────

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 读取设备超级块(校验布局用)。
fn read_device_superblock(device: &Path) -> fs3_core::Result<SuperBlock> {
    let dev = fs3_device::open_device(device, true).map_err(|e| {
        fs3_core::Error::InvalidLayout(format!("cannot open {}: {e}", device.display()))
    })?;
    fs3_device::read_superblock(dev.as_ref()).map_err(|e| {
        fs3_core::Error::InvalidLayout(format!(
            "cannot read superblock from {}: {e} (device initialized?)",
            device.display()
        ))
    })
}

fn hex_to_etag(hex: &str) -> fs3_core::Result<[u8; 16]> {
    let bytes =
        hex::decode(hex).map_err(|e| fs3_core::Error::InvalidArgument(format!("etag hex: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| fs3_core::Error::InvalidArgument(format!("etag not 16 bytes: {hex}")))
}

/// 段合法性校验:编号在布局范围内、4KiB 对齐、段长 ≥ 4KiB 且不越界。
fn validate_segments(segs: &[Segment], layout: &LayoutInfoDto) -> fs3_core::Result<()> {
    for s in segs {
        if s.extent_id as u64 >= layout.extent_count {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment extent {} out of range (extent_count {})",
                s.extent_id, layout.extent_count
            )));
        }
        if s.offset % 4096 != 0 || s.len % 4096 != 0 || s.len < 4096 {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) not 4KiB aligned",
                s.extent_id, s.offset, s.len
            )));
        }
        if s.offset as u64 + s.len as u64 > layout.extent_size {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) exceeds extent_size {}",
                s.extent_id, s.offset, s.len, layout.extent_size
            )));
        }
        // CRC 网格 ≤ extent 的 64KiB 单元数(防御性)
        let max_units = (layout.extent_size as usize).div_ceil(65536);
        if s.crcs.len() > max_units && layout.extent_size > 0 {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) crc grid {} exceeds max {max_units}",
                s.extent_id,
                s.offset,
                s.len,
                s.crcs.len()
            )));
        }
    }
    Ok(())
}

/// 恢复用分配草稿:为对象涉及的每个 distinct extent 记一条 (id,1) 分配记录
/// (镜像正常提交的 `a:` 记录;压缩相邻段成一范围)。
/// 引用计数/共享段表不在此记账——引擎打开时由段级可达性重建(与崩溃
/// 恢复语义一致)。
fn draft_for_segments(segs: &[Segment]) -> AllocDraft {
    let mut ids: Vec<u64> = segs.iter().map(|s| s.extent_id as u64).collect();
    ids.sort_unstable();
    ids.dedup();
    AllocDraft {
        alloc: compress_ranges(&ids),
        ref_inc: Vec::new(),
        ref_dec: Vec::new(),
    }
}

/// (start, count) 区间压缩。
fn compress_ranges(sorted_ids: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &id in sorted_ids {
        match out.last_mut() {
            Some((start, count)) if id == *start + *count => *count += 1,
            _ => out.push((id, 1)),
        }
    }
    out
}

/// meta 目录准备:空/不存在 → 直接用;非空且 --force → 改名备份;非空无
/// --force → 拒绝(防误伤线上数据)。
fn prepare_meta_dir(meta_dir: &Path, force: bool) -> fs3_core::Result<()> {
    if !meta_dir.exists() {
        std::fs::create_dir_all(meta_dir)?;
        return Ok(());
    }
    let mut entries = std::fs::read_dir(meta_dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.retain(|e| e.file_name() != "LOCK"); // rocksdb 偶尔残留空锁文件
    if entries.is_empty() {
        return Ok(());
    }
    if !force {
        return Err(fs3_core::Error::InvalidArgument(format!(
            "meta dir {} is not empty; refusing to import over existing metadata \
             (use --force to rename the old dir aside; your data lives on the device)",
            meta_dir.display()
        )));
    }
    let backup = meta_dir.with_extension(format!(
        "meta.bak-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    ));
    tracing::warn!(
        "meta-import --force: renaming existing meta dir {} → {}",
        meta_dir.display(),
        backup.display()
    );
    std::fs::rename(meta_dir, &backup)?;
    std::fs::create_dir_all(meta_dir)?;
    Ok(())
}

/// 写敏感文件:内容 + 0600 权限。
fn write_private(path: &Path, bytes: &[u8]) -> fs3_core::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut f = std::fs::File::create(path)?;
    std::io::Write::write_all(&mut f, bytes)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}
