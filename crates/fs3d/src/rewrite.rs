//! 值格式在线重写(M10 V5-3;ADR-11 D0 / DESIGN-FUTURE §2.4;M16 A1
//! ADR-19 DA4 扩展):ObjectMeta 旧版本值(v2~v6)→ 当前版本逐键重写工具。
//!
//! 背景:v1.0.x 写入的存量对象值 = `[版本字节 2] + postcard(v2 结构)`;v1.1
//! 起写入恒为 v3,M11 起恒为 v4(ADR-12 D-E3 尾部追加 part_checksums),
//! M13 起恒 v5(compressed),M15 起恒 v6(requested_storage_class),M16
//! 起恒 v7(storage_class/restore_state)。双读(decode_value)使旧值零迁移
//! 可读,但为:
//! ① 消除每次读的回退解码尝试;② 落实「重写完成前禁回滚」的升级纪律
//! (§2.4:v3+ 值旧二进制拒绝解码,回滚到 v1.0.x 仅在重写**未完成**且
//! 无 v1.1 新写时可行;v7 同理禁回滚 v2.1.x,完成标记 =
//! `s:value_rewrite_v7_done`,ADR-19 DA4)—— 本工具在停机/维护窗口把
//! 全部对象值归一重写为当前版本。
//!
//! 语义:
//! - 幂等:重写 = 双读结果按当前版本重编码同值;已当前版本的值跳过,
//!   中断可续跑;
//! - 跳过:当前版本值与删除标记(标记恒为当前版本 —— 版本化是 v1.1 新键,
//!   写入恒当前版本;若防御性遇到旧版本标记仍重写,完成不变量 = 全库零
//!   旧版本);
//! - 节流:Tier2 语义(每秒最多 `rate` 次重写,默认 500/s;0 = 不限速);
//! - 暂停:`--pause-file <path>` 存在即暂停(轮询 1s,`touch` 暂停 /
//!   `rm` 恢复);
//! - 完成:errors=0 且全库无旧版本残留 → 落完成标记(v3 标记 + v7 标记,
//!   持久,幂等;v6→v7 重写不改变对象语义 —— v6 值升格 storage_class/
//!   restore_state = None,见 ObjectMetaV6::from)。
//!
//! 用法(停机或维护窗口;与运行中的服务互斥 —— rocksdb 目录锁保证):
//! ```bash
//! fasts3d rewrite-values --device <dev> --meta-dir <dir> --rate 500
//! ```

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use clap::Args;
use fs3_core::Result;
use fs3_meta::{MetaConfig, MetaStore};

#[derive(Args, Debug, Clone)]
pub struct RewriteValuesArgs {
    /// 每秒重写条目数(0 = 不限速;Tier2 节流)
    #[arg(long, default_value_t = 500)]
    pub rate: u64,
    /// 存在即暂停(轮询 1s;`touch` 暂停,`rm` 恢复)
    #[arg(long)]
    pub pause_file: Option<PathBuf>,
    /// 只探测值版本分布(v2~v6/当前可读计数)不重写(演练/巡检断言用)
    #[arg(long)]
    pub count_only: bool,
}

/// 跑一轮值格式重写:旧版本值 → 当前版本(全部 o: 键,含版本条目)。
pub fn run_rewrite(meta_dir: &Path, args: &RewriteValuesArgs) -> Result<RewriteReport> {
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;
    if store.value_rewrite_v3_done()? {
        println!(
            "rewrite-values: done marker present (s:value_rewrite_v3_done); idempotent re-run"
        );
    }
    if store.value_rewrite_v7_done()? {
        println!(
            "rewrite-values: done marker present (s:value_rewrite_v7_done); idempotent re-run"
        );
    }
    let all = store.snapshot_all_objects_raw()?;
    let mut rewritten = 0usize;
    let mut skipped_cur = 0usize;
    let mut skipped_marker = 0usize;
    let mut errors = 0usize;
    let start = std::time::Instant::now();
    for e in &all {
        // 暂停语义:pause-file 存在即阻塞轮询(每 1s),移除后续跑
        if let Some(pf) = &args.pause_file {
            while pf.exists() {
                println!(
                    "rewrite-values: paused ({} exists; remove to resume)",
                    pf.display()
                );
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        // 已当前版本:跳过(删除标记恒落此处 —— 版本化键是 v1.1 新键,
        // 写入恒当前版本)
        if e.value_version == fs3_core::OBJECT_META_VERSION {
            if e.meta.is_delete_marker {
                skipped_marker += 1;
            } else {
                skipped_cur += 1;
            }
            continue;
        }
        // Tier2 节流:平均速率 ≤ rate/s(重写计次,跳过不计)
        if args.rate > 0 && rewritten > 0 {
            let target = rewritten as f64 / args.rate as f64;
            let lag = target - start.elapsed().as_secs_f64();
            if lag > 0.0 {
                std::thread::sleep(Duration::from_secs_f64(lag.min(1.0)));
            }
        }
        // 重写 = 双读归一后的同值当前版本重编码(幂等);单事务,不改统计/分配
        match store.commit_object_meta_update(&e.raw_key, &e.meta) {
            Ok(_) => rewritten += 1,
            Err(err) => {
                errors += 1;
                println!("rewrite-values: FAILED {}/{}: {err}", e.bucket, e.key);
            }
        }
    }
    let report = RewriteReport {
        scanned: all.len(),
        rewritten,
        skipped_cur,
        skipped_marker,
        errors,
        elapsed_secs: start.elapsed().as_secs_f64(),
    };
    // 完成判定:零错误且重写后全库无旧版本残留 → 落完成标记(回滚门禁)
    if errors == 0 {
        let c = store.count_object_value_versions()?;
        if c.all_current() {
            // v2→v3 标记(v1.0.x 回滚门禁;无 v2 残留即可落)
            if !c.has_v2() {
                store.mark_value_rewrite_v3_done()?;
            }
            // v6→v7 标记(M16 A1,ADR-19 DA4:v2.1.x 回滚门禁;全部值
            // 已归一当前版本即可落)
            store.mark_value_rewrite_v7_done()?;
            println!(
                "rewrite-values: all values at current version; done markers written \
                 (s:value_rewrite_v3_done, s:value_rewrite_v7_done)."
            );
            println!(
                "rewrite-values: 回滚纪律(§2.4):此后禁止回滚到 v1.0.x/v2.1.x 二进制(其拒绝 \
                 解码 v3+/v7 值);回滚只能走「meta-export 快照 + 底层卷快照」恢复路径 \
                 (fasts3d upgrade 注释)。"
            );
        } else {
            println!(
                "rewrite-values: {} old value(s) remain (new writes during run?); re-run to converge",
                c.v2 + c.v3 + c.v4 + c.v5 + c.v6
            );
        }
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy)]
pub struct RewriteReport {
    pub scanned: usize,
    pub rewritten: usize,
    pub skipped_cur: usize,
    pub skipped_marker: usize,
    pub errors: usize,
    pub elapsed_secs: f64,
}

/// 值格式探测:全库 o: 值按首字节统计(v2~v6/当前;只读;供演练脚本与
/// 完成判定断言「重写后无旧版本残留」)。
pub fn count_value_versions(meta_dir: &Path) -> Result<fs3_meta::ValueVersionCount> {
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;
    store.count_object_value_versions()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_meta::{AllocDraft, StatsDelta};

    /// v2 值格式夹具(字段与 fs3-core ObjectMetaV2 逐一对应;postcard
    /// 字段序即编码序 —— 与 fs3-meta 测试夹具同构)。
    #[derive(serde::Serialize)]
    struct ObjectMetaV2Fixture {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<fs3_core::Segment>,
        content_type: String,
        user_meta: Vec<(String, String)>,
        inline: Option<Vec<u8>>,
        parts: Vec<u64>,
        resp_headers: Vec<(String, String)>,
    }

    fn encode_v2_value(m: &fs3_core::ObjectMeta) -> Vec<u8> {
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

    fn object_meta(size: u64, seed: u8) -> fs3_core::ObjectMeta {
        let mut etag = [0u8; 16];
        etag[0] = seed;
        fs3_core::ObjectMeta {
            size,
            etag,
            mtime: 1_700_000_000,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![("k".into(), "v".into())],
            inline: Some(vec![seed; size as usize]),
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
            compressed: None,

            ..Default::default()
        }
    }

    /// v6 值格式夹具(M16 A1:M15 C1 现格式,无 storage_class/restore_state
    /// 尾部字段;postcard 字段序即编码序)。
    #[derive(serde::Serialize)]
    struct ObjectMetaV6Fixture {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<fs3_core::Segment>,
        content_type: String,
        user_meta: Vec<(String, String)>,
        inline: Option<Vec<u8>>,
        parts: Vec<u64>,
        resp_headers: Vec<(String, String)>,
        version_id: Option<[u8; 16]>,
        is_delete_marker: bool,
        tags: Vec<(String, String)>,
        sse: Option<fs3_core::SseInfo>,
        checksum: Option<fs3_core::ChecksumInfo>,
        retention: Option<fs3_core::Retention>,
        legal_hold: bool,
        part_checksums: Vec<Option<fs3_core::ChecksumInfo>>,
        compressed: Option<fs3_core::CompressionInfo>,
        requested_storage_class: Option<String>,
    }

    fn encode_v6_value(m: &fs3_core::ObjectMeta) -> Vec<u8> {
        let f = ObjectMetaV6Fixture {
            size: m.size,
            etag: m.etag,
            mtime: m.mtime,
            extents: m.extents.clone(),
            content_type: m.content_type.clone(),
            user_meta: m.user_meta.clone(),
            inline: m.inline.clone(),
            parts: m.parts.clone(),
            resp_headers: m.resp_headers.clone(),
            version_id: m.version_id,
            is_delete_marker: m.is_delete_marker,
            tags: m.tags.clone(),
            sse: m.sse.clone(),
            checksum: m.checksum.clone(),
            retention: m.retention.clone(),
            legal_hold: m.legal_hold,
            part_checksums: m.part_checksums.clone(),
            compressed: m.compressed.clone(),
            requested_storage_class: Some("GLACIER".into()),
        };
        let mut v = vec![6u8];
        v.extend(postcard::to_allocvec(&f).unwrap());
        v
    }

    fn bucket_meta() -> fs3_core::BucketMeta {
        fs3_core::BucketMeta {
            created: 1,
            owner: "test".into(),
            stats: fs3_core::BucketStats::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
            // M20 D2:无桶默认 KMS key
            default_kms_key: None,
        }
    }

    /// 混合库:当前版本单键 + v2 单键 + 当前版本版本条目 + v2 版本条目 + 删除标记。
    fn mixed_store(dir: &Path) -> (MetaStore, [u8; 16], [u8; 16]) {
        let s = MetaStore::open(dir, &MetaConfig::default()).unwrap();
        s.commit_bucket_put("b1", &bucket_meta()).unwrap();
        let zero = StatsDelta::default();
        let draft = AllocDraft::default();
        // v3 单键(当前二进制写入恒 v3)
        s.commit_object_put("b1", "new", &object_meta(8, 1), draft.clone(), zero.clone())
            .unwrap();
        // v3 版本条目 + v3 删除标记(Enabled 形态)
        let vk_data = [0x11; 16];
        let m_data = fs3_core::ObjectMeta {
            version_id: Some(vk_data),
            ..object_meta(16, 2)
        };
        s.commit_object_put_version(
            "b1",
            "vk-obj",
            &vk_data,
            &m_data,
            draft.clone(),
            zero.clone(),
        )
        .unwrap();
        let vk_marker = [0x22; 16];
        let marker = fs3_core::ObjectMeta {
            size: 0,
            inline: None,
            user_meta: vec![],
            version_id: Some(vk_marker),
            is_delete_marker: true,
            ..object_meta(0, 3)
        };
        s.commit_object_delete_current(
            "b1",
            "vk-obj",
            Some(&vk_marker),
            &marker,
            draft,
            zero.clone(),
        )
        .unwrap();
        // v2 存量值(单键 + 版本键;模拟 v1.0.x 遗留)
        s.put_object_value_raw("b1", "old", None, &encode_v2_value(&object_meta(24, 4)))
            .unwrap();
        let vk_v2 = [0x33; 16];
        s.put_object_value_raw(
            "b1",
            "vk-obj",
            Some(&vk_v2),
            &encode_v2_value(&object_meta(32, 5)),
        )
        .unwrap();
        (s, vk_data, vk_v2)
    }

    #[test]
    fn rewrite_mixed_store_converges_to_v3() {
        // V5-3 主验收:v2/v3 混合库 → rewrite → 全部 v3、内容一致(双读
        // 口径)、统计不变、完成标记落盘、幂等续跑 rewritten=0。
        let dir = tempfile::tempdir().unwrap();
        let (s, vk_data, vk_v2) = mixed_store(dir.path());
        let c = s.count_object_value_versions().unwrap();
        assert_eq!((c.v2, c.cur), (2, 3));
        let stats_before = s.get_bucket("b1").unwrap().unwrap().stats;
        drop(s);

        let report = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 0,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report.scanned, 5);
        assert_eq!(report.rewritten, 2, "两个 v2 值被重写");
        assert_eq!(report.skipped_cur, 2, "v3 数据条目跳过");
        assert_eq!(report.skipped_marker, 1, "v3 删除标记跳过");
        assert_eq!(report.errors, 0);
        let c = count_value_versions(dir.path()).unwrap();
        assert_eq!((c.v2, c.cur), (0, 5));

        // 内容一致(双读口径 = 重写前后的 decode 结果相等)+ 统计不变
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert_eq!(s.get_object("b1", "old").unwrap().unwrap().size, 24);
        assert_eq!(
            s.get_object("b1", "old").unwrap().unwrap().inline,
            Some(vec![4u8; 24])
        );
        let v = s
            .get_object_version("b1", "vk-obj", &vk_v2)
            .unwrap()
            .unwrap();
        assert_eq!(v.size, 32);
        assert_eq!(v.inline, Some(vec![5u8; 32]));
        // v3 条目字节级不受影响(version_id 保留)
        let d = s
            .get_object_version("b1", "vk-obj", &vk_data)
            .unwrap()
            .unwrap();
        assert_eq!(d.version_id, Some(vk_data));
        assert_eq!(s.get_bucket("b1").unwrap().unwrap().stats, stats_before);
        // 完成标记已落
        assert!(s.value_rewrite_v3_done().unwrap());
        drop(s);

        // 幂等续跑:全部已 v3 → rewritten=0,无错误
        let report2 = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 0,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report2.rewritten, 0);
        assert_eq!(report2.skipped_cur, 4);
        assert_eq!(report2.skipped_marker, 1);
        assert_eq!(report2.errors, 0);
    }

    #[test]
    fn rewrite_pause_file_blocks_until_removed() {
        // --pause-file 语义:文件存在即暂停(轮询),移除后续跑完成。
        let dir = tempfile::tempdir().unwrap();
        let (s, _, _) = mixed_store(dir.path());
        drop(s);
        let pause = dir.path().join("pause");
        std::fs::write(&pause, b"").unwrap();

        let meta_dir = dir.path().to_path_buf();
        let pf = pause.clone();
        let handle = std::thread::spawn(move || {
            run_rewrite(
                &meta_dir,
                &RewriteValuesArgs {
                    rate: 0,
                    pause_file: Some(pf),
                    count_only: false,
                },
            )
            .unwrap()
        });
        // 暂停中:200ms 内不应完成(轮询周期 1s)
        std::thread::sleep(Duration::from_millis(200));
        assert!(!handle.is_finished(), "pause-file 存在时必须阻塞");
        std::fs::remove_file(&pause).unwrap();
        let report = handle.join().unwrap();
        assert_eq!(report.errors, 0);
        let c = count_value_versions(dir.path()).unwrap();
        assert_eq!((c.v2, c.cur), (0, 5));
    }

    #[test]
    fn rewrite_throttle_paces_writes() {
        // Tier2 节流:rate=5/s 重写 2 个 v2 值,耗时 ≥ 0.2s(平均速率闸门)。
        let dir = tempfile::tempdir().unwrap();
        let (s, _, _) = mixed_store(dir.path());
        drop(s);
        let report = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 5,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report.rewritten, 2);
        assert!(
            report.elapsed_secs >= 0.2,
            "throttled elapsed: {}",
            report.elapsed_secs
        );
    }

    #[test]
    fn rewrite_rejects_missing_dir_gracefully() {
        // 空库(无对象)= 零扫描,直接落完成标记(全新 v1.1 部署形态)
        let dir = tempfile::tempdir().unwrap();
        let report = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 0,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report.scanned, 0);
        assert_eq!(report.errors, 0);
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert!(s.value_rewrite_v3_done().unwrap());
    }

    #[test]
    fn rewrite_v6_to_v7_converges_and_marks() {
        // M16 A1 主验收(ADR-19 DA4):v6 存量值(M15 C1 现格式,含 GLACIER
        // 请求类)→ rewrite → 全部 v7、内容一致(双读口径)、请求类保留、
        // 真实类不升格(None = STANDARD)、v7 完成标记落盘、幂等续跑。
        let dir = tempfile::tempdir().unwrap();
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        s.commit_bucket_put("b1", &bucket_meta()).unwrap();
        let zero = StatsDelta::default();
        let draft = AllocDraft::default();
        // 当前版本(v7)条目 + v6 存量值条目
        s.commit_object_put("b1", "new", &object_meta(8, 1), draft.clone(), zero.clone())
            .unwrap();
        s.put_object_value_raw("b1", "old", None, &encode_v6_value(&object_meta(24, 4)))
            .unwrap();
        let c0 = s.count_object_value_versions().unwrap();
        assert_eq!((c0.v2, c0.v6, c0.cur), (0, 1, 1));
        let stats_before = s.get_bucket("b1").unwrap().unwrap().stats;
        drop(s);

        let report = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 0,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report.scanned, 2);
        assert_eq!(report.rewritten, 1, "v6 值被重写为 v7");
        assert_eq!(report.skipped_cur, 1, "v7 数据条目跳过");
        assert_eq!(report.errors, 0);
        let c = count_value_versions(dir.path()).unwrap();
        assert_eq!((c.v2, c.v6, c.cur), (0, 0, 2));

        // 内容一致 + 请求类保留 + 真实类不升格(ADR-19 DA4:v6 存量
        // GLACIER 请求类仅审计,升级不改变对象形态)
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        let old = s.get_object("b1", "old").unwrap().unwrap();
        assert_eq!(old.size, 24);
        assert_eq!(old.inline, Some(vec![4u8; 24]));
        assert_eq!(
            old.requested_storage_class.as_deref(),
            Some("GLACIER"),
            "请求类随重写保留"
        );
        assert_eq!(old.storage_class, None, "v6 值不升格真归档");
        assert_eq!(old.restore_state, None);
        assert_eq!(s.get_bucket("b1").unwrap().unwrap().stats, stats_before);
        assert!(s.value_rewrite_v7_done().unwrap(), "v7 完成标记已落");
        drop(s);

        // 幂等续跑:全部已 v7 → rewritten=0
        let report2 = run_rewrite(
            dir.path(),
            &RewriteValuesArgs {
                rate: 0,
                pause_file: None,
                count_only: false,
            },
        )
        .unwrap();
        assert_eq!(report2.rewritten, 0);
        assert_eq!(report2.skipped_cur, 2, "两条 v7 条目均跳过");
        assert_eq!(report2.errors, 0);
    }
}
