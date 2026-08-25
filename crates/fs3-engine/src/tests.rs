//! 引擎测试:基础回归(段模型适配)+ ADR-9 打包语义 + 属性测试。
//! 压缩专项测试见 compaction.rs。

use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;

fn test_cfg(dev: &Path, meta_dir: &Path) -> EngineConfig {
    EngineConfig {
        devices: vec![dev.to_path_buf()],
        meta_dir: meta_dir.to_path_buf(),
        // 单元测试默认关后台压缩(确定性);压缩专项测试自行开启/前台调用
        compaction: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn setup() -> (tempfile::TempDir, EngineConfig) {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = test_cfg(&img, &dir.path().join("meta"));
    (dir, cfg)
}

fn open_engine(cfg: &EngineConfig) -> Engine {
    let mut e = Engine::open(cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e
}

/// M13 M1-2:双设备池测试装置(两张不同规格的镜像;`sizes` 为各设备字节)。
fn setup_multi(sizes: &[u64]) -> (tempfile::TempDir, Vec<std::path::PathBuf>, EngineConfig) {
    let dir = tempfile::tempdir().unwrap();
    let mut imgs = Vec::new();
    for (i, size) in sizes.iter().enumerate() {
        let img = dir.path().join(format!("disk{i}.img"));
        std::fs::File::create(&img).unwrap().set_len(*size).unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        imgs.push(img);
    }
    let meta_dir = dir.path().join("meta");
    // 池清单预置(等价 device-add;引擎拒绝「改配置入池」)
    seed_pool_manifest(&meta_dir, &imgs);
    let cfg = EngineConfig {
        devices: imgs.clone(),
        meta_dir: meta_dir.clone(),
        compaction: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    (dir, imgs, cfg)
}

/// 按设备序写入池清单(ADR-15 DM1';uuid/容量取自各设备超块;等价
/// `fasts3d device-add` 的持久化动作,M3-1 起由命令承担)。
fn seed_pool_manifest(meta_dir: &std::path::Path, devices: &[std::path::PathBuf]) {
    let store = fs3_meta::MetaStore::open(
        meta_dir,
        &fs3_meta::MetaConfig {
            flush_every_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            sync_mode: fs3_meta::SyncMode::Group,
            cache_capacity: None,
        },
    )
    .unwrap();
    let entries = devices
        .iter()
        .map(|p| {
            let dev = fs3_device::open_device(p, true).unwrap();
            let sb = fs3_device::read_superblock(dev.as_ref()).unwrap();
            fs3_core::pool::DeviceEntry {
                uuid: sb.uuid,
                path: p.display().to_string(),
                capacity: sb.data_end,
                extent_count: sb.extent_count(),
                weight: 1,
                added_at: 0,
            }
        })
        .collect();
    store
        .save_pool(&fs3_core::pool::PoolManifest { devices: entries })
        .unwrap();
    store.flush().unwrap();
    drop(store);
}

/// CompletePart 便捷构造(M11 C1-4;无逐分片 checksum 声明)。
fn cp(part_number: u32, etag_hex: String) -> CompletePart {
    CompletePart {
        part_number,
        etag_hex,
        checksum: None,
    }
}

/// 确定性伪随机数据(种子区分内容)。
fn rnd(len: usize, seed: u8) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed) % 251)
        .collect()
}

// ─────────────────────────── 基础回归(适配段模型) ───────────────────────────

#[test]
fn put_get_delete_roundtrip() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);

    // 空对象
    let m = e.put("b1", "empty", &mut Cursor::new(Vec::new())).unwrap();
    assert_eq!(m.size, 0);
    assert_eq!(m.extents.len(), 0);

    // 大对象(5MiB > 4MiB-4KiB 容量):首个段独占整块,尾段打包(开放)
    let big: Vec<u8> = (0..(5 * 1024 * 1024u32)).map(|i| (i % 253) as u8).collect();
    let m = e.put("b1", "big", &mut Cursor::new(big.clone())).unwrap();
    assert_eq!(m.size, big.len() as u64);
    assert!(m.extents.len() >= 2, "expected >=2 segments");
    // 首个段:独占(整块,元数据 crcs 为空);尾段:打包(带 crcs)
    assert_eq!(m.extents[0].offset, 0);
    assert!(m.extents[0].crcs.is_empty(), "exclusive segment crcs empty");
    assert!(!m.extents[1].crcs.is_empty(), "packed tail segment crcs");

    // 小对象(单段内):与 big 的打包尾段共享同一开放 extent
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let small_meta = e
        .put("b1", "small", &mut Cursor::new(data.clone()))
        .unwrap();
    assert_eq!(small_meta.size, data.len() as u64);
    assert_eq!(small_meta.extents.len(), 1);
    assert_eq!(small_meta.extents[0].extent_id, m.extents[1].extent_id);
    assert_ne!(small_meta.extents[0].extent_id, m.extents[0].extent_id);
    assert!(!small_meta.extents[0].crcs.is_empty(), "打包段带 crcs");
    let mut out = Vec::new();
    e.get_to("b1", "small", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, data);

    let mut out = Vec::new();
    e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, big);

    // Range
    let mut out = Vec::new();
    e.get_to("b1", "big", 100..200, &mut out).unwrap();
    assert_eq!(out, &big[100..200]);
    let mut out = Vec::new();
    e.get_to("b1", "big", big.len() as u64 - 10..u64::MAX, &mut out)
        .unwrap();
    assert_eq!(out, &big[big.len() - 10..]);

    // 删除
    assert!(e.delete("b1", "small").unwrap().is_some());
    assert!(e.delete("b1", "small").unwrap().is_none());
    assert!(e.delete("b1", "big").unwrap().is_some());
    assert_eq!(e.allocator().allocated_count(), 0, "all extents freed");
    e.close().unwrap();
}

#[test]
fn overwrite_releases_old_segments() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let d1 = vec![1u8; 100_000];
    let d2 = vec![2u8; 200_000];
    e.put("b1", "k", &mut Cursor::new(d1)).unwrap();
    e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap();
    let m = e.head("b1", "k").unwrap().unwrap();
    assert_eq!(m.size, 200_000);
    assert_eq!(e.allocator().allocated_count(), 1);
    let mut out = Vec::new();
    e.get_to("b1", "k", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d2);
    e.close().unwrap();
}

/// M11 G-2:Off 桶内联覆盖不得把 objects +1。历史口径 `counted = 段非空`
/// 把内联(extents 空)当成新对象,覆盖后再被生命周期物理删除会留下
/// admin stats (1,0) 而列表为空。
#[test]
fn off_inline_overwrite_does_not_inflate_object_count() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.put("b1", "k", &mut Cursor::new(vec![1u8; 100])).unwrap();
    assert_eq!(stats_of(&e), (1, 100));
    e.put("b1", "k", &mut Cursor::new(vec![2u8; 200])).unwrap();
    assert_eq!(
        stats_of(&e),
        (1, 200),
        "inline overwrite must not increment objects"
    );
    let big = vec![3u8; 100_000];
    e.put("b1", "k", &mut Cursor::new(big)).unwrap();
    assert_eq!(stats_of(&e), (1, 100_000), "inline→extent overwrite");
    e.put("b1", "k", &mut Cursor::new(vec![4u8; 50])).unwrap();
    assert_eq!(stats_of(&e), (1, 50), "extent→inline overwrite");
    assert!(e.delete("b1", "k").unwrap().is_some());
    assert_eq!(stats_of(&e), (0, 0));
    e.close().unwrap();
}

#[test]
fn put_interrupted_rolls_back() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    struct FailingReader {
        remaining: usize,
    }
    impl Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client gone",
                ));
            }
            let n = buf.len().min(1024).min(self.remaining);
            buf[..n].fill(0xEE);
            self.remaining -= n;
            Ok(n)
        }
    }
    let r = e.put(
        "b1",
        "partial",
        &mut FailingReader {
            remaining: 3 * 1024 * 1024,
        },
    );
    assert!(r.is_err());
    // 未提交事务:对象不可见;位图回滚后开放会话丢弃(非水位回退覆写)
    assert!(e.head("b1", "partial").unwrap().is_none());
    assert_eq!(e.allocator().allocated_count(), 0);
    // 释放后可继续写入(新 extent)
    let d = vec![9u8; 100_000];
    e.put("b1", "after", &mut Cursor::new(d.clone())).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "after", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d);
    assert_eq!(e.allocator().allocated_count(), 1);
    e.close().unwrap();
}

/// 已提交打包对象之后的失败 PUT 必须回退到该对象之后,不得从
/// committed_end=0 覆写前驱(开放 extent 在进程内跨多次 PUT 存活)。
#[test]
fn abort_does_not_overwrite_committed_packed_predecessor() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let a = rnd(100_000, 11);
    e.put("b1", "a", &mut Cursor::new(a.clone())).unwrap();
    struct FailingReader {
        remaining: usize,
    }
    impl Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client gone",
                ));
            }
            let n = buf.len().min(1024).min(self.remaining);
            buf[..n].fill(0xEE);
            self.remaining -= n;
            Ok(n)
        }
    }
    assert!(e
        .put("b1", "partial", &mut FailingReader { remaining: 200_000 },)
        .is_err());
    let c = rnd(80_000, 13);
    e.put("b1", "c", &mut Cursor::new(c.clone())).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "a", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, a, "committed predecessor must survive abort_draft");
    out.clear();
    e.get_to("b1", "c", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, c);
    e.close().unwrap();
}

/// 同上,SSE-C 网格:覆写密文表现为 GCM tag 失败而非明文错字节。
#[test]
fn abort_does_not_overwrite_committed_sse_packed_predecessor() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let a = rnd(100_000, 17);
    e.put_with_meta(
        "b1",
        "a",
        &mut Cursor::new(a.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    struct FailingReader {
        remaining: usize,
    }
    impl Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client gone",
                ));
            }
            let n = buf.len().min(1024).min(self.remaining);
            buf[..n].fill(0xEE);
            self.remaining -= n;
            Ok(n)
        }
    }
    assert!(e
        .put_with_meta(
            "b1",
            "partial",
            &mut FailingReader { remaining: 200_000 },
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .is_err());
    let mut out = vec![0u8; a.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "a",
            None,
            0,
            &mut out,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(&out[..n], a.as_slice(), "SSE predecessor must decrypt");
    e.close().unwrap();
}

/// 已提交打包 SSE 对象之后的失败 upload_part 同样不得覆写前驱。
#[test]
fn abort_upload_part_does_not_overwrite_committed_sse_packed_predecessor() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let a = rnd(100_000, 19);
    e.put_with_meta(
        "b1",
        "a",
        &mut Cursor::new(a.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    struct FailingReader {
        remaining: usize,
    }
    impl Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client gone",
                ));
            }
            let n = buf.len().min(1024).min(self.remaining);
            buf[..n].fill(0xEE);
            self.remaining -= n;
            Ok(n)
        }
    }
    let uid = create_sse_upload(&mut e, "partial-part");
    assert!(e
        .upload_part(
            &uid,
            1,
            &mut FailingReader { remaining: 200_000 },
            None,
            Some(&key),
        )
        .is_err());
    let c = rnd(80_000, 23);
    e.put_with_meta(
        "b1",
        "c",
        &mut Cursor::new(c.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    let mut out = vec![0u8; a.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "a",
            None,
            0,
            &mut out,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(
        &out[..n],
        a.as_slice(),
        "SSE predecessor must survive failed part"
    );
    out = vec![0u8; c.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "c",
            None,
            0,
            &mut out,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(&out[..n], c.as_slice());
    e.close().unwrap();
}

#[test]
fn recovery_after_clean_close() {
    let (_d, cfg) = setup();
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "a", &mut Cursor::new(data.clone())).unwrap();
        e.close().unwrap();
    }
    let mut e = Engine::open(&cfg).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "a", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, data);
    assert_eq!(e.allocator().allocated_count(), 1);
    assert!(e.allocator().leaks().is_empty());
    e.close().unwrap();
}

#[test]
fn recovery_without_close_resumes_open_extent() {
    let (_d, cfg) = setup();
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "a", &mut Cursor::new(data.clone())).unwrap();
        // 显式 flush 使 rocksdb 落盘(模拟组提交窗口已过)
        e.meta().flush().unwrap();
        e.abort(); // 模拟 kill -9:跳过最终检查点;开放 extent 无头残留
    }
    let mut e = Engine::open(&cfg).unwrap();
    assert_eq!(e.allocator().allocated_count(), 1);
    assert!(e.allocator().leaks().is_empty());
    // 开放 extent 被续写:watermark = 活段最大 end
    e.put("b1", "b", &mut Cursor::new(vec![7u8; 100_000]))
        .unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "a", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, data);
    let mut out = Vec::new();
    e.get_to("b1", "b", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, vec![7u8; 100_000]);
    assert_eq!(e.allocator().allocated_count(), 1, "续写不新开 extent");
    // 恢复水位必须为 4KiB 对齐物理端点(段长记实际字节,尾垫补零):
    // 300_000 的非对齐逻辑端点 → 续写段起点 = align_up(300_000, 4KiB);
    // 非对齐恢复水位会让 O_DIRECT 追加 EINVAL(ext4/xfs;tmpfs 不强制)。
    let segs = &e.meta().get_object("b1", "b").unwrap().unwrap().extents;
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].offset % 4096, 0, "续写段起点必须 4KiB 对齐");
    assert_eq!(segs[0].offset, 303_104, "align_up(300_000, 4KiB)");
    e.close().unwrap();
}

#[test]
fn verify_reads_detects_corruption() {
    let (_d, cfg) = setup();
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let img_path = cfg.devices[0].clone();
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "k", &mut Cursor::new(data)).unwrap();
        e.close().unwrap();
    }
    // 篡改数据区第一字节(64MiB 设备:数据区起点 = 1MiB + 2×4KiB)
    let dev = fs3_device::open_device(&img_path, false).unwrap();
    let mut buf = fs3_device::AlignedBuffer::new(4096).unwrap();
    let off = 1024 * 1024 + 2 * 4096 + 4096;
    dev.pread_aligned(buf.as_mut_slice(), off).unwrap();
    buf.as_mut_slice()[0] ^= 0xFF;
    dev.pwrite_aligned(buf.as_slice(), off).unwrap();

    let mut cfg2 = cfg.clone();
    cfg2.verify_reads = false;
    {
        let mut e = Engine::open(&cfg2).unwrap();
        let mut out = Vec::new();
        e.get_to("b1", "k", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out.len(), 100_000);
        e.close().unwrap();
    } // 释放 rocksdb 锁

    let mut cfg3 = cfg.clone();
    cfg3.verify_reads = true;
    let mut e = Engine::open(&cfg3).unwrap();
    let mut out = Vec::new();
    let r = e.get_to("b1", "k", 0..u64::MAX, &mut out);
    assert!(r.is_err(), "verify_reads must detect corruption");
    e.close().unwrap();
}

#[test]
fn checkpoint_rolls_bitmap() {
    let (_d, cfg) = setup();
    let data = vec![7u8; 100_000];
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "k", &mut Cursor::new(data)).unwrap();
        e.checkpoint().unwrap();
        assert_eq!(e.checkpoint.lock().unwrap().seq, 2); // bucket create + put
        e.close().unwrap();
    }
    let mut e = Engine::open(&cfg).unwrap();
    assert_eq!(e.allocator().allocated_count(), 1);
    assert!(e.allocator().leaks().is_empty());
    e.close().unwrap();
}

#[test]
fn trusted_clock_persists_high_water_across_reopen() {
    // M12 W1-1:启动落盘;检查点刷新后重开 last_wall 不回退。
    let (_d, cfg) = setup();
    let first = {
        let mut e = open_engine(&cfg);
        let t0 = e.trusted_clock_state().last_wall;
        assert!(t0 > 0, "首次启动以墙钟为初值");
        e.debug_inject_clock(t0 + 3600, 10_000_000_000);
        e.debug_refresh_trusted_clock().unwrap();
        assert_eq!(e.trusted_clock_state().last_wall, t0 + 3600);
        e.close().unwrap();
        t0 + 3600
    };
    let e = Engine::open(&cfg).unwrap();
    assert!(
        e.trusted_clock_state().last_wall >= first,
        "重开保留 last_wall 高水位"
    );
    // 注入回拨:同一次采样的 mono 不回退,lock_now 不得低于高水位
    let st = e.trusted_clock_state();
    e.debug_inject_clock(st.last_wall - 86_400, st.last_mono_ns);
    e.debug_refresh_trusted_clock().unwrap();
    assert!(
        e.trusted_clock_divergence() >= 86_400,
        "回拨后 gauge 记录落后秒数"
    );
    assert!(e.trusted_clock_divergence_events() >= 1);
    let mut e = e;
    e.close().unwrap();
}

#[test]
fn trusted_clock_rollback_does_not_unexpire() {
    // 已到期(until < last_wall):回拨墙钟不得复活。
    let (_d, cfg) = setup();
    let e = open_engine(&cfg);
    let wall = 2_000_000_000i64;
    e.debug_inject_clock(wall, 0);
    e.debug_refresh_trusted_clock().unwrap();
    e.debug_inject_clock(wall - 3600, 1_000_000_000);
    let lock = e.lock_now();
    assert!(lock >= wall, "回拨后 lock_now 仍取单调下界");
    let trusted = e
        .trusted_clock_state()
        .trusted_now(wall - 3600, 1_000_000_000);
    assert!(fs3_core::retention_expired(wall - 1, wall - 3600, trusted));
    let mut e = e;
    e.close().unwrap();
}

#[test]
fn list_objects_prefix() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    for k in ["a/1", "a/2", "b/1"] {
        e.put("b1", k, &mut Cursor::new(vec![1u8; 10])).unwrap();
    }
    let all = e.list_objects("b1", "").unwrap();
    assert_eq!(all.len(), 3);
    let a = e.list_objects("b1", "a/").unwrap();
    assert_eq!(a.len(), 2);
    e.close().unwrap();
}

#[test]
fn delete_bucket_force() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.put("b1", "k", &mut Cursor::new(vec![1u8; 100])).unwrap();
    assert!(e.delete_bucket("b1", false).is_err());
    e.delete_bucket("b1", true).unwrap();
    assert!(e.list_buckets().unwrap().is_empty());
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn multi_extent_boundary_split() {
    // 回归:输入 chunk 跨 extent 边界拆分时,不得越界写坏下一 extent 头
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let cap = e.superblock().extent_capacity();
    // 2 个整 extent + 余量(触发第 3 个 extent,且第 64 个输入 chunk 拆分)
    let total = 2 * cap + 300_000;
    let data: Vec<u8> = (0..total as u32).map(|i| (i % 253) as u8).collect();
    let m = e.put("b1", "big3", &mut Cursor::new(data.clone())).unwrap();
    assert_eq!(m.size, total);
    assert_eq!(m.extents.len(), 3);
    let mut out = Vec::new();
    e.get_to("b1", "big3", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, data, "3-segment object must roundtrip exactly");
    e.close().unwrap();
}

#[test]
fn inline_small_objects_zero_device_io() {
    // E3:≤ small_object_limit 的对象内联进元数据,零设备 I/O
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let data: Vec<u8> = (0..30_000u32).map(|i| (i % 251) as u8).collect();
    let m = e
        .put("b1", "small", &mut Cursor::new(data.clone()))
        .unwrap();
    assert_eq!(m.size, data.len() as u64);
    assert!(m.extents.is_empty(), "inline object must not use extents");
    assert_eq!(m.inline.as_ref().unwrap(), &data);
    assert_eq!(e.allocator().allocated_count(), 0, "zero device allocation");

    // 读回
    let mut out = Vec::new();
    e.get_to("b1", "small", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, data);
    // read_at 原语同样支持内联
    let mut buf = vec![0u8; 1024];
    let n = e.read_at("b1", "small", 100, &mut buf).unwrap();
    assert_eq!(n, 1024);
    assert_eq!(&buf[..1024], &data[100..1124]);
    e.close().unwrap();
}

#[test]
fn inline_threshold_boundary() {
    // 阈值边界:limit 内内联,limit+1 落盘
    let (_d, mut cfg) = setup();
    cfg.small_object_limit = 4096;
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();

    let exact = vec![0xAAu8; 4096];
    let m = e.put("b1", "exact", &mut Cursor::new(exact)).unwrap();
    assert!(m.inline.is_some());
    assert!(m.extents.is_empty());

    let over = vec![0xBBu8; 4097];
    let m = e.put("b1", "over", &mut Cursor::new(over.clone())).unwrap();
    assert!(m.inline.is_none());
    assert_eq!(m.extents.len(), 1);
    assert_eq!(e.allocator().allocated_count(), 1);

    // 读回一致
    let mut out = Vec::new();
    e.get_to("b1", "over", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, over);
    e.close().unwrap();
}

#[test]
fn inline_with_meta_headers() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let data = vec![9u8; 100];
    let m = e
        .put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(data),
            Some("text/plain"),
            vec![("x-amz-meta-foo".into(), "bar".into())],
            vec![("cache-control".into(), "max-age=60".into())],
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(m.content_type, "text/plain");
    assert_eq!(m.user_meta, vec![("x-amz-meta-foo".into(), "bar".into())]);
    assert_eq!(
        m.resp_headers,
        vec![("cache-control".into(), "max-age=60".into())]
    );
    e.close().unwrap();
}

/// M11 C1-2:声明 checksum 算法时引擎边写边算并落 ObjectMeta.checksum
/// (内联 + extent 两条路径);未声明时不算不记。
#[test]
fn put_with_checksum_records_inline_and_extent() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);

    // 内联路径(小对象)
    let small = rnd(1_000, 7);
    let m = e
        .put_with_meta(
            "b1",
            "ck-inline",
            &mut Cursor::new(small.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            Some(ChecksumAlgorithm::Crc32c),
            None,
        )
        .unwrap();
    let expect = fs3_core::checksum_one_shot(ChecksumAlgorithm::Crc32c, &small);
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: ChecksumAlgorithm::Crc32c,
            value: expect,
        })
    );
    // 读回元数据同样带值(序列化往返)
    let got = e.head("b1", "ck-inline").unwrap().unwrap();
    assert_eq!(got.checksum, m.checksum);

    // extent 路径(> small_object_limit)
    let big = rnd(200_000, 9);
    let m = e
        .put_with_meta(
            "b1",
            "ck-extent",
            &mut Cursor::new(big.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            Some(ChecksumAlgorithm::Sha256),
            None,
        )
        .unwrap();
    assert!(m.inline.is_none() && !m.extents.is_empty());
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: ChecksumAlgorithm::Sha256,
            value: fs3_core::checksum_one_shot(ChecksumAlgorithm::Sha256, &big),
        })
    );

    // 未声明算法:不算不记(现状零变化)
    let m = e
        .put("b1", "ck-none", &mut Cursor::new(rnd(100, 1)))
        .unwrap();
    assert_eq!(m.checksum, None);
    e.close().unwrap();
}

#[test]
fn put_requires_bucket() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    let r = e.put("nobucket", "k", &mut Cursor::new(vec![1u8; 10]));
    assert!(matches!(r, Err(Error::NotFound(_))));
    e.close().unwrap();
}

// ─────────────────────────── multipart(适配段模型) ───────────────────────────

/// 小分片(内联)+ 大分片(extent)混合 multipart 全流程。
#[test]
fn multipart_upload_complete_roundtrip() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);

    let uid = e
        .create_multipart(
            "b1",
            "big",
            Some("text/bla"),
            vec![("k".into(), "v".into())],
            vec![("content-encoding".into(), "gzip".into())],
            Vec::new(),
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(uid.len(), 32);

    // 分片 1:5MiB(内联阈值 32KiB 之上 → extent);分片 2:小内联
    let part1 = vec![0x11u8; 5 * 1024 * 1024];
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(part1.clone()), None, None)
        .unwrap();
    assert_eq!(p1.size, part1.len() as u64);
    assert!(p1.inline.is_none() && !p1.extents.is_empty());
    let part2 = vec![0x22u8; 1000];
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(part2.clone()), None, None)
        .unwrap();
    assert!(p2.inline.is_some());

    // ListParts 升序
    let parts = e.list_parts(&uid).unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].0, 1);
    assert_eq!(parts[1].0, 2);

    // 完成:混合路径(数据组合)
    let m = e
        .complete_multipart(
            "b1",
            "big",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    assert_eq!(m.size, (part1.len() + part2.len()) as u64);
    assert_eq!(m.parts, vec![part1.len() as u64, 1000]);
    assert_eq!(m.content_type, "text/bla");
    assert_eq!(m.user_meta, vec![("k".into(), "v".into())]);
    // 内容完整
    let mut out = Vec::new();
    e.get_to("b1", "big", 0..m.size, &mut out).unwrap();
    assert_eq!(out.len(), m.size as usize);
    assert_eq!(&out[..part1.len()], &part1[..]);
    assert_eq!(&out[part1.len()..], &part2[..]);
    // 二次 Complete 幂等(同 ETag/Size)
    let m2 = e
        .complete_multipart(
            "b1",
            "big",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    assert_eq!(m2.etag, m.etag);
    assert_eq!(m2.size, m.size);

    // 会话仍在(重传分片可 reactivate)
    let p_new = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0x33u8; 100]), None, None)
        .unwrap();
    let m3 = e
        .complete_multipart("b1", "big", &uid, &[cp(1, p_new.etag_hex())], None, None)
        .unwrap();
    assert_eq!(m3.size, 100);
    let mut out3 = Vec::new();
    e.get_to("b1", "big", 0..100, &mut out3).unwrap();
    assert_eq!(out3, vec![0x33u8; 100]);
    e.close().unwrap();
}

// ─────────────────── M11 C1-4:分片 checksum 落值与 Complete 复合验算 ───────────────────

#[test]
fn upload_part_stores_checksum_inline_and_extent() {
    // ADR-12 D-E3:声明算法时引擎边写边算并落 PartMeta.checksum(内联/
    // extent 两臂);未声明 → None。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    // 内联臂(≤ 内联阈值)
    let small = b"part-inline".to_vec();
    let p1 = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(small.clone()),
            Some(ChecksumAlgorithm::Sha256),
            None,
        )
        .unwrap();
    assert!(p1.inline.is_some());
    let expect1 = ChecksumInfo {
        algorithm: ChecksumAlgorithm::Sha256,
        value: fs3_core::checksum_one_shot(ChecksumAlgorithm::Sha256, &small),
    };
    assert_eq!(p1.checksum, Some(expect1.clone()));
    // extent 臂(> 内联阈值)
    let big = vec![0x5Au8; 100_000];
    let p2 = e
        .upload_part(
            &uid,
            2,
            &mut Cursor::new(big.clone()),
            Some(ChecksumAlgorithm::Crc32c),
            None,
        )
        .unwrap();
    assert!(p2.inline.is_none() && !p2.extents.is_empty());
    let expect2 = ChecksumInfo {
        algorithm: ChecksumAlgorithm::Crc32c,
        value: fs3_core::checksum_one_shot(ChecksumAlgorithm::Crc32c, &big),
    };
    assert_eq!(p2.checksum, Some(expect2.clone()));
    // 落盘持久化:list_parts 读回同值
    let parts = e.list_parts(&uid).unwrap();
    assert_eq!(parts[0].1.checksum, Some(expect1));
    assert_eq!(parts[1].1.checksum, Some(expect2));
    // 未声明算法 → 不算不记
    let p3 = e
        .upload_part(&uid, 3, &mut Cursor::new(b"x".to_vec()), None, None)
        .unwrap();
    assert_eq!(p3.checksum, None);
    e.close().unwrap();
}

/// 构造带逐分片 checksum 声明的 CompletePart(与落盘值一致的正例声明)。
fn cp_ck(p: &PartMeta, no: u32) -> CompletePart {
    CompletePart {
        part_number: no,
        etag_hex: p.etag_hex(),
        checksum: p.checksum.clone(),
    }
}

/// 复合值 = alg(concat(各分片 checksum 原始字节))(AWS CompositeChecksum)。
fn composite_of(alg: ChecksumAlgorithm, parts: &[&PartMeta]) -> CompositeChecksum {
    let mut concat = Vec::new();
    for p in parts {
        concat.extend_from_slice(&p.checksum.as_ref().unwrap().value);
    }
    CompositeChecksum {
        algorithm: alg,
        value: fs3_core::checksum_one_shot(alg, &concat),
        parts: Some(parts.len() as u32),
    }
}

#[test]
fn complete_multipart_checksum_validation() {
    // 正例:逐分片声明 + 复合头一致 → 复合值落 ObjectMeta.checksum,
    // 逐分片落 part_checksums(混合臂:extent + 内联);反例逐一断言
    // (验算失败在写路径之前,分片/会话原样保留,可重试)。
    // 算法取 Sha256:默认类型 COMPOSITE(AWS 口径;CRC 族默认 FULL_OBJECT,
    // 见 complete_multipart_full_object_* 测试)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let alg = ChecksumAlgorithm::Sha256;
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let d1 = vec![0x11u8; 5 * 1024 * 1024];
    let d2 = vec![0x22u8; 1000];
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(d1), Some(alg), None)
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(d2), Some(alg), None)
        .unwrap();
    let good = composite_of(alg, &[&p1, &p2]);

    // 反例 1:逐分片声明值与落盘不符 → BadDigest
    let mut bad_part = cp_ck(&p1, 1);
    bad_part.checksum = Some(ChecksumInfo {
        algorithm: alg,
        value: vec![0u8; 32],
    });
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[bad_part, cp_ck(&p2, 2)],
        Some(&good),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 反例 2:逐分片声明算法与落盘不符 → BadDigest
    let mut bad_alg = cp_ck(&p1, 1);
    bad_alg.checksum = Some(ChecksumInfo {
        algorithm: ChecksumAlgorithm::Sha1,
        value: vec![0u8; 20],
    });
    let r = e.complete_multipart("b1", "k", &uid, &[bad_alg, cp_ck(&p2, 2)], None, None);
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 反例 3:复合值不符 → BadDigest
    let bad_composite = CompositeChecksum {
        value: vec![0u8; 32],
        ..good.clone()
    };
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp_ck(&p1, 1), cp_ck(&p2, 2)],
        Some(&bad_composite),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 反例 4:复合 -N 分片数不符 → BadDigest
    let bad_count = CompositeChecksum {
        parts: Some(3),
        ..good.clone()
    };
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp_ck(&p1, 1), cp_ck(&p2, 2)],
        Some(&bad_count),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 反例 5:COMPOSITE 算法给裸值(缺 -N)→ BadDigest(形态不符)
    let bare = CompositeChecksum {
        parts: None,
        ..good.clone()
    };
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp_ck(&p1, 1), cp_ck(&p2, 2)],
        Some(&bare),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");

    // 正例(混合臂)
    let m = e
        .complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp_ck(&p1, 1), cp_ck(&p2, 2)],
            Some(&good),
            None,
        )
        .unwrap();
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: alg,
            value: good.value.clone(),
        })
    );
    assert_eq!(
        m.part_checksums,
        vec![p1.checksum.clone(), p2.checksum.clone()]
    );
    // 对象级读回(GetObjectAttributes ObjectParts 的查询路径)
    let h = e.head_version("b1", "k", None).unwrap();
    assert_eq!(h.part_checksums.len(), 2);
    assert_eq!(h.checksum, m.checksum);
    e.close().unwrap();
}

#[test]
fn complete_multipart_composite_requires_part_checksums() {
    // 复合头在场但分片无落盘 checksum(或算法不一致)→ InvalidRequest
    // (AWS 口径:无法复合);逐分片声明落在无 checksum 分片上 → BadDigest。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let alg = ChecksumAlgorithm::Sha256;
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    // part 1 带 checksum(≥5MiB,非最后分片下限);part 2 不带;part 3 带异种算法
    let p1 = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]),
            Some(alg),
            None,
        )
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(vec![2u8; 100]), None, None)
        .unwrap();
    let p3 = e
        .upload_part(
            &uid,
            3,
            &mut Cursor::new(vec![3u8; 100]),
            Some(ChecksumAlgorithm::Crc32c),
            None,
        )
        .unwrap();
    // 缺 checksum 的分片参与复合 → InvalidRequest
    let comp = composite_of(alg, &[&p1]); // 值无关,验算在缺片处先失败
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp_ck(&p1, 1), cp(2, p2.etag_hex())],
        Some(&CompositeChecksum {
            parts: Some(2),
            ..comp.clone()
        }),
        None,
    );
    assert!(matches!(r, Err(Error::InvalidRequest(_))), "{r:?}");
    // 异种算法分片参与复合 → InvalidRequest
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp_ck(&p1, 1), cp_ck(&p3, 3)],
        Some(&CompositeChecksum {
            parts: Some(2),
            ..comp.clone()
        }),
        None,
    );
    assert!(matches!(r, Err(Error::InvalidRequest(_))), "{r:?}");
    // 逐分片声明落在无 checksum 分片上 → BadDigest(声明无法匹配缺失值)
    let mut declared = cp(2, p2.etag_hex());
    declared.checksum = Some(ChecksumInfo {
        algorithm: alg,
        value: vec![0u8; 32],
    });
    let r = e.complete_multipart("b1", "k", &uid, &[cp_ck(&p1, 1), declared], None, None);
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    e.close().unwrap();
}

#[test]
fn complete_multipart_session_algorithm_auto_compute() {
    // M11 门禁补强(AWS 口径):Create 声明 x-amz-checksum-algorithm 的会话,
    // Complete 无任何客户端 checksum 头也由服务端代算对象级 checksum 落值;
    // 客户端头仅作验算。类型 = 算法默认(Sha256 → COMPOSITE 复合 -N 形态)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let alg = ChecksumAlgorithm::Sha256;
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            Some(alg),
            None,
            None,
        )
        .unwrap();
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![7u8; 1000]), Some(alg), None)
        .unwrap();
    // 无复合头、无逐分片声明:服务端代算复合值并落对象
    let m = e
        .complete_multipart("b1", "k", &uid, &[cp(1, p1.etag_hex())], None, None)
        .unwrap();
    let expect = composite_of(alg, &[&p1]);
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: alg,
            value: expect.value.clone(),
        })
    );
    // 会话算法与客户端复合头算法相左 → InvalidRequest
    let uid2 = e
        .create_multipart(
            "b1",
            "k2",
            None,
            vec![],
            Vec::new(),
            vec![],
            Some(alg),
            None,
            None,
        )
        .unwrap();
    let q1 = e
        .upload_part(&uid2, 1, &mut Cursor::new(vec![7u8; 1000]), Some(alg), None)
        .unwrap();
    let clash = CompositeChecksum {
        algorithm: ChecksumAlgorithm::Crc32,
        value: vec![0u8; 4],
        parts: Some(1),
    };
    let r = e.complete_multipart("b1", "k2", &uid2, &[cp_ck(&q1, 1)], Some(&clash), None);
    assert!(matches!(r, Err(Error::InvalidRequest(_))), "{r:?}");
    e.close().unwrap();
}

#[test]
fn complete_multipart_full_object_crc_family() {
    // FULL_OBJECT(CRC 族默认类型):对象级 checksum = alg(拼接字节流),
    // 与分片是否带 checksum 无关;客户端裸 base64 头验算,-N 形态拒绝。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let alg = ChecksumAlgorithm::Crc32c;
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            Some(alg),
            None,
            None,
        )
        .unwrap();
    let d1 = vec![0x31u8; 5 * 1024 * 1024];
    let d2 = vec![0x32u8; 1000];
    // part1 带 checksum(extent 臂);part2 不带(内联臂)——FULL_OBJECT
    // 走数据流,不依赖逐分片落值
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(d1.clone()), Some(alg), None)
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(d2.clone()), None, None)
        .unwrap();
    let mut data = d1.clone();
    data.extend_from_slice(&d2);
    let full = fs3_core::checksum_one_shot(alg, &data);
    // -N 形态(FULL_OBJECT 会话)→ BadDigest
    let dash = CompositeChecksum {
        algorithm: alg,
        value: full.clone(),
        parts: Some(2),
    };
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
        Some(&dash),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 裸值不符 → BadDigest
    let wrong = CompositeChecksum {
        algorithm: alg,
        value: vec![0u8; 4],
        parts: None,
    };
    let r = e.complete_multipart(
        "b1",
        "k",
        &uid,
        &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
        Some(&wrong),
        None,
    );
    assert!(matches!(r, Err(Error::BadDigest(_))), "{r:?}");
    // 裸值正确 → 200(混合臂:extent + 内联)
    let m = e
        .complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            Some(&CompositeChecksum {
                algorithm: alg,
                value: full.clone(),
                parts: None,
            }),
            None,
        )
        .unwrap();
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: alg,
            value: full,
        })
    );
    e.close().unwrap();
}

#[test]
fn complete_multipart_checksum_three_arms() {
    // 三臂落值:全内联 / 全 extent 均落复合 checksum 与逐分片明细
    // (混合臂见 complete_multipart_checksum_validation)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let alg = ChecksumAlgorithm::Sha256;
    // 全内联臂(AWS 非最后分片 ≥5MiB 约束下,全内联只可能单片:
    // 单小分片带 checksum,复合值 = alg(该分片 checksum),-1 形态)
    let uid = e
        .create_multipart(
            "b1",
            "inl",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![1u8; 100]), Some(alg), None)
        .unwrap();
    let comp = composite_of(alg, &[&p1]);
    let m = e
        .complete_multipart("b1", "inl", &uid, &[cp_ck(&p1, 1)], Some(&comp), None)
        .unwrap();
    assert!(m.inline.is_some(), "全内联臂");
    assert_eq!(m.checksum.as_ref().map(|c| &c.value), Some(&comp.value));
    assert_eq!(m.part_checksums.len(), 1);
    // 全 extent 臂(大分片,零数据搬运)
    let uid = e
        .create_multipart(
            "b1",
            "ext",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let p1 = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(vec![3u8; 5 * 1024 * 1024]),
            Some(alg),
            None,
        )
        .unwrap();
    let p2 = e
        .upload_part(
            &uid,
            2,
            &mut Cursor::new(vec![4u8; 5 * 1024 * 1024]),
            Some(alg),
            None,
        )
        .unwrap();
    let comp = composite_of(alg, &[&p1, &p2]);
    let m = e
        .complete_multipart(
            "b1",
            "ext",
            &uid,
            &[cp_ck(&p1, 1), cp_ck(&p2, 2)],
            Some(&comp),
            None,
        )
        .unwrap();
    assert!(m.inline.is_none() && !m.extents.is_empty(), "全 extent 臂");
    assert_eq!(m.checksum.as_ref().map(|c| &c.value), Some(&comp.value));
    assert_eq!(m.part_checksums.len(), 2);
    // 无 checksum 分片完成(复合头缺席)→ checksum None、part_checksums 空
    let uid = e
        .create_multipart(
            "b1",
            "plain",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let p = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![9u8; 10]), None, None)
        .unwrap();
    let m = e
        .complete_multipart("b1", "plain", &uid, &[cp(1, p.etag_hex())], None, None)
        .unwrap();
    assert_eq!(m.checksum, None);
    assert!(m.part_checksums.is_empty());
    e.close().unwrap();
}

/// 零数据搬运:全部大分片段直接拼接(对象段引用 == 分片之和)。
#[test]
fn multipart_extent_concat_no_copy() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "big",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let mut total_refs = 0usize;
    let mut parts_meta = Vec::new();
    for i in 0..3 {
        let data = vec![i as u8; 5 * 1024 * 1024];
        let p = e
            .upload_part(&uid, i + 1, &mut Cursor::new(data), None, None)
            .unwrap();
        total_refs += p.extents.len();
        parts_meta.push(cp(i + 1, p.etag_hex()));
    }
    let m = e
        .complete_multipart("b1", "big", &uid, &parts_meta, None, None)
        .unwrap();
    assert_eq!(m.extents.len(), total_refs);
    assert_eq!(m.size, 15 * 1024 * 1024);
    // 内容校验(抽样)
    let mut out = Vec::new();
    e.get_to("b1", "big", 0..m.size, &mut out).unwrap();
    for i in 0..3 {
        assert!(out[i * 5 * 1024 * 1024..(i + 1) * 5 * 1024 * 1024]
            .iter()
            .all(|&b| b == i as u8));
    }
    e.close().unwrap();
}

/// 分片与普通对象共享开放 extent(打包);分片独占整块 + 普通对象尾段。
#[test]
fn multipart_parts_pack_with_objects() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "big",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    // 5MiB 分片:独占整块 + 尾段(1028KiB,开放)
    let p1 = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]),
            None,
            None,
        )
        .unwrap();
    // 普通对象进入同一开放 extent(打包)
    e.put("b1", "plain", &mut Cursor::new(vec![2u8; 100_000]))
        .unwrap();
    // 末分片(无大小约束)继续打包
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(vec![3u8; 100_000]), None, None)
        .unwrap();
    let p1_seg = &p1.extents[0];
    let plain_seg = &e.head("b1", "plain").unwrap().unwrap().extents[0];
    let p2_seg = &p2.extents[0];
    assert_eq!(p1_seg.extent_id, 0, "分片 1 独占 extent 0");
    assert_eq!(
        plain_seg.extent_id, p1.extents[1].extent_id,
        "普通对象与分片尾段同 extent"
    );
    assert_eq!(p2_seg.extent_id, plain_seg.extent_id, "末分片继续打包");

    let m = e
        .complete_multipart(
            "b1",
            "big",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    assert_eq!(m.size, (5 * 1024 * 1024 + 100_000) as u64);
    let mut out = Vec::new();
    e.get_to("b1", "big", 0..m.size, &mut out).unwrap();
    assert_eq!(out[..5 * 1024 * 1024], vec![1u8; 5 * 1024 * 1024]);
    assert_eq!(out[5 * 1024 * 1024..], vec![3u8; 100_000]);
    let mut out = Vec::new();
    e.get_to("b1", "plain", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, vec![2u8; 100_000]);
    assert!(e.allocator().leaks().is_empty());
    e.close().unwrap();
}

#[test]
fn multipart_validation_errors() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();

    // 未知会话
    assert!(matches!(
        e.upload_part("nope", 1, &mut Cursor::new(vec![1u8; 10]), None, None),
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        e.complete_multipart("b1", "k", "nope", &[cp(1, "x".into())], None, None),
        Err(Error::NoSuchUpload(_))
    ));
    // 分片 ETag 不匹配 → InvalidPart
    let p = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0u8; 1]), None, None)
        .unwrap();
    assert!(matches!(
        e.complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp(1, "ffffffffffffffffffffffffffffffff".into())],
            None,
            None
        ),
        Err(Error::InvalidPart(_))
    ));
    // 列出不存在的分片号 → InvalidPart(s3-tests missing_part)
    let p2 = e
        .upload_part(&uid, 3, &mut Cursor::new(vec![0u8; 1]), None, None)
        .unwrap();
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[cp(9999, p.etag_hex())], None, None),
        Err(Error::InvalidPart(_))
    ));
    // 非最后分片 < 5MiB → PartTooSmall(part 1 非最后且 < 5MiB)
    assert!(matches!(
        e.complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp(1, p.etag_hex()), cp(3, p2.etag_hex())],
            None,
            None
        ),
        Err(Error::PartTooSmall(_))
    ));
    // 乱序 part_no:REVIEW §3.10 后客户端列表必须严格递增 → InvalidPartOrder
    // (此前 BTreeMap 自动排序被静默接受,乱序 + 小分片只能报 PartTooSmall)
    assert!(matches!(
        e.complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp(3, p2.etag_hex()), cp(1, p.etag_hex())],
            None,
            None
        ),
        Err(Error::InvalidPartOrder(_))
    ));
    // 重复 part_no 同样非严格递增 → InvalidPartOrder
    assert!(matches!(
        e.complete_multipart(
            "b1",
            "k",
            &uid,
            &[cp(1, p.etag_hex()), cp(1, p2.etag_hex())],
            None,
            None
        ),
        Err(Error::InvalidPartOrder(_))
    ));
    // resend_first_finishes_last 语义:重新上传同一分片号(覆盖)后,
    // complete 列表只含该分片号一次、用新 ETag → 新数据生效
    let big = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(vec![0x55u8; 5 * 1024 * 1024]),
            None,
            None,
        )
        .unwrap();
    let m = e
        .complete_multipart("b1", "k", &uid, &[cp(1, big.etag_hex())], None, None)
        .unwrap();
    assert_eq!(m.size, 5 * 1024 * 1024);
    let mut out = Vec::new();
    e.get_to("b1", "k", 0..m.size, &mut out).unwrap();
    assert!(out.iter().all(|&b| b == 0x55));
    // 空列表 → InvalidArgument(服务层映射 MalformedXML)
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[], None, None),
        Err(Error::InvalidArgument(_))
    ));
    e.close().unwrap();
}

#[test]
fn multipart_abort_frees_extents() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    e.upload_part(
        &uid,
        1,
        &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]),
        None,
        None,
    )
    .unwrap();
    assert!(e.alloc.allocated_count() >= 1);
    e.abort_multipart(&uid).unwrap();
    assert_eq!(e.alloc.allocated_count(), 0);
    assert!(matches!(
        e.abort_multipart(&uid),
        Err(Error::NoSuchUpload(_))
    ));
    // 对象不可见
    assert!(e.head("b1", "k").unwrap().is_none());
    e.close().unwrap();
}

#[test]
fn copy_object_cow_share_and_release() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let data = vec![7u8; 5 * 1024 * 1024];
    e.put("b1", "src", &mut Cursor::new(data.clone())).unwrap();
    let src = e.head("b1", "src").unwrap().unwrap();
    let ext_id = src.extents[0].extent_id as u64;

    // COW 复制:共享段,无新分配
    let before = e.alloc.allocated_count();
    let c = e
        .copy_object("b1", "src", "b1", "dst", None, None, None)
        .unwrap();
    assert_eq!(e.alloc.allocated_count(), before);
    assert_eq!(c.extents, src.extents);
    // 内容一致
    let mut out = Vec::new();
    e.get_to("b1", "dst", 0..c.size, &mut out).unwrap();
    assert_eq!(out, data);
    // 引用计数 = 2(两个 extent 都被共享)
    assert_eq!(e.alloc.refcount(ext_id), 2);
    // 删除一个引用:extent 仍在(计数 1)
    e.delete("b1", "dst").unwrap();
    assert_eq!(e.alloc.refcount(ext_id), 1);
    // 再删:extent 归还位图
    e.delete("b1", "src").unwrap();
    assert_eq!(e.alloc.refcount(ext_id), 0);
    assert!(!e.alloc.test_bit(ext_id));
    // REPLACE 指令
    e.put("b1", "src", &mut Cursor::new(vec![1u8; 10])).unwrap();
    let c2 = e
        .copy_object(
            "b1",
            "src",
            "b1",
            "dst",
            Some("text/x"),
            Some(&[("m".into(), "n".into())]),
            Some(&[("content-encoding".into(), "gzip".into())]),
        )
        .unwrap();
    assert_eq!(c2.content_type, "text/x");
    assert_eq!(c2.user_meta, vec![("m".into(), "n".into())]);
    assert_eq!(
        c2.resp_headers,
        vec![("content-encoding".into(), "gzip".into())]
    );
    // 源不存在 → NotFound
    assert!(matches!(
        e.copy_object("b1", "nope", "b1", "x", None, None, None),
        Err(Error::NotFound(_))
    ));
    e.close().unwrap();
}

/// 会话过期回收(TTL=0 → 立即过期)。
#[test]
fn multipart_sweep_expired() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "k",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    e.upload_part(
        &uid,
        1,
        &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]),
        None,
        None,
    )
    .unwrap();
    let n = e.sweep_expired_sessions(0).unwrap();
    assert_eq!(n, 1);
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[], None, None),
        Err(Error::NoSuchUpload(_))
    ));
    e.close().unwrap();
}

// ─────────────────────────── ADR-9 打包语义 ───────────────────────────

/// 利用率门禁(ADR-9 §10.4):1MiB 对象负载,设备占用 / 逻辑字节 ≥ 99%;
/// 多个对象打包进同一 extent;extent 数远小于对象数。
#[test]
fn packing_utilization_1mib_objects() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let n = 20;
    let mut logical = 0u64;
    for i in 0..n {
        let data = vec![(i % 251) as u8; 1024 * 1024];
        e.put("b1", &format!("k{i}"), &mut Cursor::new(data))
            .unwrap();
        logical += 1024 * 1024;
    }
    // 设备占用 = Σ 活段 == 逻辑字节(1MiB 对象全落盘,段恰好覆盖对象)
    let live = e.allocator().live_bytes_total();
    assert_eq!(live, logical);
    assert!(
        live as f64 / logical as f64 >= 0.99,
        "利用率 ≥ 99%,实际 {}",
        live as f64 / logical as f64
    );
    // 打包验证:相邻对象共享同一 extent
    let m0 = e.head("b1", "k0").unwrap().unwrap();
    let m1 = e.head("b1", "k1").unwrap().unwrap();
    assert_eq!(
        m0.extents[0].extent_id, m1.extents[0].extent_id,
        "对象共享 extent(打包)"
    );
    // 空间收敛:20 × 1MiB 应打包在 ≤ 7 个 extent(现状独占需 20 个)
    let alloc = e.allocator().allocated_count();
    assert!(alloc <= 7, "20 × 1MiB 打包在少量 extent,实际 {alloc}");
    // 全部对象读回
    for i in 0..n {
        let mut out = Vec::new();
        e.get_to("b1", &format!("k{i}"), 0..u64::MAX, &mut out)
            .unwrap();
        assert!(out.iter().all(|&b| b == (i % 251) as u8));
    }
    let r = e.check_report().unwrap();
    assert!(r.leaks.is_empty());
    assert_eq!(r.live_bytes, logical);
    e.close().unwrap();
}

/// 封口判定与类型(ADR-9 §5.1/§5.2):写满 → 独占;剩余 < 32KiB → 封口;
/// seal-on-delete。
#[test]
fn seal_conditions_and_types() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let cap = e.superblock().extent_capacity();

    // (a) 写满 → 独占 extent(单对象整块,头带完整 CRC 表)
    let big = rnd(cap as usize, 7);
    e.put("b1", "big", &mut Cursor::new(big)).unwrap();
    let m = e.head("b1", "big").unwrap().unwrap();
    assert_eq!(m.extents.len(), 1);
    let s0 = &m.extents[0];
    assert_eq!(s0.offset, 0);
    assert!(s0.crcs.is_empty(), "独占段元数据 crcs 为空");
    let h = e.read_extent_header(s0.extent_id as u64).unwrap().unwrap();
    assert!(!h.is_packed(), "独占头非 packed");
    assert!(!h.chunk_crcs.is_empty(), "独占头带完整 CRC 表");
    assert_eq!(h.generation, e.allocator().generation(s0.extent_id as u64));

    // 下一个对象 → 新开放 extent
    e.put("b1", "a", &mut Cursor::new(vec![1u8; 100_000]))
        .unwrap();
    let ma = e.head("b1", "a").unwrap().unwrap();
    assert_ne!(ma.extents[0].extent_id, s0.extent_id);

    // (b) 剩余 < 32KiB → 封口:填充到剩 16KiB,再写对象 → 换新 extent
    let e1 = ma.extents[0].extent_id;
    let fill = cap - 100_000 - 16 * 1024; // 剩余恰好 16KiB < 32KiB
    e.put("b1", "fill", &mut Cursor::new(rnd(fill as usize, 3)))
        .unwrap();
    e.put("b1", "after", &mut Cursor::new(vec![4u8; 100_000]))
        .unwrap();
    let mfill = e.head("b1", "after").unwrap().unwrap();
    assert_ne!(mfill.extents[0].extent_id, e1, "剩余 < 32KiB → 新 extent");
    // E1 已被打包封口
    let h1 = e.read_extent_header(e1 as u64).unwrap().unwrap();
    assert!(h1.is_packed(), "非独占封口 → 打包头");

    // (c) seal-on-delete:开放 extent 内删除对象 → 封口
    let e2 = mfill.extents[0].extent_id;
    e.put("b1", "keep", &mut Cursor::new(vec![5u8; 100_000]))
        .unwrap();
    e.delete("b1", "after").unwrap();
    let h2 = e.read_extent_header(e2 as u64).unwrap().unwrap();
    assert!(h2.is_packed(), "seal-on-delete → 打包头");
    // 新写入 → 新 extent
    e.put("b1", "next", &mut Cursor::new(vec![6u8; 100_000]))
        .unwrap();
    let mn = e.head("b1", "next").unwrap().unwrap();
    assert_ne!(mn.extents[0].extent_id, e2);
    // 全部读回
    for k in ["big", "a", "fill", "keep", "next"] {
        let mut out = Vec::new();
        e.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
        assert!(!out.is_empty());
    }
    assert!(e.allocator().leaks().is_empty());
    e.close().unwrap();
}

/// a: 记录触发时机(ADR-9 §4.5):首段分配发 alloc;末段消亡发 ref_dec。
#[test]
fn alloc_records_first_and_last_segment() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let allocs = |e: &Engine| -> Vec<(u64, u64)> {
        e.meta()
            .list_alloc_records(0)
            .unwrap()
            .into_iter()
            .flat_map(|r| r.alloc)
            .collect()
    };
    let decs = |e: &Engine| -> Vec<u64> {
        e.meta()
            .list_alloc_records(0)
            .unwrap()
            .into_iter()
            .flat_map(|r| r.ref_dec)
            .collect()
    };
    // 首个对象 → alloc 记录
    e.put("b1", "k0", &mut Cursor::new(vec![1u8; 100_000]))
        .unwrap();
    let a0 = allocs(&e);
    assert_eq!(a0.len(), 1, "首段分配发 alloc");
    let e0 = a0[0].0;
    // 同 extent 再写对象 → 无新 alloc 记录
    e.put("b1", "k1", &mut Cursor::new(vec![2u8; 100_000]))
        .unwrap();
    assert_eq!(allocs(&e).len(), 1, "同 extent 后续段不发 alloc");
    // 删除 k0(extent 仍有 k1 的活段)→ 无 ref_dec
    e.delete("b1", "k0").unwrap();
    assert!(decs(&e).is_empty(), "未归零不发 ref_dec");
    // 删除 k1(末段消亡)→ ref_dec + 位图清位
    e.delete("b1", "k1").unwrap();
    let d = decs(&e);
    assert_eq!(d, vec![e0]);
    assert!(!e.allocator().test_bit(e0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

/// COW 段级共享(ADR-9 §5.5):打包 extent 内只共享部分段;
/// 删除一个持有者不释放;全部释放才回收。
#[test]
fn cow_packed_segment_sharing() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let d1 = vec![1u8; 100_000];
    let d2 = vec![2u8; 100_000];
    e.put("b1", "a", &mut Cursor::new(d1.clone())).unwrap();
    e.put("b1", "b", &mut Cursor::new(d2.clone())).unwrap();
    let a = e.head("b1", "a").unwrap().unwrap();
    let e0 = a.extents[0].extent_id as u64;
    assert_eq!(
        e.head("b1", "b").unwrap().unwrap().extents[0].extent_id as u64,
        e0,
        "同 extent 打包"
    );
    // COW 复制 a → a2:共享 a 的段(非整 extent)
    e.copy_object("b1", "a", "b1", "a2", None, None, None)
        .unwrap();
    assert_eq!(e.allocator().refcount(e0), 3, "a+b+a2 引用同一 extent");
    let lb = e.allocator().live_bytes_of(e0);
    // 删除 a:a 的段仍被 a2 共享,extent 保持、live_bytes 不减
    e.delete("b1", "a").unwrap();
    assert!(e.allocator().test_bit(e0));
    assert_eq!(e.allocator().live_bytes_of(e0), lb, "共享段不重复计/不减");
    // a2 读回正常
    let mut out = Vec::new();
    e.get_to("b1", "a2", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d1);
    // 删除 b + a2 → live_bytes 归零 → extent 释放
    e.delete("b1", "b").unwrap();
    e.delete("b1", "a2").unwrap();
    assert!(!e.allocator().test_bit(e0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

/// 崩溃恢复:开放 extent 按活段最大 end 续写;跨崩溃会话孤儿区被自然覆盖。
#[test]
fn recovery_resumes_open_extent_and_overwrites_orphans() {
    let (_d, cfg) = setup();
    let d1 = vec![1u8; 300 * 1024]; // 300KiB = 4KiB 对齐
    let d2 = vec![2u8; 500 * 1024];
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "a", &mut Cursor::new(d1.clone())).unwrap();
        e.meta().flush().unwrap();
        // 模拟"崩溃前数据已落盘但事务未提交":直接往开放 extent 尾部写孤儿数据
        let dev = fs3_device::open_device(&cfg.devices[0], false).unwrap();
        let mut buf = fs3_device::AlignedBuffer::new(4096).unwrap();
        buf.as_mut_slice().fill(0xEE);
        // extent 0 数据区 + 300KiB(已提交水位)处
        let off = 1024 * 1024 + 2 * 4096 + 4096 + 300 * 1024;
        dev.pwrite_aligned(buf.as_slice(), off).unwrap();
        e.abort(); // 模拟 kill -9
    }
    // 重启:watermark = 活段最大 end(300K),孤儿区 [300K, 304K) 无活段
    let mut e = Engine::open(&cfg).unwrap();
    assert_eq!(e.allocator().allocated_count(), 1);
    assert!(e.allocator().leaks().is_empty());
    // 继续写入:孤儿数据被自然覆盖,不残留
    e.put("b1", "b", &mut Cursor::new(d2.clone())).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "a", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d1);
    let mut out = Vec::new();
    e.get_to("b1", "b", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d2);
    assert!(e.allocator().leaks().is_empty());
    e.close().unwrap();
}

/// 崩溃恢复:写满未封口(头缺失)的 extent 补写独占头(重算 CRC),verify 通过。
#[test]
fn recovery_seals_headerless_full_extent() {
    let (_d, cfg) = setup();
    let data = rnd((fs3_core::DEFAULT_EXTENT_SIZE - 4096) as usize, 9);
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "big", &mut Cursor::new(data.clone())).unwrap();
        e.meta().flush().unwrap();
        // 模拟"封口写头前崩溃":清掉 extent 0 的头
        let dev = fs3_device::open_device(&cfg.devices[0], false).unwrap();
        let mut zero = fs3_device::AlignedBuffer::new(4096).unwrap();
        zero.as_mut_slice().fill(0);
        let hdr_off = 1024 * 1024 + 2 * 4096; // extent 0 起点
        dev.pwrite_aligned(zero.as_slice(), hdr_off).unwrap();
        e.abort();
    }
    // 重启:识别为"写满未封口" → 补独占头(重算 CRC 表)
    let mut e = Engine::open(&cfg).unwrap();
    assert!(e.allocator().leaks().is_empty());
    let h = e.read_extent_header(0).unwrap().unwrap();
    assert!(!h.is_packed());
    let units = (fs3_core::DEFAULT_EXTENT_SIZE - 4096).div_ceil(65536);
    assert_eq!(h.chunk_crcs.len(), units as usize, "重算 CRC 表");
    e.close().unwrap();
    drop(e); // 释放 rocksdb 锁
             // verify_reads 走补写的头 CRC → 通过
    let mut cfg2 = cfg.clone();
    cfg2.verify_reads = true;
    {
        let mut e2 = Engine::open(&cfg2).unwrap();
        let mut out = Vec::new();
        e2.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        e2.close().unwrap();
    }
}

/// verify_reads 双来源(ADR-9 §4.3):打包段(元数据 CRC)损坏可检测。
#[test]
fn verify_reads_packed_segment_detects_corruption() {
    let (_d, cfg) = setup();
    let data = vec![5u8; 100_000];
    let img = cfg.devices[0].clone();
    {
        let mut e = open_engine(&cfg);
        e.put("b1", "k", &mut Cursor::new(data)).unwrap();
        e.close().unwrap();
    }
    // 篡改打包段数据区(extent 0 数据区 + 4096 处,第一 64KiB 网格内)
    let dev = fs3_device::open_device(&img, false).unwrap();
    let mut buf = fs3_device::AlignedBuffer::new(4096).unwrap();
    let off = 1024 * 1024 + 2 * 4096 + 4096 + 4096;
    dev.pread_aligned(buf.as_mut_slice(), off).unwrap();
    buf.as_mut_slice()[0] ^= 0xFF;
    dev.pwrite_aligned(buf.as_slice(), off).unwrap();
    // 普通读:不校验
    {
        let mut e = Engine::open(&cfg).unwrap();
        let mut out = Vec::new();
        e.get_to("b1", "k", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out.len(), 100_000);
        e.close().unwrap();
    }
    // verify_reads:段内网格 CRC 检测
    let mut cfg2 = cfg.clone();
    cfg2.verify_reads = true;
    let mut e2 = Engine::open(&cfg2).unwrap();
    let mut out = Vec::new();
    let r = e2.get_to("b1", "k", 0..u64::MAX, &mut out);
    assert!(r.is_err(), "packed segment crc must detect corruption");
    e2.close().unwrap();
}

/// 混合段(独占 + 打包 + spill)verify_reads 全量回读。
#[test]
fn verify_reads_mixed_segments_roundtrip() {
    let (_d, mut cfg) = setup();
    cfg.verify_reads = true;
    let mut e = open_engine(&cfg);
    for (i, size) in [
        4096usize,
        100_000,
        5 * 1024 * 1024,
        4 * 1024 * 1024 + 123_456,
    ]
    .into_iter()
    .enumerate()
    {
        let d = rnd(size, i as u8 + 1);
        e.put("b1", &format!("k{i}"), &mut Cursor::new(d.clone()))
            .unwrap();
        let mut out = Vec::new();
        e.get_to("b1", &format!("k{i}"), 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, d);
    }
    e.close().unwrap();
}

/// 分页列举回归(段模型不影响列表语义)。
#[test]
fn list_page_after_marker_is_exclusive() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    for k in ["bar", "baz", "foo", "quxx"] {
        e.put("b1", k, &mut Cursor::new(vec![1u8; 10])).unwrap();
    }
    let p = e.list_objects_page("b1", "", None, None, 2).unwrap();
    let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["bar", "baz"]);
    let p = e
        .list_objects_page("b1", "", None, Some("baz"), 100)
        .unwrap();
    let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["foo", "quxx"]);
    e.close().unwrap();
}

// ─────────────────────────── 属性测试 ───────────────────────────

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]

    /// 随机尺寸(32KiB~8MiB,跨 extent/跨 64KiB 网格)读写逐字节一致;
    /// verify_reads 开关随机。
    #[test]
    fn spill_roundtrip_random_sizes(
        sizes in proptest::collection::vec(33 * 1024usize..8 * 1024 * 1024, 1..6),
        verify in proptest::bool::ANY,
    ) {
        let (_d, cfg) = setup();
        let mut cfg2 = cfg.clone();
        cfg2.verify_reads = verify;
        let mut e = open_engine(&cfg2);
        let mut expected: Vec<(String, Vec<u8>)> = Vec::new();
        for (i, size) in sizes.into_iter().enumerate() {
            let data = rnd(size, i as u8 + 11);
            let key = format!("k{i}");
            e.put("b1", &key, &mut Cursor::new(data.clone())).unwrap();
            expected.push((key, data));
        }
        for (key, data) in &expected {
            let mut out = Vec::new();
            e.get_to("b1", key, 0..u64::MAX, &mut out).unwrap();
            prop_assert_eq!(&out, data, "verify_reads={} object {}", verify, key);
        }
        e.close().unwrap();
    }

    /// 随机操作序列(PUT/覆盖/删除/COW 复制/内联)不变量:读回一致、
    /// 零泄漏、重启后状态完整。
    #[test]
    fn random_ops_roundtrip_and_reopen(
        ops in proptest::collection::vec(0u8..5, 1..24),
        sizes in proptest::collection::vec(33 * 1024usize..3 * 1024 * 1024, 1..24),
    ) {
        let (_d, cfg) = setup();
        let mut e = open_engine(&cfg);
        let mut state: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (i, &op) in ops.iter().enumerate() {
            let sz = sizes[i % sizes.len()];
            let key = format!("k{}", i % 5); // 5 个键:覆盖/删除/复制
            match op {
                0 => {
                    let data = rnd(sz, i as u8);
                    e.put("b1", &key, &mut Cursor::new(data.clone())).unwrap();
                    state.insert(key, data);
                }
                1 => {
                    e.delete("b1", &key).unwrap();
                    state.remove(&key);
                }
                2 => {
                    if let Some(src) = state.get(&key).cloned() {
                        let dst = format!("copy-{i}");
                        e.copy_object("b1", &key, "b1", &dst, None, None, None).unwrap();
                        state.insert(dst, src);
                    }
                }
                3 => {
                    // 覆盖为内联对象
                    let data = vec![7u8; 1000];
                    e.put("b1", &key, &mut Cursor::new(data.clone())).unwrap();
                    state.insert(key, data);
                }
                _ => {}
            }
            // 不变量:全部现存对象读回一致
            for (k, d) in &state {
                let mut out = Vec::new();
                e.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
                prop_assert_eq!(&out, d);
            }
        }
        prop_assert!(e.allocator().leaks().is_empty());
        e.close().unwrap();
        drop(e); // 释放 rocksdb 锁后重启
        // 重启:状态完整、零泄漏
        let mut e2 = Engine::open(&cfg).unwrap();
        for (k, d) in &state {
            let mut out = Vec::new();
            e2.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
            prop_assert_eq!(&out, d);
        }
        prop_assert!(e2.allocator().leaks().is_empty());
        e2.close().unwrap();
    }
}

// ─────────────────────────── 读写调用栈基准 ───────────────────────────

/// 引擎级读写路径基准(调用栈开销测量;非门禁):
/// 10MiB 对象 get_to/read_at 全量读回,报告 MB/s。
/// 运行: cargo test -p fs3-engine bench_read_path -- --ignored --nocapture
#[test]
#[ignore]
fn bench_read_path() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(512 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = EngineConfig {
        devices: vec![img],
        meta_dir: dir.path().join("meta"),
        compaction: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut e = open_engine(&cfg);
    let size = 10 * 1024 * 1024;
    let data = rnd(size, 3);
    e.put("b1", "big", &mut Cursor::new(data.clone())).unwrap();
    e.close().unwrap();
    drop(e);

    let mut e = open_engine(&cfg);
    // 预热
    for _ in 0..3 {
        let mut out = Vec::with_capacity(size);
        e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out.len(), size);
    }
    let rounds = 20;
    let t0 = std::time::Instant::now();
    for _ in 0..rounds {
        let mut out = Vec::with_capacity(size);
        e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out.len(), size);
    }
    let dt = t0.elapsed().as_secs_f64();
    let mb = (size * rounds) as f64 / (1024.0 * 1024.0);
    eprintln!("get_to  10MiB x{rounds}: {:.1} MiB/s ({:.2}s)", mb / dt, dt);

    // read_at 路径(HTTP 流式语义;4MiB 缓冲,与 handler.rs 一致)
    for (label, bufsz) in [
        ("read_at(64KiB)", 64 * 1024),
        ("read_at(4MiB)", 4 * 1024 * 1024),
    ] {
        let t0 = std::time::Instant::now();
        let mut total = 0usize;
        let mut buf = vec![0u8; bufsz];
        for _ in 0..rounds {
            let mut off = 0usize;
            while off < size {
                let n = e.read_at("b1", "big", off as u64, &mut buf).unwrap();
                assert!(n > 0);
                off += n;
            }
            total += off;
        }
        let dt = t0.elapsed().as_secs_f64();
        let mb = total as f64 / (1024.0 * 1024.0);
        eprintln!("{label} 10MiB x{rounds}: {:.1} MiB/s ({:.2}s)", mb / dt, dt);
    }

    // PUT 路径
    let t0 = std::time::Instant::now();
    for i in 0..rounds {
        e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
            .unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    let mb = (size * rounds) as f64 / (1024.0 * 1024.0);
    eprintln!("put     10MiB x{rounds}: {:.1} MiB/s ({:.2}s)", mb / dt, dt);
    e.close().unwrap();
}

/// 组件级计时:定位 flush 路径隐藏开销(临时诊断)。
#[test]
#[ignore]
fn bench_flush_components() {
    use fs3_core::crc32c::crc32c;
    use md5::Digest;
    let buf = rnd(64 * 1024, 1);
    let mut hasher = md5::Md5::new();
    // md5.update 64KiB
    let t0 = std::time::Instant::now();
    for _ in 0..2000 {
        hasher.update(&buf);
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!("md5.update 64KiB x2000: {:.1}us/op", dt * 1e6 / 2000.0);
    // crc32c 64KiB
    let t0 = std::time::Instant::now();
    let mut c = 0u32;
    for _ in 0..2000 {
        c = crc32c(&buf, c);
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!("crc32c    64KiB x2000: {:.1}us/op", dt * 1e6 / 2000.0);
    // io.lock + submit(单 op)
    let dev = fs3_device::open_device(std::path::Path::new("/tmp/bench.img"), false).unwrap();
    let mut io = crate::io::open_io_engine(true).unwrap();
    let mut w = fs3_device::AlignedBuffer::new(64 * 1024).unwrap();
    w.as_mut_slice().copy_from_slice(&buf);
    let t0 = std::time::Instant::now();
    for i in 0..2000u64 {
        let off = 1_056_768 + (i % 5000) * 65_536;
        crate::io::write_all(&mut *io, dev.raw_fd(), w.as_slice(), off).unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!("uring write 64KiB x2000: {:.1}us/op", dt * 1e6 / 2000.0);
    // std Mutex 空锁
    let m = std::sync::Mutex::new(());
    let t0 = std::time::Instant::now();
    for _ in 0..2000 {
        let _g = m.lock().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!("std Mutex lock x2000: {:.3}us/op", dt * 1e6 / 2000.0);
}

// ─────────────────────────── E4 配额 ───────────────────────────

/// 桶配额执行:put 超限拒绝、覆盖放行、删除后恢复。
#[test]
fn quota_enforcement() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    // 设配额 100KiB
    let mut b = e.meta().get_bucket("b1").unwrap().unwrap();
    b.quota = Some(100 * 1024);
    e.meta().commit_bucket_put("b1", &b).unwrap();

    // 60KiB 成功
    e.put("b1", "a", &mut Cursor::new(rnd(60 * 1024, 1)))
        .unwrap();
    // 60KiB 再放 → 超限
    let err = e
        .put("b1", "b", &mut Cursor::new(rnd(60 * 1024, 2)))
        .unwrap_err();
    assert!(matches!(err, Error::QuotaExceeded(_)), "got {err:?}");
    // 未提交:对象不可见
    assert!(e.meta().get_object("b1", "b").unwrap().is_none());
    // 覆盖 a(60KiB→40KiB):净增量 -20KiB → 放行
    e.put("b1", "a", &mut Cursor::new(rnd(40 * 1024, 3)))
        .unwrap();
    // 统计:40KiB
    let b = e.meta().get_bucket("b1").unwrap().unwrap();
    assert_eq!(b.stats.bytes, 40 * 1024);
    // 内联对象也受配额约束(小对象 1KiB)
    let err = e
        .put("b1", "c", &mut Cursor::new(rnd(80 * 1024, 4)))
        .unwrap_err();
    assert!(matches!(err, Error::QuotaExceeded(_)));
}

/// 配额拒绝后无泄漏:位图与元数据一致(check 收敛)。
#[test]
fn quota_rejection_leaves_no_leaks() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let mut b = e.meta().get_bucket("b1").unwrap().unwrap();
    b.quota = Some(10 * 1024);
    e.meta().commit_bucket_put("b1", &b).unwrap();
    // 大对象(> 内联阈值)超限:数据已写盘但事务回滚 → 无泄漏
    let err = e
        .put("b1", "big", &mut Cursor::new(rnd(2 * 1024 * 1024, 5)))
        .unwrap_err();
    assert!(matches!(err, Error::QuotaExceeded(_)));
    let r = e.check_report().unwrap();
    assert!(
        r.leaks.is_empty(),
        "leaks after quota rejection: {:?}",
        r.leaks
    );
}

// ─────────────────────────── C4 泄漏修复 ───────────────────────────

/// 手工制造泄漏(绕过元数据直接置位 + 注入 alloc 记录)→ 修复回收。
#[test]
fn repair_leaks_recovers_bitmap() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    // 分配一个 extent(正常写对象,占用首个 extent)
    e.put("b1", "x", &mut Cursor::new(rnd(64 * 1024, 7)))
        .unwrap();
    let before = e.check_report().unwrap();
    assert!(before.leaks.is_empty());

    // 手工注入泄漏:把第 1 个 extent 置位但不写元数据
    let alloc = e.allocator();
    let leaked_id = 0u64;
    // 直接置位(模拟崩溃后位图与元数据不一致)
    {
        use fs3_alloc::Staged;
        let mut draft = Staged::default();
        // 用 allocator 公开接口构造:allocate 一个再当作泄漏
        let ids = alloc.allocate(&mut draft, 1).unwrap();
        let id = ids[0];
        e.meta()
            .commit(&[Op::Alloc {
                draft: fs3_meta::AllocDraft {
                    alloc: draft.alloc.clone(),
                    ref_inc: vec![],
                    ref_dec: vec![],
                },
            }])
            .unwrap();
        let _ = (leaked_id, id);
    }
    // 泄漏检测应发现
    let r = e.check_report().unwrap();
    assert!(!r.leaks.is_empty(), "expected leaks");
    let leaks_before = r.leaks.len() as u64;

    // 修复
    let rep = e.repair_leaks().unwrap();
    assert_eq!(rep.leaks_found, leaks_before);
    assert_eq!(rep.freed_extents, leaks_before);
    assert!(rep.bytes_reclaimed > 0);

    // 修复后收敛
    let r2 = e.check_report().unwrap();
    assert!(r2.leaks.is_empty(), "leaks after repair: {:?}", r2.leaks);
}

/// W4-2:候选泄漏若仍被 legal_hold 版本引用,拒绝释放;真正不可达泄漏照收。
#[test]
fn repair_leaks_skips_locked_referenced_extents() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.put("b1", "locked", &mut Cursor::new(rnd(64 * 1024, 7)))
        .unwrap();
    e.set_object_legal_hold("b1", "locked", None, true).unwrap();
    let locked_ids: Vec<u64> = e
        .head("b1", "locked")
        .unwrap()
        .unwrap()
        .extents
        .iter()
        .map(|s| u64::from(s.extent_id))
        .collect();
    assert!(!locked_ids.is_empty());
    for &id in &locked_ids {
        e.allocator().restore_occupancy(id, 0, 0);
    }
    let genuine = {
        use fs3_alloc::Staged;
        let mut draft = Staged::default();
        let ids = e.allocator().allocate(&mut draft, 1).unwrap();
        e.meta()
            .commit(&[Op::Alloc {
                draft: fs3_meta::AllocDraft {
                    alloc: draft.alloc.clone(),
                    ref_inc: vec![],
                    ref_dec: vec![],
                },
            }])
            .unwrap();
        ids[0]
    };
    assert!(
        !locked_ids.contains(&genuine),
        "genuine leak must not collide with locked extents"
    );
    let r = e.check_report().unwrap();
    assert!(r.leaks.contains(&genuine));
    for &id in &locked_ids {
        assert!(r.leaks.contains(&id), "zeroed live_bytes → leak candidate");
    }
    let rep = e.repair_leaks().unwrap();
    assert!(rep.skipped_locked >= locked_ids.len() as u64);
    assert_eq!(rep.freed_extents, 1);
    assert!(!e.allocator().test_bit(genuine), "genuine leak reclaimed");
    for &id in &locked_ids {
        assert!(
            e.allocator().test_bit(id),
            "locked extent {id} must not be reclaimed"
        );
    }
    let mut out = Vec::new();
    e.get_to("b1", "locked", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, rnd(64 * 1024, 7));
    assert!(e.head("b1", "locked").unwrap().unwrap().legal_hold);
}

/// 无泄漏时修复为幂等空操作。
#[test]
fn repair_no_leaks_is_noop() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.put("b1", "x", &mut Cursor::new(rnd(32 * 1024, 8)))
        .unwrap();
    let rep = e.repair_leaks().unwrap();
    assert_eq!(rep.leaks_found, 0);
    assert_eq!(rep.freed_extents, 0);
    assert_eq!(rep.bytes_reclaimed, 0);
    // 对象完好
    assert!(e.meta().get_object("b1", "x").unwrap().is_some());
}

// ─────────────────────────── M4 D4 故障注入 ───────────────────────────

/// 故障注入 I/O 引擎:前 `budget` 次写 submit 成功,其后写操作失败(errno)。
/// 读始终透传。用于模拟掉盘(EIO/ENXIO)与磁盘满(ENOSPC)。
struct FlakyIo {
    inner: crate::io::PreadEngine,
    writes_budget: std::sync::atomic::AtomicUsize,
    fail_errno: i32,
}

impl FlakyIo {
    fn new(writes_budget: usize, fail_errno: i32) -> Self {
        FlakyIo {
            inner: crate::io::PreadEngine,
            writes_budget: std::sync::atomic::AtomicUsize::new(writes_budget),
            fail_errno,
        }
    }
}

impl crate::io::IoEngine for FlakyIo {
    fn submit(&mut self, ops: &[crate::io::IoOp]) -> std::io::Result<()> {
        let is_write = ops.iter().any(|op| {
            matches!(
                op,
                crate::io::IoOp::Write { .. }
                    | crate::io::IoOp::WriteFixed { .. }
                    | crate::io::IoOp::Fsync { .. }
            )
        });
        if is_write {
            // checked_sub 防下溢(预算 0 时保持 0 → 立即失败)
            let remaining = self
                .writes_budget
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |r| r.checked_sub(1),
                )
                .unwrap_or(0);
            if remaining == 0 {
                return Err(std::io::Error::from_raw_os_error(self.fail_errno));
            }
        }
        self.inner.submit(ops)
    }

    fn name(&self) -> &'static str {
        "flaky"
    }
}

/// 掉盘模拟(EIO):设备写失败 → put 报错 + degraded 标志置位(只读降级)。
#[test]
fn io_failure_marks_degraded() {
    let (_d, mut cfg) = setup();
    // 预算 4 次写:数据段写入会消耗多次 submit(64KiB chunk 粒度,1MiB = 16 次)
    let flaky = Arc::new(Mutex::new(
        Box::new(FlakyIo::new(4, libc::EIO)) as Box<dyn IoEngine>
    ));
    cfg.debug_io = Some(flaky);
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    assert!(!e.degraded());

    let r = e.put("b1", "k", &mut Cursor::new(rnd(1024 * 1024, 3)));
    // 预算耗尽后某次写失败 → 错误(不 panic、不撕裂)
    assert!(r.is_err());
    assert!(e.degraded(), "device I/O failure must mark degraded");

    // 其他操作不可用:写失败持续(降级期行为由 S3 层拒绝写)
}

/// ENOSPC(磁盘满)不触发降级,映射 507 语义(服务层校验)。
#[test]
fn enospc_does_not_mark_degraded() {
    let (_d, mut cfg) = setup();
    let flaky = Arc::new(Mutex::new(
        Box::new(FlakyIo::new(1, libc::ENOSPC)) as Box<dyn IoEngine>
    ));
    cfg.debug_io = Some(flaky);
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();

    let r = e.put("b1", "k", &mut Cursor::new(rnd(256 * 1024, 4)));
    assert!(r.is_err());
    // 磁盘满是健康状态:不降级,服务层返回 InsufficientStorage(507)
    assert!(!e.degraded(), "ENOSPC must NOT mark degraded");
}

/// Drop 注入:掉盘后读仍可(只读模式保留),写持续失败且 flag 保持粘性。
#[test]
fn degraded_is_sticky_across_failures() {
    let (_d, mut cfg) = setup();
    // 预算 0:所有写立即失败(ENXIO 掉盘)
    let flaky = Arc::new(Mutex::new(
        Box::new(FlakyIo::new(0, libc::ENXIO)) as Box<dyn IoEngine>
    ));
    cfg.debug_io = Some(flaky);
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    let _ = e.put("b1", "k", &mut Cursor::new(rnd(512 * 1024, 5)));
    assert!(e.degraded());
    // 后续写依旧失败(预算已耗尽)
    let r = e.put("b1", "k2", &mut Cursor::new(rnd(64 * 1024, 6)));
    assert!(r.is_err());
    assert!(e.degraded());
}

#[test]
fn debug_flaky_eio() {
    let (_d, mut cfg) = setup();
    let flaky = Arc::new(Mutex::new(
        Box::new(FlakyIo::new(4, libc::EIO)) as Box<dyn IoEngine>
    ));
    cfg.debug_io = Some(flaky);
    let mut e = Engine::open(&cfg).unwrap();
    println!("degraded after open: {}", e.degraded());
    let r = e.put("b1", "k", &mut Cursor::new(rnd(1024 * 1024, 3)));
    println!("put result: {:?}", r.as_ref().err());
    println!("degraded after put: {}", e.degraded());
    let r2 = e.put("b1", "k2", &mut Cursor::new(rnd(1024 * 1024, 7)));
    println!("put2 result: {:?}", r2.as_ref().err());
    println!("degraded after put2: {}", e.degraded());
}

// ───────────────────────── M5 etag=fast(CRC32C 降级)─────────────────────────

/// etag=fast:小对象内联 + 大对象 extent + multipart 分片,ETag 均为 CRC32C。
/// crc32c 的 ETag 布局:[0u8;12] + crc32c(data).to_be_bytes()。
#[test]
fn etag_fast_crc32c_mode() {
    use fs3_core::EtagMode;

    let (_d, mut cfg) = setup();
    cfg.etag_mode = EtagMode::Crc32c;
    let mut e = open_engine(&cfg);

    // 1) 内联小对象(< small_object_limit)
    let small = rnd(8 * 1024, 9);
    let m = e
        .put("b1", "small", &mut Cursor::new(small.clone()))
        .unwrap();
    assert!(m.inline.is_some(), "small object should be inline");
    let mut want = [0u8; 16];
    want[12..16].copy_from_slice(&crc32c(&small, 0).to_be_bytes());
    assert_eq!(m.etag, want, "inline etag = crc32c");
    let sm: [u8; 16] = md5::Md5::digest(&small).into();
    assert_ne!(m.etag, sm, "should NOT be md5");

    // 2) 大对象(extent 路径)
    let big = rnd(512 * 1024, 11);
    let m2 = e.put("b1", "big", &mut Cursor::new(big.clone())).unwrap();
    assert!(m2.inline.is_none(), "big object should go to extent");
    let mut want2 = [0u8; 16];
    want2[12..16].copy_from_slice(&crc32c(&big, 0).to_be_bytes());
    assert_eq!(m2.etag, want2, "extent etag = crc32c");

    // 3) multipart 分片(内联分片)
    let upload = e
        .create_multipart(
            "b1",
            "mp",
            Some("application/octet-stream"),
            Vec::new(),
            vec![],
            Vec::new(),
            None,
            None,
            None,
        )
        .unwrap();
    let part_data = rnd(16 * 1024, 13);
    e.upload_part(&upload, 1, &mut Cursor::new(part_data.clone()), None, None)
        .unwrap();
    let stored = e.list_parts(&upload).unwrap();
    let (_, pm) = &stored[0];
    let mut wantp = [0u8; 16];
    wantp[12..16].copy_from_slice(&crc32c(&part_data, 0).to_be_bytes());
    assert_eq!(pm.etag, wantp, "part etag = crc32c");

    // 4) 读回内容一致
    let mut out = Vec::new();
    let n = e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(n as usize, big.len());
    assert_eq!(&out, &big, "big readback intact");
    let mut out2 = Vec::new();
    e.get_to("b1", "small", 0..u64::MAX, &mut out2).unwrap();
    assert_eq!(&out2, &small, "small readback intact");
    e.close().unwrap();
}

/// md5 模式(默认)回归:ETag 仍为 MD5(与 etag=fast 互斥)。
#[test]
fn etag_md5_mode_default() {
    use fs3_core::EtagMode;
    let (_d, cfg) = setup();
    assert_eq!(cfg.etag_mode, EtagMode::Md5, "default is md5");
    let mut e = open_engine(&cfg);
    let data = rnd(200 * 1024, 5);
    let m = e.put("b1", "k", &mut Cursor::new(data.clone())).unwrap();
    let want: [u8; 16] = md5::Md5::digest(&data).into();
    assert_eq!(m.etag, want, "default etag = md5");
    e.close().unwrap();
}

/// REVIEW §4.12:multipart 空洞——只完成分片 1、3(2 号未传),对象必须只含
/// 请求子集;ETag-N 的 N = 请求分片数(2,此前按最大分片号=3 补齐 0 得 "-3");
/// EntityTooSmall 只检查请求内非末分片(未列出的 1 号小分片不触发)。
#[test]
fn multipart_complete_with_holes_uses_request_subset() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e
        .create_multipart(
            "b1",
            "sub",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    // 1 号分片极小(<5MiB,若被检查 EntityTooSmall 必失败;此处刻意不列
    // 入 complete 请求,验证不参与检查)
    let _p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0x11u8; 100]), None, None)
        .unwrap();
    // 2 号分片存在但**不**出现在 complete 请求里(5MiB)
    let p2 = e
        .upload_part(
            &uid,
            2,
            &mut Cursor::new(vec![0x22u8; 5 * 1024 * 1024]),
            None,
            None,
        )
        .unwrap();
    // 3 号分片 5MiB
    let p3 = e
        .upload_part(
            &uid,
            3,
            &mut Cursor::new(vec![0x33u8; 5 * 1024 * 1024]),
            None,
            None,
        )
        .unwrap();

    // 只完成 [1, 3]:1 是非末分片但 <5MiB → 按请求子集检查应报 EntityTooSmall
    // (这是 AWS 语义:首片 <5MiB 本来就非法;REVIEW 场景是「大分片 + 空洞」)

    // 先验证正常的空洞场景:完成 [2, 3](1 号未列出、虽然小,但不参与检查)
    let m = e
        .complete_multipart(
            "b1",
            "sub",
            &uid,
            &[cp(2, p2.etag_hex()), cp(3, p3.etag_hex())],
            None,
            None,
        )
        .unwrap();
    assert_eq!(m.size, 10 * 1024 * 1024, "only parts 2+3 combined");
    assert_eq!(
        m.parts,
        vec![5 * 1024 * 1024, 5 * 1024 * 1024],
        "parts 数组紧凑无空洞占位"
    );
    // ETag-N:N = 请求分片数 = 2(此前按最大分片号 3 补齐 → "-3")
    let full = m.etag_full();
    assert!(
        full.ends_with("-2"),
        "ETag-N must equal requested part count: {full}"
    );
    // 内容 = p2 ++ p3
    let mut out = Vec::new();
    e.get_to("b1", "sub", 0..m.size, &mut out).unwrap();
    assert_eq!(&out[..5 * 1024 * 1024], &vec![0x22u8; 5 * 1024 * 1024][..]);
    assert_eq!(&out[5 * 1024 * 1024..], &vec![0x33u8; 5 * 1024 * 1024][..]);
    // 未列出的小分片 p1 不触发 EntityTooSmall(已按请求子集完成)

    // 单独验证 EntityTooSmall 仍对请求内非末分片生效:新会话,完成 [1, 2]
    // 必须先重开(上一个会话已 completed)
    let uid2 = e
        .create_multipart(
            "b1",
            "sub2",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let a = e
        .upload_part(&uid2, 1, &mut Cursor::new(vec![0xAAu8; 100]), None, None)
        .unwrap();
    let b = e
        .upload_part(
            &uid2,
            2,
            &mut Cursor::new(vec![0xBBu8; 5 * 1024 * 1024]),
            None,
            None,
        )
        .unwrap();
    let r = e.complete_multipart(
        "b1",
        "sub2",
        &uid2,
        &[cp(1, a.etag_hex()), cp(2, b.etag_hex())],
        None,
        None,
    );
    assert!(matches!(r, Err(Error::PartTooSmall(_))), "{r:?}");
    e.close().unwrap();
}

// ─────────────────── 版本化(M10/ADR-11,V2 引擎分叉) ───────────────────

/// 测试直写桶版本化状态(V3 PutBucketVersioning 落地前的元数据层入口)。
fn set_versioning(e: &Engine, state: VersioningState) {
    let mut m = e.meta().get_bucket("b1").unwrap().unwrap();
    m.versioning = state;
    e.meta().commit_bucket_put("b1", &m).unwrap();
}

/// 测试辅助:确定性改写版本键/单键条目的 mtime(D1a 的 mtime 裁决需要
/// 确定性时间序;引擎写路径 mtime 取墙钟秒,快路径测试内多次写会同秒)。
/// `vk = None` 指遗留未版本化单键;统计零 delta(仅改时间戳)。
fn set_entry_mtime(e: &Engine, bucket: &str, key: &str, vk: &[u8; 16], mtime: i64) {
    let mut m = if *vk == VK_NULL {
        // null 族:null 槽或遗留单键,哪个存在取哪个(D1a-4 同口径)
        match e.meta().get_object_version(bucket, key, &VK_NULL).unwrap() {
            Some(m) => (m, Some(VK_NULL)),
            None => (e.meta().get_object(bucket, key).unwrap().unwrap(), None),
        }
    } else {
        (
            e.meta()
                .get_object_version(bucket, key, vk)
                .unwrap()
                .unwrap(),
            Some(*vk),
        )
    };
    m.0.mtime = mtime;
    let (meta, target) = (m.0, m.1);
    if meta.is_delete_marker {
        e.meta()
            .commit_object_delete_current(
                bucket,
                key,
                target.as_ref(),
                &meta,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
    } else {
        match target {
            Some(vk) => e
                .meta()
                .commit_object_put_version(
                    bucket,
                    key,
                    &vk,
                    &meta,
                    AllocDraft::default(),
                    StatsDelta::default(),
                )
                .unwrap(),
            None => e
                .meta()
                .commit_object_put(
                    bucket,
                    key,
                    &meta,
                    AllocDraft::default(),
                    StatsDelta::default(),
                )
                .unwrap(),
        };
    }
}

fn stats_of(e: &Engine) -> (u64, u64) {
    let b = e.meta().get_bucket("b1").unwrap().unwrap();
    (b.stats.objects, b.stats.bytes)
}

fn read_all(e: &Engine, bucket: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::new();
    e.get_to(bucket, key, 0..u64::MAX, &mut out).unwrap();
    out
}

fn read_version(e: &Engine, bucket: &str, key: &str, vk: &[u8; 16]) -> Vec<u8> {
    let mut out = Vec::new();
    e.get_to_version(bucket, key, Some(vk), 0..u64::MAX, &mut out)
        .unwrap();
    out
}

#[test]
fn versioned_put_keeps_old_version_segments() {
    // V2-2 Enabled:覆盖写 = 新版本;旧版本段不释放(旧版本元数据继续持有)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(100_000, 1); // extent 路径(> small_object_limit)
    let d2 = rnd(200_000, 2);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .expect("Enabled 桶 PUT 返回 vk");
    let v2 = e
        .put("b1", "k", &mut Cursor::new(d2.clone()))
        .unwrap()
        .version_id
        .unwrap();
    assert!(v2 > v1, "vk 字典序 = 时间序");
    // 当前版本 = 新写入;旧版本段不动、可精确读
    assert_eq!(read_all(&e, "b1", "k"), d2);
    assert_eq!(e.head_version("b1", "k", None).unwrap().size, 200_000);
    assert_eq!(read_version(&e, "b1", "k", &v1), d1);
    // 统计 D5:两个版本都入账(覆盖写 += 新版本,旧版本不扣)
    assert_eq!(stats_of(&e), (2, 300_000));
    assert_eq!(e.meta().list_key_versions("b1", "k").unwrap().len(), 2);
    // 物理删除两版本 → 段逐一释放归零、统计归零
    assert!(e.delete_version("b1", "k", Some(v1)).unwrap().is_some());
    assert_eq!(stats_of(&e), (1, 200_000));
    assert!(e.delete_version("b1", "k", Some(v2)).unwrap().is_some());
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0, "版本段全部释放");
    e.close().unwrap();
}

#[test]
fn versioned_delete_marker_and_version_delete() {
    // V2-3 Enabled:无 versionId DELETE = 删除标记(不动数据段,零 delta);
    // 带 versionId = 物理删除(幂等);当前回退语义。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(100_000, 3);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    // 无 versionId DELETE → 删除标记
    let dm = e.delete("b1", "k").unwrap().unwrap();
    assert!(dm.is_delete_marker);
    let vdm = dm.version_id.unwrap();
    assert!(vdm > v1);
    assert_eq!(stats_of(&e), (1, 100_000), "删除标记零 delta");
    // 无 versionId 命中当前标记 → 可区分错误变体(协议层 404)
    let err = e.head_version("b1", "k", None).unwrap_err();
    assert!(
        matches!(err, Error::DeleteMarker(ref m) if *m == hex::encode(vdm)),
        "got {err:?}"
    );
    let err = e
        .get_to_version("b1", "k", None, 0..u64::MAX, &mut Vec::new())
        .unwrap_err();
    assert!(matches!(err, Error::DeleteMarker(_)));
    // 带 versionId 命中标记 → 同一变体(协议层 405)
    let err = e.head_version("b1", "k", Some(&vdm)).unwrap_err();
    assert!(matches!(err, Error::DeleteMarker(_)));
    // 数据段未被标记触碰:旧版本仍可读
    assert_eq!(read_version(&e, "b1", "k", &v1), d1);
    // 列表隐藏当前为标记的 key
    assert!(e.list_objects("b1", "").unwrap().is_empty());
    assert!(e
        .list_objects_page("b1", "", None, None, 10)
        .unwrap()
        .items
        .is_empty());
    // 重复 DELETE = 再插一条标记(vk 递增,与 AWS 一致)
    let dm2 = e.delete("b1", "k").unwrap().unwrap();
    let vdm2 = dm2.version_id.unwrap();
    assert!(vdm2 > vdm);
    // 版本不存在 → 幂等 Ok(None)(AWS 语义)
    assert!(e
        .delete_version("b1", "k", Some([0x99u8; 16]))
        .unwrap()
        .is_none());
    // 删除两条标记版本(零 delta)→ 当前回退到 v1 数据版本
    assert!(
        e.delete_version("b1", "k", Some(vdm2))
            .unwrap()
            .unwrap()
            .is_delete_marker
    );
    assert_eq!(stats_of(&e), (1, 100_000), "标记版本删除零 delta");
    assert!(e.delete_version("b1", "k", Some(vdm)).unwrap().is_some());
    assert_eq!(
        read_all(&e, "b1", "k"),
        d1,
        "标记消失后当前回退到次新数据版本"
    );
    // Off 桶带 versionId → InvalidArgument(协议层拦截兜底)
    e.ensure_bucket("b2").unwrap();
    assert!(matches!(
        e.delete_version("b2", "k", Some([1u8; 16])),
        Err(Error::InvalidArgument(_))
    ));
    e.close().unwrap();
}

#[test]
fn off_fast_path_state_aware_reads_equivalent() {
    // F-1:桶状态感知读变体(head_version_for / read_at_version_for /
    // object_segments_version_for / delete_version_for)在 Off 桶走单键
    // 点读快速路径(不反扫),语义与全量 D1a 逐值等价;版本化桶行为不变。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    // Off 桶(默认):内联小对象 + extent 大对象两形态
    let small = rnd(100, 7);
    let big = rnd(100_000, 8);
    e.put("b1", "s", &mut Cursor::new(small.clone())).unwrap();
    e.put("b1", "g", &mut Cursor::new(big.clone())).unwrap();
    for (key, data) in [("s", &small), ("g", &big)] {
        // head:状态感知变体 = 全量 D1a
        let base = e.head_version("b1", key, None).unwrap();
        let fast = e
            .head_version_for("b1", key, None, VersioningState::Off)
            .unwrap();
        assert_eq!(base, fast);
        // read_at:数据逐字节一致
        let mut b1 = vec![0u8; data.len()];
        let n1 = e.read_at_version("b1", key, None, 0, &mut b1).unwrap();
        let mut b2 = vec![0u8; data.len()];
        let n2 = e
            .read_at_version_for("b1", key, None, 0, &mut b2, VersioningState::Off, None)
            .unwrap();
        assert_eq!(n1, n2);
        assert_eq!(b1, b2);
        assert_eq!(b2, *data);
        // segments:裁剪结果一致
        let seg = |segs: Option<Vec<DevSegment>>| {
            segs.unwrap()
                .iter()
                .map(|s| (s.dev_offset, s.len))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            seg(e
                .object_segments_version("b1", key, None, 0, data.len() as u64)
                .unwrap()),
            seg(e
                .object_segments_version_for(
                    "b1",
                    key,
                    None,
                    0,
                    data.len() as u64,
                    VersioningState::Off
                )
                .unwrap()),
        );
    }
    // Off 桶不存在键 → NotFound(协议层 404;快速路径同样判定)
    let err = e
        .head_version_for("b1", "ghost", None, VersioningState::Off)
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    assert!(e
        .object_segments_version_for("b1", "ghost", None, 0, 1, VersioningState::Off)
        .unwrap()
        .is_none());
    // delete_version_for(Off)= 物理删除,与 delete_version 同语义
    assert!(e
        .delete_version_for("b1", "s", None, VersioningState::Off)
        .unwrap()
        .is_some());
    assert!(e.head("b1", "s").unwrap().is_none());
    assert!(e
        .delete_version_for("b1", "ghost", None, VersioningState::Off)
        .unwrap()
        .is_none());
    // Off 桶带非 null versionId → InvalidArgument(同 delete_version 口径)
    assert!(matches!(
        e.delete_version_for("b1", "g", Some([1u8; 16]), VersioningState::Off),
        Err(Error::InvalidArgument(_))
    ));

    // —— 版本化桶:_for 与基础变体同路,行为不变 ——
    set_versioning(&e, VersioningState::Enabled);
    e.put("b1", "k", &mut Cursor::new(rnd(1000, 9))).unwrap();
    e.put("b1", "k", &mut Cursor::new(rnd(2000, 10))).unwrap();
    let base = e.head_version("b1", "k", None).unwrap();
    let fast = e
        .head_version_for("b1", "k", None, VersioningState::Enabled)
        .unwrap();
    assert_eq!(base, fast);
    assert_eq!(fast.size, 2000);
    // 当前为删除标记:两变体同报 DeleteMarker
    e.delete("b1", "k").unwrap();
    let e1 = e.head_version("b1", "k", None).unwrap_err();
    let e2 = e
        .head_version_for("b1", "k", None, VersioningState::Enabled)
        .unwrap_err();
    assert!(matches!(e1, Error::DeleteMarker(_)));
    assert!(matches!(e2, Error::DeleteMarker(_)));
    e.close().unwrap();
}

#[test]
fn suspended_null_slot_semantics() {
    // V2-2/V2-3 Suspended:写/删落 null 槽原地覆盖;旧 null 数据版本
    // release + 统计先扣后加;对外 VersionId = "null"。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Suspended);
    let d1 = rnd(100_000, 4);
    let d2 = rnd(60_000, 5);
    let m1 = e.put("b1", "k", &mut Cursor::new(d1.clone())).unwrap();
    assert_eq!(m1.version_id, None, "null 槽 version_id = None");
    assert_eq!(read_all(&e, "b1", "k"), d1);
    assert_eq!(stats_of(&e), (1, 100_000));
    // null 槽原地覆盖:条目数不变,统计净额正确
    let m2 = e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap();
    assert_eq!(m2.version_id, None);
    assert_eq!(read_all(&e, "b1", "k"), d2);
    assert_eq!(stats_of(&e), (1, 60_000), "覆盖 null = 先扣旧再加新");
    assert_eq!(
        e.meta().list_key_versions("b1", "k").unwrap().len(),
        1,
        "原地覆盖,版本条目数不变"
    );
    // DELETE → 标记落 null 槽(覆盖旧 null 数据版本:release + 扣减)
    let dm = e.delete("b1", "k").unwrap().unwrap();
    assert!(dm.is_delete_marker && dm.version_id.is_none());
    assert_eq!(stats_of(&e), (0, 0));
    let err = e.head_version("b1", "k", None).unwrap_err();
    assert!(matches!(err, Error::DeleteMarker(ref m) if m == "null"));
    // 标记上再写 = 覆盖标记(标记未入账 → +1)
    let d3 = rnd(1_000, 6); // 内联路径
    e.put("b1", "k", &mut Cursor::new(d3.clone())).unwrap();
    assert_eq!(read_all(&e, "b1", "k"), d3);
    assert_eq!(stats_of(&e), (1, 1_000));
    // 物理删 null 槽(模拟 ?versionId=null)
    assert!(e
        .delete_version("b1", "k", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert_eq!(stats_of(&e), (0, 0));
    assert!(matches!(
        e.head_version("b1", "k", None),
        Err(Error::NotFound(_))
    ));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn enabled_to_suspended_transition_reads() {
    // Enabled 时代版本 + Suspended 时代 null 槽共存(D1a 裁决):
    // 当前版本 = {null 槽, 最大真实 vk} 取 mtime 最大,mtime 相等取真实版本;
    // Enabled 时代版本仍可精确寻址。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(50_000, 7);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    let m1 = e.head_version("b1", "k", None).unwrap().mtime;
    set_versioning(&e, VersioningState::Suspended);
    let d2 = rnd(40_000, 8);
    let m2 = e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap();
    assert_eq!(m2.version_id, None);
    // D1a:null 槽 mtime ≥ 真实版本;同秒相等时真实版本胜出(引擎写 mtime
    // 取墙钟秒,测试内同秒)——把 null 槽 mtime 确定性拨快,恢复「挂起期
    // 写入更晚」的真实时序
    set_entry_mtime(&e, "b1", "k", &VK_NULL, m1 + 10);
    assert_eq!(read_all(&e, "b1", "k"), d2, "null 槽 mtime 更大 → 当前版本");
    assert_eq!(read_version(&e, "b1", "k", &v1), d1);
    assert_eq!(stats_of(&e), (2, 90_000));
    // D1a 同秒 tie → 真实版本为当前(重启用后的写必然后于挂起期写)
    set_entry_mtime(&e, "b1", "k", &VK_NULL, m1);
    assert_eq!(read_all(&e, "b1", "k"), d1, "mtime 相等取真实版本");
    e.close().unwrap();
}

#[test]
fn copy_object_version_addressing() {
    // V2-4:源版本寻址(指定 → 精确读;未指定 → 当前版本);目标复用写分叉;
    // 段共享零 I/O;复制删除标记 → 目标同落标记;标记 → Off 桶拒绝。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.ensure_bucket("b2").unwrap(); // Off 桶
    set_versioning(&e, VersioningState::Enabled); // b1 Enabled
    let d1 = rnd(100_000, 9);
    let d2 = rnd(100_000, 10);
    let v1 = e
        .put("b1", "src", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    let v2 = e
        .put("b1", "src", &mut Cursor::new(d2.clone()))
        .unwrap()
        .version_id
        .unwrap();
    // 未指定版本 → 复制当前版本(d2);目标 = 新版本(段共享,零数据 I/O)
    let mc = e
        .copy_object("b1", "src", "b1", "dst", None, None, None)
        .unwrap();
    let vc = mc.version_id.unwrap();
    assert!(vc > v2, "复制落新版本");
    assert_eq!(read_all(&e, "b1", "dst"), d2);
    assert_eq!(
        mc.extents,
        e.head_version("b1", "src", None).unwrap().extents,
        "段共享(share_object,无数据拷贝)"
    );
    // 指定历史版本 → 精确复制 d1
    let mh = e
        .copy_object_version(
            "b1",
            "src",
            Some(&v1),
            "b1",
            "dst-hist",
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(read_all(&e, "b1", "dst-hist"), d1);
    assert!(mh.version_id.unwrap() > vc);
    assert_eq!(stats_of(&e), (4, 400_000));
    // 源历史版本删除后,复制目标仍可读(共享段引用计数)
    assert!(e.delete_version("b1", "src", Some(v1)).unwrap().is_some());
    assert_eq!(read_all(&e, "b1", "dst-hist"), d1);
    assert_eq!(stats_of(&e), (3, 300_000));
    // 复制删除标记(源 = 标记版本)→ 目标同落标记,零入账
    let dm = e.delete("b1", "src").unwrap().unwrap();
    let vdm = dm.version_id.unwrap();
    let md = e
        .copy_object_version(
            "b1",
            "src",
            Some(&vdm),
            "b1",
            "dst-dm",
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert!(md.is_delete_marker && md.version_id.is_some());
    assert!(md.extents.is_empty() && md.inline.is_none());
    assert!(matches!(
        e.head_version("b1", "dst-dm", None),
        Err(Error::DeleteMarker(_))
    ));
    assert_eq!(stats_of(&e), (3, 300_000), "标记复制零入账");
    // 未指定版本且源当前 = 标记 → 同样复制标记(§3.4.5)
    let md2 = e
        .copy_object("b1", "src", "b1", "dst-dm2", None, None, None)
        .unwrap();
    assert!(md2.is_delete_marker);
    // 复制标记到 Off 桶 → InvalidArgument
    assert!(matches!(
        e.copy_object_version("b1", "src", Some(&vdm), "b2", "x", None, None, None, None),
        Err(Error::InvalidArgument(_))
    ));
    // Off → Enabled 复制:目标落新版本
    e.put("b2", "s", &mut Cursor::new(rnd(1_000, 11))).unwrap();
    let mx = e
        .copy_object("b2", "s", "b1", "from-off", None, None, None)
        .unwrap();
    assert!(mx.version_id.is_some());
    e.close().unwrap();
}

#[test]
fn complete_multipart_lands_new_version() {
    // V2-5:Complete = 新版本(Enabled)/覆盖 null 槽(Suspended);
    // 会话/分片键不变;幂等重放不重复入账。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let p1 = rnd(100_000, 12);
    let uid = e
        .create_multipart(
            "b1",
            "mp",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let part = e
        .upload_part(&uid, 1, &mut Cursor::new(p1.clone()), None, None)
        .unwrap();
    let m = e
        .complete_multipart("b1", "mp", &uid, &[cp(1, part.etag_hex())], None, None)
        .unwrap();
    let v1 = m.version_id.expect("Complete 落新版本");
    assert_eq!(read_all(&e, "b1", "mp"), p1);
    assert_eq!(stats_of(&e), (1, 100_000));
    // 幂等重放:相同 ETag,不重复入账
    let replay = e
        .complete_multipart("b1", "mp", &uid, &[cp(1, part.etag_hex())], None, None)
        .unwrap();
    assert_eq!(replay.etag, m.etag);
    assert_eq!(stats_of(&e), (1, 100_000));
    // 第二次 Complete(新会话)= 第二个版本;旧版本可读
    let p2 = rnd(120_000, 13);
    let uid2 = e
        .create_multipart(
            "b1",
            "mp",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let part2 = e
        .upload_part(&uid2, 1, &mut Cursor::new(p2.clone()), None, None)
        .unwrap();
    let m3 = e
        .complete_multipart("b1", "mp", &uid2, &[cp(1, part2.etag_hex())], None, None)
        .unwrap();
    assert!(m3.version_id.unwrap() > v1);
    assert_eq!(read_all(&e, "b1", "mp"), p2);
    assert_eq!(read_version(&e, "b1", "mp", &v1), p1);
    assert_eq!(stats_of(&e), (2, 220_000));
    // Suspended:Complete 覆盖 null 槽(version_id = None)
    set_versioning(&e, VersioningState::Suspended);
    let p3 = rnd(80_000, 14);
    let uid3 = e
        .create_multipart(
            "b1",
            "mp",
            None,
            vec![],
            Vec::new(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let part3 = e
        .upload_part(&uid3, 1, &mut Cursor::new(p3.clone()), None, None)
        .unwrap();
    let m4 = e
        .complete_multipart("b1", "mp", &uid3, &[cp(1, part3.etag_hex())], None, None)
        .unwrap();
    assert_eq!(m4.version_id, None, "Suspended Complete 落 null 槽");
    // D1a:null 槽与真实版本同秒时真实版本胜出;确定性拨快 null 槽 mtime,
    // 恢复「挂起期写入更晚」的真实时序后再断言当前版本
    let latest_real = e.meta().max_real_vk("b1", "mp").unwrap().unwrap();
    let real_mtime = e
        .head_version("b1", "mp", Some(&latest_real))
        .unwrap()
        .mtime;
    set_entry_mtime(&e, "b1", "mp", &VK_NULL, real_mtime + 10);
    assert_eq!(read_all(&e, "b1", "mp"), p3);
    assert_eq!(stats_of(&e), (3, 300_000));
    e.close().unwrap();
}

#[test]
fn vk_anti_rollback_engine_level() {
    // V2-1 防回拨:注入远未来版本(模拟回拨前高水位),新写 vk 仍严格递增。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d0 = rnd(1_000, 15);
    let m0 = e.put("b1", "k", &mut Cursor::new(d0)).unwrap();
    // 远未来版本条目(时钟回拨场景的高水位;直写版本键)
    let future_vk = new_version_vk(now_us() + 10_000_000_000_000, None).unwrap();
    let mut fm = m0.clone();
    fm.version_id = Some(future_vk);
    e.meta()
        .commit_object_put_version(
            "b1",
            "k",
            &future_vk,
            &fm,
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: fm.size as i64,
            },
        )
        .unwrap();
    // 新写:prev_ts = 远未来 → 新 vk 时间戳 = prev+1,字典序仍最大
    let d1 = rnd(2_000, 16);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    assert!(v1 > future_vk, "时钟回拨后新 vk 仍递增");
    assert_eq!(read_all(&e, "b1", "k"), d1, "当前版本 = 新写入");
    e.close().unwrap();
}

#[test]
fn delete_bucket_force_releases_all_versions() {
    // V2-3 delete_bucket:枚举全部版本条目(含删除标记)逐一释放。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    e.put("b1", "a", &mut Cursor::new(rnd(50_000, 17))).unwrap();
    e.put("b1", "a", &mut Cursor::new(rnd(50_000, 18))).unwrap();
    e.put("b1", "b", &mut Cursor::new(rnd(1_000, 19))).unwrap();
    e.delete("b1", "b").unwrap(); // 删除标记残留
    assert!(e.delete_bucket("b1", false).is_err(), "版本残留 → 非空拒绝");
    e.delete_bucket("b1", true).unwrap();
    assert!(e.meta().get_bucket("b1").unwrap().is_none());
    assert_eq!(e.allocator().allocated_count(), 0, "全部版本段释放");
    e.close().unwrap();
}

#[test]
fn object_lock_blocks_version_delete_and_force_bucket() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let meta = e.put("b1", "k", &mut Cursor::new(vec![1u8; 32])).unwrap();
    let vk = meta.version_id.unwrap();
    e.set_object_retention(
        "b1",
        "k",
        Some(&vk),
        Some(fs3_core::Retention {
            mode: fs3_core::RetentionMode::Governance,
            retain_until: e.lock_now() + 86_400,
        }),
    )
    .unwrap();
    assert!(
        matches!(
            e.delete_version("b1", "k", Some(vk)),
            Err(Error::AccessDenied(_))
        ),
        "GOVERNANCE 无 bypass 拒绝"
    );
    assert!(e
        .delete_version_with_lock("b1", "k", Some(vk), VersioningState::Enabled, true)
        .unwrap()
        .is_some());
    let meta = e.put("b1", "c", &mut Cursor::new(vec![2u8; 16])).unwrap();
    let vk = meta.version_id.unwrap();
    e.set_object_retention(
        "b1",
        "c",
        Some(&vk),
        Some(fs3_core::Retention {
            mode: fs3_core::RetentionMode::Compliance,
            retain_until: e.lock_now() + 86_400,
        }),
    )
    .unwrap();
    assert!(matches!(
        e.delete_version_with_lock("b1", "c", Some(vk), VersioningState::Enabled, true),
        Err(Error::AccessDenied(_))
    ));
    assert!(matches!(
        e.delete_bucket("b1", true),
        Err(Error::AccessDenied(_))
    ));
    e.close().unwrap();
}

#[test]
fn unversioned_bucket_paths_untouched() {
    // D1 硬承诺:Off 桶路径零改动——无版本键残留、version_id 恒 None、
    // 带 versionId 删除被拒绝、DELETE 幂等。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let m = e.put("b1", "k", &mut Cursor::new(rnd(1_000, 20))).unwrap();
    assert_eq!(m.version_id, None);
    assert!(!m.is_delete_marker);
    assert!(e.meta().list_key_versions("b1", "k").unwrap().is_empty());
    let entries = e.meta().list_object_entries("b1").unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].1.is_none(), "Off 桶恒为未版本化单键");
    assert!(matches!(
        e.delete_version("b1", "k", Some([1u8; 16])),
        Err(Error::InvalidArgument(_))
    ));
    assert!(e.delete("b1", "k").unwrap().is_some());
    assert!(e.delete("b1", "k").unwrap().is_none());
    e.close().unwrap();
}

#[test]
fn versioned_engine_reopen_recovers() {
    // §3.4.6:版本化数据崩溃重开 = 恢复零改动(可达性扫描覆盖版本键,
    // 无泄漏误报;重开后版本寻址/当前解析正常)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(100_000, 21);
    let d2 = rnd(100_000, 22);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap();
    e.delete("b1", "ghost").unwrap(); // 标记条目(无段引用)
    e.close().unwrap();
    drop(e); // 释放 rocksdb LOCK 后重开
             // 重开:恢复扫描必须识别版本键段引用(o: 前缀双形态)
    let mut e = Engine::open(&cfg).unwrap();
    assert_eq!(read_all(&e, "b1", "k"), d2);
    assert_eq!(read_version(&e, "b1", "k", &v1), d1);
    assert_eq!(stats_of(&e), (2, 200_000));
    // 无泄漏:删除全部版本后位图归零
    let v2 = e.head_version("b1", "k", None).unwrap().version_id.unwrap();
    e.delete_version("b1", "k", Some(v1)).unwrap();
    e.delete_version("b1", "k", Some(v2)).unwrap();
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn check_versioned_mixed_bucket_converges() {
    // V5-2:混合版本桶(Off 遗留单键 + Enabled 多版本 + 删除标记 +
    // Suspended null 槽数据/标记)重开引擎(check 可达性重建全路径)→
    // 零误报泄漏;再注入游离段 → check 必须检出(零漏报)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    e.put("b1", "legacy", &mut Cursor::new(rnd(100_000, 60))) // Off 时代遗留单键(段形态)
        .unwrap();
    set_versioning(&e, VersioningState::Enabled);
    e.put("b1", "k1", &mut Cursor::new(rnd(90_000, 61)))
        .unwrap();
    e.put("b1", "k1", &mut Cursor::new(rnd(800, 62))).unwrap(); // 内联形态版本
    e.put("b1", "k2", &mut Cursor::new(rnd(70_000, 63)))
        .unwrap();
    assert!(e.delete("b1", "k2").unwrap().unwrap().is_delete_marker); // 标记当前
    set_versioning(&e, VersioningState::Suspended);
    e.put("b1", "k3", &mut Cursor::new(rnd(60_000, 64)))
        .unwrap(); // null 槽数据
    assert!(e.delete("b1", "k4").unwrap().unwrap().is_delete_marker); // null 槽标记
    e.close().unwrap();
    drop(e); // 释放 rocksdb LOCK 后重开(可达性重建覆盖全形态条目)

    let e = Engine::open(&cfg).unwrap();
    let r = e.check_report().unwrap();
    assert!(
        r.leaks.is_empty(),
        "混合版本桶重建后零误报泄漏: {:?}",
        r.leaks
    );
    // 删除标记/空 extents 条目不误判:桶统计口径另行覆盖(D5);
    // 注入游离段(置位 + a: 记录,无元数据引用)→ 必须检出
    {
        use fs3_alloc::Staged;
        let mut draft = Staged::default();
        let ids = e.allocator().allocate(&mut draft, 1).unwrap();
        e.meta()
            .commit(&[Op::Alloc {
                draft: fs3_meta::AllocDraft {
                    alloc: draft.alloc.clone(),
                    ref_inc: vec![],
                    ref_dec: vec![],
                },
            }])
            .unwrap();
        let r2 = e.check_report().unwrap();
        assert!(
            r2.leaks.contains(&ids[0]),
            "注入的游离段 {} 必须被 check 检出: {:?}",
            ids[0],
            r2.leaks
        );
    }
    e.abort();
}

// ─────────────────── D1a 跨状态转换(ADR-11 D1a;V3-0) ───────────────────

#[test]
fn d1a_off_to_enabled_legacy_shadowing() {
    // Off→Enabled 遗留键遮蔽回归:遗留未版本化单键与新真实版本共存时,
    // 当前版本 = mtime 最大者(新写入);?versionId=null 寻址遗留单键;
    // 真实版本删除后遗留回升;?versionId=null 删除物理移除遗留单键。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let d0 = rnd(100_000, 30);
    e.put("b1", "k", &mut Cursor::new(d0.clone())).unwrap(); // Off 时代遗留
    assert_eq!(stats_of(&e), (1, 100_000));
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(100_000, 31);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    // 新真实版本(mtime ≥ 遗留)遮蔽遗留单键
    assert_eq!(read_all(&e, "b1", "k"), d1, "Off→Enabled:新版本遮蔽遗留键");
    // ?versionId=null 寻址遗留单键(VK_NULL 通道)
    assert_eq!(read_version(&e, "b1", "k", &VK_NULL), d0);
    assert_eq!(read_version(&e, "b1", "k", &v1), d1);
    assert_eq!(stats_of(&e), (2, 200_000));
    // 删除真实版本 → 遗留单键回升为当前(AWS null 版本语义)
    assert!(e.delete_version("b1", "k", Some(v1)).unwrap().is_some());
    assert_eq!(read_all(&e, "b1", "k"), d0);
    // ?versionId=null 删除 → 物理删遗留单键;对象消失
    assert!(e
        .delete_version("b1", "k", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert!(matches!(
        e.head_version("b1", "k", None),
        Err(Error::NotFound(_))
    ));
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn d1a_suspended_overwrites_legacy_inplace() {
    // D1a-1:Suspended 桶写/删除标记原地覆盖遗留单键(对外 VersionId 恒
    // "null"),不写 null 槽;遗留单键与 null 槽不共存。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let d0 = rnd(100_000, 32);
    e.put("b1", "k", &mut Cursor::new(d0.clone())).unwrap(); // Off 时代遗留
    set_versioning(&e, VersioningState::Suspended);
    // Suspended 写:原地覆盖遗留单键(不落版本键)
    let d1 = rnd(60_000, 33);
    let m1 = e.put("b1", "k", &mut Cursor::new(d1.clone())).unwrap();
    assert_eq!(m1.version_id, None);
    assert_eq!(read_all(&e, "b1", "k"), d1);
    assert!(
        e.meta().list_key_versions("b1", "k").unwrap().is_empty(),
        "遗留单键原地覆盖:不产生 null 槽/版本键"
    );
    assert_eq!(stats_of(&e), (1, 60_000), "覆盖遗留 = 先扣后加");
    // Suspended DELETE:标记原地覆盖遗留单键
    let dm = e.delete("b1", "k").unwrap().unwrap();
    assert!(dm.is_delete_marker && dm.version_id.is_none());
    assert!(
        e.meta()
            .get_object("b1", "k")
            .unwrap()
            .unwrap()
            .is_delete_marker
    );
    assert!(e.meta().list_key_versions("b1", "k").unwrap().is_empty());
    assert_eq!(stats_of(&e), (0, 0));
    assert!(matches!(
        e.head_version("b1", "k", None),
        Err(Error::DeleteMarker(ref m)) if m == "null"
    ));
    // 标记上再写 = 覆盖标记(未入账 → +1)
    let d2 = rnd(1_000, 34);
    e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap();
    assert_eq!(read_all(&e, "b1", "k"), d2);
    assert_eq!(stats_of(&e), (1, 1_000));
    // ?versionId=null 删除 = 物理删遗留单键
    assert!(e
        .delete_version("b1", "k", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn d1a_suspended_to_enabled_null_slot_shadowing() {
    // Suspended→Enabled null 槽遮蔽回归(D1a-2):重启用后的新真实版本
    // (即使与 null 槽同秒)遮蔽 null 槽;null 槽仍可 ?versionId=null 寻址。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(50_000, 35);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    set_versioning(&e, VersioningState::Suspended);
    let d2 = rnd(40_000, 36);
    e.put("b1", "k", &mut Cursor::new(d2.clone())).unwrap(); // null 槽
                                                             // 确定性时序:null 槽 mtime 拨晚 → null 槽为当前
    let m1 = e.head_version("b1", "k", Some(&v1)).unwrap().mtime;
    set_entry_mtime(&e, "b1", "k", &VK_NULL, m1 + 10);
    assert_eq!(read_all(&e, "b1", "k"), d2);
    // 重启用:null 槽 mtime 拨回同秒(重启用后的写必然后于挂起期写,
    // 同秒 tie 取真实版本);新真实版本遮蔽 null 槽(vk 防回拨不纳入
    // null 槽,D1a-5)
    set_entry_mtime(&e, "b1", "k", &VK_NULL, m1);
    set_versioning(&e, VersioningState::Enabled);
    let d3 = rnd(70_000, 37);
    let v3 = e
        .put("b1", "k", &mut Cursor::new(d3.clone()))
        .unwrap()
        .version_id
        .unwrap();
    assert!(v3 > v1, "null 槽不参与防回拨基址");
    assert_eq!(
        read_all(&e, "b1", "k"),
        d3,
        "重启用后的写遮蔽 null 槽(tie 取真实版本)"
    );
    // null 槽仍按 mtime 插入列表序列且可寻址
    assert_eq!(read_version(&e, "b1", "k", &VK_NULL), d2);
    let vers = e.meta().list_key_versions("b1", "k").unwrap();
    assert_eq!(vers.len(), 3, "v1 + null 槽 + v3");
    // 清场:三个条目逐一物理删除
    e.delete_version("b1", "k", Some(v1)).unwrap();
    e.delete_version("b1", "k", Some(v3)).unwrap();
    e.delete_version("b1", "k", Some(VK_NULL)).unwrap();
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

// ─────────────────── V4-1 Suspended null 槽边界矩阵(ADR-11 D1a;统计对账) ───────────────────

#[test]
fn suspended_null_slot_overwrite_stats_boundary() {
    // Suspended 连续 PUT:null 槽原地覆盖,统计先扣旧再加新(extent→extent
    // →inline→extent 全形态);版本条目恒 1;位图无泄漏。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Suspended);
    for (size, seed) in [
        (100_000usize, 50u8),
        (60_000, 51),
        (1_000, 52),
        (200_000, 53),
    ] {
        let d = rnd(size, seed);
        e.put("b1", "k", &mut Cursor::new(d.clone())).unwrap();
        assert_eq!(read_all(&e, "b1", "k"), d);
        assert_eq!(
            stats_of(&e),
            (1, size as u64),
            "覆盖 null 槽 = 先扣后加(size={size})"
        );
        assert_eq!(
            e.meta().list_key_versions("b1", "k").unwrap().len(),
            1,
            "null 槽原地覆盖:条目数不变(size={size})"
        );
    }
    // 清场:物理删 null 槽 → 统计归零、位图归零
    assert!(e
        .delete_version("b1", "k", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn suspended_repeated_delete_marker_idempotent() {
    // Suspended DELETE 幂等:重复删除 = 再插标记覆盖 null 槽旧标记,统计
    // 零漂移、条目恒 1;不存在键 DELETE 同样落 null 槽标记(AWS 204 语义)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Suspended);
    let d = rnd(80_000, 54);
    e.put("b1", "k", &mut Cursor::new(d)).unwrap();
    assert_eq!(stats_of(&e), (1, 80_000));
    for round in 0..2 {
        let dm = e.delete("b1", "k").unwrap().unwrap();
        assert!(dm.is_delete_marker && dm.version_id.is_none());
        assert_eq!(stats_of(&e), (0, 0), "第 {round} 次删除后统计归零");
        assert_eq!(
            e.meta().list_key_versions("b1", "k").unwrap().len(),
            1,
            "重复删除仍只一条 null 槽标记"
        );
        assert!(matches!(
            e.head_version("b1", "k", None),
            Err(Error::DeleteMarker(ref m)) if m == "null"
        ));
    }
    // 不存在键 DELETE → 落 null 槽标记;统计零 delta
    assert!(e.delete("b1", "ghost").unwrap().unwrap().is_delete_marker);
    assert_eq!(stats_of(&e), (0, 0));
    assert!(matches!(
        e.head_version("b1", "ghost", None),
        Err(Error::DeleteMarker(ref m)) if m == "null"
    ));
    // 清场:两条标记版本删除零 delta
    assert!(e
        .delete_version("b1", "k", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert!(e
        .delete_version("b1", "ghost", Some(VK_NULL))
        .unwrap()
        .is_some());
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn enabled_suspended_enabled_cycle_stats_reconcile() {
    // Enabled→Suspended→Enabled 全循环统计对账:每步 objects/bytes 精确
    // 断言,全程零漂移;清场后归零、位图归零。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    // Enabled:A(100_000)→ v1,B(50_000)→ v2(纯追加)
    let v1 = e
        .put("b1", "k", &mut Cursor::new(rnd(100_000, 55)))
        .unwrap()
        .version_id
        .unwrap();
    assert_eq!(stats_of(&e), (1, 100_000));
    let v2 = e
        .put("b1", "k", &mut Cursor::new(rnd(50_000, 56)))
        .unwrap()
        .version_id
        .unwrap();
    assert_eq!(stats_of(&e), (2, 150_000));
    // Suspended:C(40_000)→ null 槽;D(20_000)覆盖 null(先扣后加)
    set_versioning(&e, VersioningState::Suspended);
    e.put("b1", "k", &mut Cursor::new(rnd(40_000, 57))).unwrap();
    assert_eq!(stats_of(&e), (3, 190_000));
    e.put("b1", "k", &mut Cursor::new(rnd(20_000, 58))).unwrap();
    assert_eq!(stats_of(&e), (3, 170_000));
    // DELETE → null 标记覆盖 null 数据(扣 D);再写 E(10_000)覆盖标记(未入账 → +1)
    assert!(e.delete("b1", "k").unwrap().unwrap().is_delete_marker);
    assert_eq!(stats_of(&e), (2, 150_000));
    e.put("b1", "k", &mut Cursor::new(rnd(10_000, 59))).unwrap();
    assert_eq!(stats_of(&e), (3, 160_000));
    // 重启用:F(5_000)→ v3(纯追加);当前 = v3(同秒 tie 取真实版本)
    set_versioning(&e, VersioningState::Enabled);
    let d_f = rnd(5_000, 60);
    let v3 = e
        .put("b1", "k", &mut Cursor::new(d_f.clone()))
        .unwrap()
        .version_id
        .unwrap();
    assert!(v3 > v2);
    assert_eq!(stats_of(&e), (4, 165_000));
    assert_eq!(read_all(&e, "b1", "k"), d_f);
    assert_eq!(e.meta().list_key_versions("b1", "k").unwrap().len(), 4);
    // 清场:逐版本物理删除,逐步扣减对账至零
    e.delete_version("b1", "k", Some(v1)).unwrap();
    assert_eq!(stats_of(&e), (3, 65_000));
    e.delete_version("b1", "k", Some(v2)).unwrap();
    assert_eq!(stats_of(&e), (2, 15_000));
    e.delete_version("b1", "k", Some(VK_NULL)).unwrap(); // null 槽数据 E
    assert_eq!(stats_of(&e), (1, 5_000));
    e.delete_version("b1", "k", Some(v3)).unwrap();
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn suspended_null_marker_delete_version_fallback() {
    // null 槽为删除标记时,?versionId=null 删除 = 物理删标记(零 delta),
    // 当前版本回退到 Enabled 时代真实版本(AWS null 版本语义)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    set_versioning(&e, VersioningState::Enabled);
    let d1 = rnd(30_000, 61);
    let v1 = e
        .put("b1", "k", &mut Cursor::new(d1.clone()))
        .unwrap()
        .version_id
        .unwrap();
    let m1 = e.head_version("b1", "k", Some(&v1)).unwrap().mtime;
    set_versioning(&e, VersioningState::Suspended);
    e.put("b1", "k", &mut Cursor::new(rnd(20_000, 62))).unwrap(); // null 槽
    assert!(e.delete("b1", "k").unwrap().unwrap().is_delete_marker);
    assert_eq!(stats_of(&e), (1, 30_000), "标记覆盖 null 数据:扣 20_000");
    // 确定性时序:null 标记 mtime 拨晚 → 标记为当前
    set_entry_mtime(&e, "b1", "k", &VK_NULL, m1 + 10);
    assert!(matches!(
        e.head_version("b1", "k", None),
        Err(Error::DeleteMarker(ref m)) if m == "null"
    ));
    // ?versionId=null 删除标记 → 零 delta;当前回退到 v1
    let removed = e.delete_version("b1", "k", Some(VK_NULL)).unwrap().unwrap();
    assert!(removed.is_delete_marker);
    assert_eq!(stats_of(&e), (1, 30_000), "删标记零 delta");
    assert_eq!(read_all(&e, "b1", "k"), d1, "回退到 Enabled 时代版本");
    assert_eq!(e.meta().list_key_versions("b1", "k").unwrap().len(), 1);
    // 清场
    e.delete_version("b1", "k", Some(v1)).unwrap();
    assert_eq!(stats_of(&e), (0, 0));
    assert_eq!(e.allocator().allocated_count(), 0);
    e.close().unwrap();
}

#[test]
fn conditional_put_preconditions() {
    // ADR-11 D6:PUT 条件写(引擎写锁内对当前版本判定;Off/Enabled 同语义)。
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let d1 = rnd(1_000, 40);
    let m1 = e.put("b1", "k", &mut Cursor::new(d1.clone())).unwrap();
    let etag1 = m1.etag_full();
    let none_match_star = || WritePrecondition {
        if_none_match: Some(vec!["*".to_string()]),
        ..Default::default()
    };
    // If-None-Match: * 于已存在对象 → PreconditionFailed
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(rnd(1_000, 41)),
            None,
            vec![],
            vec![],
            vec![],
            Some(&none_match_star()),
            None,
            None,
        ),
        Err(Error::PreconditionFailed(_))
    ));
    // If-None-Match: * 于不存在键 → 放行
    e.put_with_meta(
        "b1",
        "new",
        &mut Cursor::new(rnd(1_000, 42)),
        None,
        vec![],
        vec![],
        vec![],
        Some(&none_match_star()),
        None,
        None,
    )
    .unwrap();
    // If-Match 命中当前 ETag → 放行;不匹配 → 412
    let if_match_hit = || WritePrecondition {
        if_match: Some(vec![etag1.clone()]),
        ..Default::default()
    };
    e.put_with_meta(
        "b1",
        "k",
        &mut Cursor::new(d1.clone()),
        None,
        vec![],
        vec![],
        vec![],
        Some(&if_match_hit()),
        None,
        None,
    )
    .unwrap();
    let if_match_miss = || WritePrecondition {
        if_match: Some(vec!["deadbeef".to_string()]),
        ..Default::default()
    };
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(d1.clone()),
            None,
            vec![],
            vec![],
            vec![],
            Some(&if_match_miss()),
            None,
            None,
        ),
        Err(Error::PreconditionFailed(_))
    ));
    // If-Match 于不存在键 → NotFound(协议层 404 NoSuchKey,s3-tests 口径)
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "ghost",
            &mut Cursor::new(d1.clone()),
            None,
            vec![],
            vec![],
            vec![],
            Some(&if_match_miss()),
            None,
            None,
        ),
        Err(Error::NotFound(_))
    ));
    // size/mtime 组合:size 不符 → 412;mtime 覆盖当前 → 放行
    let cur = e.head("b1", "k").unwrap().unwrap();
    let bad_size = WritePrecondition {
        if_match: Some(vec!["*".to_string()]),
        if_match_size: Some(cur.size + 1),
        ..Default::default()
    };
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(d1.clone()),
            None,
            vec![],
            vec![],
            vec![],
            Some(&bad_size),
            None,
            None,
        ),
        Err(Error::PreconditionFailed(_))
    ));
    let good_combo = WritePrecondition {
        if_match: Some(vec!["*".to_string()]),
        if_match_size: Some(cur.size),
        if_match_mtime: Some(cur.mtime),
        ..Default::default()
    };
    e.put_with_meta(
        "b1",
        "k",
        &mut Cursor::new(d1.clone()),
        None,
        vec![],
        vec![],
        vec![],
        Some(&good_combo),
        None,
        None,
    )
    .unwrap();
    // Enabled 桶:判定对 D1a 当前版本执行(与 Off 同语义)
    set_versioning(&e, VersioningState::Enabled);
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(d1.clone()),
            None,
            vec![],
            vec![],
            vec![],
            Some(&none_match_star()),
            None,
            None,
        ),
        Err(Error::PreconditionFailed(_))
    ));
    // 当前 = 删除标记 = 不存在:If-None-Match:* 放行,If-Match:* → NotFound
    e.delete("b1", "k").unwrap();
    e.put_with_meta(
        "b1",
        "k",
        &mut Cursor::new(d1.clone()),
        None,
        vec![],
        vec![],
        vec![],
        Some(&none_match_star()),
        None,
        None,
    )
    .unwrap();
    e.delete("b1", "k").unwrap();
    let if_match_star = WritePrecondition {
        if_match: Some(vec!["*".to_string()]),
        ..Default::default()
    };
    // 当前为删除标记:If-Match:* 按不存在处理(NotFound → 协议层 404)
    assert!(matches!(
        e.put_with_meta(
            "b1",
            "k",
            &mut Cursor::new(d1.clone()),
            None,
            vec![],
            vec![],
            vec![],
            Some(&if_match_star),
            None,
            None,
        ),
        Err(Error::NotFound(_))
    ));
    e.close().unwrap();
}

#[test]
fn write_precondition_delete_semantics() {
    // check_delete 纯单元语义:目标不存在 → 放行(幂等 204);存在(含
    // 删除标记)→ 逐条判定。
    let mut meta = object_meta_for_precond(100, 7000);
    let p = WritePrecondition {
        if_match: Some(vec!["*".to_string()]),
        ..Default::default()
    };
    assert!(p.check_delete(None).is_ok(), "不存在 → 幂等放行");
    assert!(p.check_delete(Some(&meta)).is_ok());
    let bad = WritePrecondition {
        if_match: Some(vec!["nomatch".to_string()]),
        ..Default::default()
    };
    assert!(matches!(
        bad.check_delete(Some(&meta)),
        Err(Error::PreconditionFailed(_))
    ));
    // 删除标记是合法判定目标(V2 语义:标记可删)
    meta.is_delete_marker = true;
    meta.etag = [0u8; 16];
    assert!(p.check_delete(Some(&meta)).is_ok());
    assert!(matches!(
        bad.check_delete(Some(&meta)),
        Err(Error::PreconditionFailed(_))
    ));
    // mtime/size 判定
    let mt = WritePrecondition {
        if_match_mtime: Some(6999),
        ..Default::default()
    };
    meta.is_delete_marker = false;
    meta.mtime = 7000;
    assert!(matches!(
        mt.check_delete(Some(&meta)),
        Err(Error::PreconditionFailed(_))
    ));
    let sz = WritePrecondition {
        if_match_size: Some(100),
        ..Default::default()
    };
    assert!(sz.check_delete(Some(&meta)).is_ok());
}

/// 条件写单元测试用对象(meta 层 object_meta 不可见,本地构造)。
fn object_meta_for_precond(size: u64, mtime: i64) -> ObjectMeta {
    ObjectMeta {
        size,
        etag: [9u8; 16],
        mtime,
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

// ─────────────────────────── M11 E1-7/E1-3:SSE-C 引擎链路 ───────────────────────────

/// SSE-C 测试密钥(固定字节,测试确定性)。
fn sse_test_key() -> fs3_core::SseCKey {
    fs3_core::SseCKey::from_bytes(&[0x5Au8; 32]).unwrap()
}

/// 独立重放写路径网格:按 meta.sse 的 nonce_base 对明文逐 chunk 加密,
/// 得期望密文(ETag=密文 MD5 / 内联密文等断言用)。
fn sse_reencrypt(plain: &[u8], sse: &fs3_core::SseInfo) -> Vec<u8> {
    let key = sse_test_key();
    let mut cipher = fs3_core::ChunkedGcm::new(key.data_key(), sse.nonce_base);
    let mut ct = Vec::with_capacity(plain.len());
    for (no, c) in plain.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate() {
        let (c, tag) = cipher.encrypt_chunk(no as u64, c);
        assert_eq!(tag, sse.chunk_tags[no], "chunk {no} tag 与落盘一致");
        ct.extend_from_slice(&c);
    }
    ct
}

/// M11 E1-7/E1-3:内联 + extent 两臂加密写读往返;ETag = 密文 MD5(DE2);
/// chunk_tags 网格 = ceil(size/64KiB)(尾部 partial 也有 tag)。
#[test]
fn sse_c_put_get_roundtrip() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();

    // —— 内联臂(≤32KiB,恒单 chunk)——
    let small = rnd(1_000, 3);
    let m = e
        .put_with_meta(
            "b1",
            "sse-small",
            &mut Cursor::new(small.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .unwrap();
    let sse = m.sse.as_ref().expect("sse meta");
    assert_eq!(sse.kind, fs3_core::SseKind::SseC);
    assert_eq!(sse.kek_id, 0, "SSE-C 不用 KEK(约定 0)");
    assert!(sse.wrapped_dek.is_empty(), "SSE-C 不落 DEK(约定空)");
    assert_eq!(sse.key_md5, key.key_md5(), "D-E5:校验子 = 客户密钥 MD5");
    assert_eq!(sse.chunk_tags.len(), 1, "内联恒单 chunk(D-E1 口径)");
    let ct = m.inline.as_ref().expect("inline ciphertext");
    assert_ne!(*ct, small, "落盘为密文");
    assert_eq!(ct.len(), small.len(), "密文等长(DE1)");
    assert_eq!(ct, &sse_reencrypt(&small, sse), "写路径网格 = ssec 网格");
    // ETag = 密文 MD5(DE2)
    let expect_etag: [u8; 16] = md5::Md5::digest(ct).into();
    assert_eq!(m.etag, expect_etag, "ETag = 密文 MD5");
    assert_ne!(m.etag, md5::Md5::digest(&small).as_slice(), "非明文 MD5");
    // 持久化往返(读回 meta 一致)
    let got = e.head("b1", "sse-small").unwrap().unwrap();
    assert_eq!(got.sse, m.sse);
    // 带密钥读回 = 明文
    let mut buf = vec![0u8; small.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-small",
            None,
            0,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(&buf[..n], &small[..]);

    // —— extent 臂(200_000B = 3×64KiB 满块 + 3_392B 尾块)——
    let big = rnd(200_000, 9);
    let chunks = big.len().div_ceil(fs3_core::SSE_CHUNK_SIZE);
    assert_eq!(chunks, 4);
    let m = e
        .put_with_meta(
            "b1",
            "sse-big",
            &mut Cursor::new(big.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .unwrap();
    assert!(m.inline.is_none() && !m.extents.is_empty());
    let sse = m.sse.as_ref().expect("sse meta");
    assert_eq!(sse.chunk_tags.len(), chunks, "尾部 partial 也有 tag");
    let ct = sse_reencrypt(&big, sse);
    let expect_etag: [u8; 16] = md5::Md5::digest(&ct).into();
    assert_eq!(m.etag, expect_etag, "extent 臂 ETag = 密文 MD5");
    // 整体读回(get_to_meta SSE 臂)与流式读回(read_at_meta)一致
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, big, "get_to_meta SSE 臂解密往返");
    let mut buf = vec![0u8; big.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-big",
            None,
            0,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(&buf[..n], &big[..], "read_at SSE 臂解密往返");
    // 解密字节指标(全对象两遍:get_to + read_at)
    assert_eq!(
        e.sse_decrypt_bytes(),
        (small.len() + 2 * big.len()) as u64,
        "按字节计解密量(DE1 指标)"
    );
    e.close().unwrap();
}

/// M11 E1-3:Range/偏移读只解密命中 chunk(首尾 partial 网格裁剪);
/// 跨 chunk 边界窗口逐字节一致。
#[test]
fn sse_c_range_read_chunk_aligned() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let big = rnd(200_000, 11);
    e.put_with_meta(
        "b1",
        "sse-r",
        &mut Cursor::new(big.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    let grid = fs3_core::SSE_CHUNK_SIZE as u64;
    let before = e.sse_decrypt_bytes();
    // 命中单个 chunk 中部:[70_000, 70_100)——只解密 chunk 1(64KiB)
    let mut buf = vec![0u8; 100];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-r",
            None,
            70_000,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(n, 100);
    assert_eq!(&buf[..], &big[70_000..70_100]);
    assert_eq!(
        e.sse_decrypt_bytes() - before,
        grid,
        "单 chunk 命中只解密一个 chunk"
    );
    // 跨 chunk 边界、首尾 partial:[60_000, 140_000)跨 chunk 0/1/2
    let mut buf = vec![0u8; 80_000];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-r",
            None,
            60_000,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(n, 80_000);
    assert_eq!(&buf[..], &big[60_000..140_000]);
    // 尾 chunk partial:[199_000, 200_000)
    let mut buf = vec![0u8; 1_000];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-r",
            None,
            199_000,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(&buf[..n], &big[199_000..]);
    e.close().unwrap();
}

/// M11 E1-3:错密钥 / 篡改 tag / 篡改密文 → 解密验 tag 失败(Corrupt,
/// 数据不可读语义);无密钥读 SSE 对象 → 显式 InvalidRequest(不返回密文)。
#[test]
fn sse_c_read_failure_modes() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let data = rnd(100_000, 13);
    e.put_with_meta(
        "b1",
        "sse-f",
        &mut Cursor::new(data.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();

    // 无密钥:read_at / get_to 显式报错(内部调用方拿不到密文)
    let mut buf = vec![0u8; 4096];
    let err = e.read_at("b1", "sse-f", 0, &mut buf).unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    let mut out = Vec::new();
    let err = e.get_to("b1", "sse-f", 0..u64::MAX, &mut out).unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");

    // 错密钥:GCM 认证失败 → Corrupt(不泄漏密钥信息)
    let wrong = fs3_core::SseCKey::from_bytes(&[0xA5u8; 32]).unwrap();
    let err = e
        .read_at_version_for(
            "b1",
            "sse-f",
            None,
            0,
            &mut buf,
            VersioningState::Off,
            Some(&wrong),
        )
        .unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    let msg = err.to_string();
    assert!(!msg.contains("5a5a"), "错误消息不含密钥材料: {msg}");

    // 篡改 chunk_tags[0](经 meta 直接改值)→ 读 chunk 0 失败,chunk 1 仍可读
    let raw = fs3_meta::keys::object_key("b1", "sse-f");
    let mut m = e.head("b1", "sse-f").unwrap().unwrap();
    m.sse.as_mut().unwrap().chunk_tags[0][0] ^= 0x80;
    e.meta().commit_object_meta_update(&raw, &m).unwrap();
    let err = e
        .read_at_version_for(
            "b1",
            "sse-f",
            None,
            0,
            &mut buf,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::Corrupt(_)),
        "篡改 tag → Corrupt: {err}"
    );
    let mut buf2 = vec![0u8; 100];
    let n = e
        .read_at_version_for(
            "b1",
            "sse-f",
            None,
            fs3_core::SSE_CHUNK_SIZE as u64,
            &mut buf2,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap();
    assert_eq!(
        &buf2[..n],
        &data[fs3_core::SSE_CHUNK_SIZE..fs3_core::SSE_CHUNK_SIZE + 100],
        "未篡改 chunk 不受影响"
    );
    e.close().unwrap();
}

/// M11 E1-3(DE1):SSE 对象禁零拷贝(object_segments_meta → None);
/// 空对象 SSE-C(零 chunk)往返;ETag=fast(crc32c)组合 ETag 落密文 CRC。
#[test]
fn sse_c_zero_copy_disabled_and_edges() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    // extent 对象(零拷贝候选形态)加密后禁零拷贝
    let big = rnd(100_000, 17);
    e.put_with_meta(
        "b1",
        "sse-zc",
        &mut Cursor::new(big),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    assert!(e
        .object_segments_version_for("b1", "sse-zc", None, 0, 1, VersioningState::Off)
        .unwrap()
        .is_none());
    // 同对象未加密重写后恢复零拷贝(对照)
    e.put("b1", "plain-zc", &mut Cursor::new(rnd(100_000, 19)))
        .unwrap();
    assert!(e
        .object_segments_version_for("b1", "plain-zc", None, 0, 1, VersioningState::Off)
        .unwrap()
        .is_some());

    // 空对象 + SSE-C:零 chunk、零 tag,往返一致
    let m = e
        .put_with_meta(
            "b1",
            "sse-empty",
            &mut Cursor::new(Vec::new()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .unwrap();
    assert_eq!(m.size, 0);
    assert_eq!(m.sse.as_ref().unwrap().chunk_tags.len(), 0);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert!(out.is_empty());
    e.close().unwrap();
}

/// M11 E1-7:EtagMode::Crc32c(etag=fast)组合下 SSE 对象 ETag = 密文
/// CRC32C(DE2 组合口径);checksum tee 在明文侧(与加密顺序:明文
/// checksum → 加密)。
#[test]
fn sse_c_etag_fast_and_plaintext_checksum() {
    let (_d, cfg) = setup();
    let mut cfg2 = cfg.clone();
    cfg2.etag_mode = fs3_core::EtagMode::Crc32c;
    let mut e = open_engine(&cfg2);
    let key = sse_test_key();
    let data = rnd(150_000, 23);
    let m = e
        .put_with_meta(
            "b1",
            "sse-fast",
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            Some(ChecksumAlgorithm::Sha256),
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .unwrap();
    let sse = m.sse.as_ref().unwrap();
    // ETag 低 4 字节 = 密文 CRC32C(etag=fast 口径)
    let ct = sse_reencrypt(&data, sse);
    let expect_crc = crc32c(&ct, 0).to_be_bytes();
    assert_eq!(&m.etag[12..16], &expect_crc, "ETag = 密文 CRC32C(DE2 组合)");
    // checksum 为明文语义(ADR-12 checksum 决策:先于加密)
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: ChecksumAlgorithm::Sha256,
            value: fs3_core::checksum_one_shot(ChecksumAlgorithm::Sha256, &data),
        }),
        "checksum 落明文值"
    );
    // 读回明文一致
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, data);
    e.close().unwrap();
}

// ─────────────────────────── M11 E1-4:multipart SSE-C ───────────────────────────

/// 第二把测试密钥(异密钥重加密路径用)。
fn sse_test_key_b() -> fs3_core::SseCKey {
    fs3_core::SseCKey::from_bytes(&[0xA5u8; 32]).unwrap()
}

/// 创建 SSE-C multipart 会话(会话只落 key-MD5,引擎不校验其值——逐值
/// 比对在协议层;此处给真实 base64(md5(key)) 保持形态真实)。
fn create_sse_upload(e: &mut Engine, key: &str) -> String {
    let md5_b64 = "BuASUiSbvhMWBKAmsiYRhg=="; // base64(md5([0x5A;32]))
    e.create_multipart(
        "b1",
        key,
        None,
        vec![],
        vec![],
        vec![],
        None,
        Some(md5_b64.into()),
        None,
    )
    .unwrap()
}

/// M11 E1-4(DE2 + D-E4/D-E6 裁决):multipart SSE-C 全流程——每 part 独立
/// 加密(D-E6 确定性派生 nonce_base,重传幂等;part ETag = 密文 MD5,
/// PartMeta.sse 落 nonce/tags/key_md5;extent + 内联两臂),Complete 解密
/// 重加密为单一 nonce_base 对象网格,复合 ETag 维持 md5-N,GET 带密钥往返。
#[test]
fn sse_c_multipart_full_flow() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();

    // part1 = 5MiB(extent 臂,非末片 ≥ 5MiB 门槛);part2 = 100B(内联臂)
    let p1_data = rnd(5 * 1024 * 1024, 31);
    let p2_data = rnd(100, 37);
    let uid = create_sse_upload(&mut e, "mp-sse");
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(p1_data.clone()), None, Some(&key))
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(p2_data.clone()), None, Some(&key))
        .unwrap();
    // 每 part 独立加密:PartMeta.sse 落 nonce_base + chunk_tags;两 part
    // nonce_base 不同(D-E6 派生按 part_number 区分);part ETag = 密文
    // MD5(≠ 明文 MD5)
    let s1 = p1.sse.as_ref().expect("part1 sse");
    let s2 = p2.sse.as_ref().expect("part2 sse");
    assert_ne!(s1.nonce_base, s2.nonce_base, "每 part 独立 nonce_base(DE2)");
    // D-E6:nonce_base = HMAC-SHA256(data_key, "fasts3-sse-c-part" ‖
    // upload_id ‖ be32(part_number)) 前 12B(确定性派生,与 ssec 公式逐值一致)
    let dk = key.data_key();
    assert_eq!(
        s1.nonce_base,
        fs3_core::derive_part_nonce_base(&dk, &uid, 1)
    );
    assert_eq!(
        s2.nonce_base,
        fs3_core::derive_part_nonce_base(&dk, &uid, 2)
    );
    // D-E6:同 part 同内容重传 ⇒ 同 nonce 同密文 ⇒ ETag 稳定(重传幂等;
    // part1 = extent 臂,part2 = 内联臂,两臂同口径)
    let p1r = e
        .upload_part(&uid, 1, &mut Cursor::new(p1_data.clone()), None, Some(&key))
        .unwrap();
    let p2r = e
        .upload_part(&uid, 2, &mut Cursor::new(p2_data.clone()), None, Some(&key))
        .unwrap();
    assert_eq!(p1r.etag, p1.etag, "extent 臂重传 ETag 稳定");
    assert_eq!(p2r.etag, p2.etag, "内联臂重传 ETag 稳定");
    assert_eq!(
        p1r.sse.as_ref().unwrap().nonce_base,
        s1.nonce_base,
        "重传复用同一派生 nonce_base"
    );
    assert_eq!(
        s1.chunk_tags.len(),
        p1_data.len().div_ceil(fs3_core::SSE_CHUNK_SIZE)
    );
    assert_eq!(s2.chunk_tags.len(), 1, "内联 part 恒单 chunk");
    assert_eq!(
        p1.etag,
        md5::Md5::digest(sse_reencrypt(&p1_data, s1)).as_slice()
    );
    assert_eq!(
        p2.etag,
        md5::Md5::digest(sse_reencrypt(&p2_data, s2)).as_slice()
    );
    assert_ne!(
        p1.etag,
        md5::Md5::digest(&p1_data).as_slice(),
        "ETag = 密文 MD5"
    );
    // D-E5:校验子落盘 = 客户密钥 MD5(part 级与下方对象级同口径)
    assert_eq!(s1.key_md5, key.key_md5());
    assert_eq!(s2.key_md5, key.key_md5());

    // Complete:重加密为单一对象网格(D-E4);复合 ETag = md5(各 part 密文
    // MD5 拼接)-N(N=2)
    let m = e
        .complete_multipart(
            "b1",
            "mp-sse",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            Some(&key),
        )
        .unwrap();
    let total = p1_data.len() + p2_data.len();
    assert_eq!(m.size, total as u64);
    let osse = m.sse.as_ref().expect("object sse");
    assert_eq!(osse.key_md5, key.key_md5(), "D-E5:对象级校验子同密钥 MD5");
    assert_eq!(
        osse.chunk_tags.len(),
        total.div_ceil(fs3_core::SSE_CHUNK_SIZE),
        "对象全局 64KiB 网格(单一 nonce_base)"
    );
    assert_ne!(osse.nonce_base, s1.nonce_base);
    assert_ne!(osse.nonce_base, s2.nonce_base);
    let mut concat = Vec::new();
    concat.extend_from_slice(&p1.etag);
    concat.extend_from_slice(&p2.etag);
    assert_eq!(
        m.etag,
        md5::Md5::digest(&concat).as_slice(),
        "复合 ETag = md5-N"
    );
    assert_eq!(m.parts, vec![p1_data.len() as u64, p2_data.len() as u64]);
    assert!(m.inline.is_none(), "5MiB+ 对象走 extent 臂");

    // GET 往返(带密钥 = 拼接明文;持久化重读同口径)
    let mut expect = p1_data.clone();
    expect.extend_from_slice(&p2_data);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, expect, "Complete 后 GET 解密往返");
    let m2 = e.head("b1", "mp-sse").unwrap().unwrap();
    assert_eq!(m2.sse, m.sse, "对象级 SseInfo 持久化一致");
    let mut out = Vec::new();
    e.get_to_meta(&m2, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, expect);
    // 无密钥读 → 显式 InvalidRequest(不返回密文);错密钥 → Corrupt
    let mut out = Vec::new();
    let err = e.get_to_meta(&m2, 0..u64::MAX, &mut out, None).unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    let mut out = Vec::new();
    let err = e
        .get_to_meta(&m2, 0..u64::MAX, &mut out, Some(&sse_test_key_b()))
        .unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    e.close().unwrap();
}

/// M11 E1-4:SSE-C 全内联 multipart(总大小 ≤ 内联阈值)→ Complete 内联
/// 臂整体加密(恒单 chunk);会话一致性错误矩阵(缺头/多头/Complete 缺
/// 密钥 → InvalidRequest)。
#[test]
fn sse_c_multipart_inline_and_consistency() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();

    // 全内联:单片 200B,Complete 走内联重加密臂
    let data = rnd(200, 41);
    let uid = create_sse_upload(&mut e, "mp-inl");
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(data.clone()), None, Some(&key))
        .unwrap();
    assert!(p1.inline.is_some() && p1.sse.is_some());
    let m = e
        .complete_multipart(
            "b1",
            "mp-inl",
            &uid,
            &[cp(1, p1.etag_hex())],
            None,
            Some(&key),
        )
        .unwrap();
    let osse = m.sse.as_ref().expect("object sse");
    assert_eq!(osse.chunk_tags.len(), 1, "内联对象恒单 chunk(D-E1 口径)");
    let ct = m.inline.as_ref().expect("inline ciphertext");
    assert_ne!(*ct, data, "落盘为密文");
    assert_eq!(*ct, sse_reencrypt(&data, osse), "对象网格 = ssec 网格");
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, data);

    // —— 会话一致性(引擎兜底;协议层另有 key-MD5 逐值比对)——
    // SSE 会话 UploadPart 缺密钥 → InvalidRequest
    let uid2 = create_sse_upload(&mut e, "mp-c1");
    let err = e
        .upload_part(&uid2, 1, &mut Cursor::new(rnd(10, 1)), None, None)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // SSE 会话 Complete 缺密钥 → InvalidRequest(重加密必需密钥本体)
    e.upload_part(&uid2, 1, &mut Cursor::new(rnd(10, 1)), None, Some(&key))
        .unwrap();
    let p = e.list_parts(&uid2).unwrap()[0].1.clone();
    let err = e
        .complete_multipart("b1", "mp-c1", &uid2, &[cp(1, p.etag_hex())], None, None)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // 明文会话 UploadPart 带密钥 → InvalidRequest(不静默加密)
    let uid3 = e
        .create_multipart(
            "b1",
            "mp-c2",
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let err = e
        .upload_part(&uid3, 1, &mut Cursor::new(rnd(10, 1)), None, Some(&key))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // 明文会话 Complete 带密钥 → InvalidRequest
    let p = e
        .upload_part(&uid3, 1, &mut Cursor::new(rnd(10, 1)), None, None)
        .unwrap();
    let err = e
        .complete_multipart(
            "b1",
            "mp-c2",
            &uid3,
            &[cp(1, p.etag_hex())],
            None,
            Some(&key),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    e.close().unwrap();
}

/// M11 E1-4:SSE-C 会话 + checksum 组合——分片 checksum 落明文值(上传期
/// tee 在加密前);Create 声明算法时 Complete 由服务端按明文代算对象级
/// 值(FullObject 臂经 read_part_plain_to 解密重算,不明文落密文)。
#[test]
fn sse_c_multipart_with_checksum() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let alg = ChecksumAlgorithm::Crc32c; // FullObject 默认类型
    let md5_b64 = "BuASUiSbvhMWBKAmsiYRhg==";
    let uid = e
        .create_multipart(
            "b1",
            "mp-ck",
            None,
            vec![],
            vec![],
            vec![],
            Some(alg),
            Some(md5_b64.into()),
            None,
        )
        .unwrap();
    let p1_data = rnd(5 * 1024 * 1024, 43);
    let p2_data = rnd(50_000, 47);
    let p1 = e
        .upload_part(
            &uid,
            1,
            &mut Cursor::new(p1_data.clone()),
            Some(alg),
            Some(&key),
        )
        .unwrap();
    let p2 = e
        .upload_part(
            &uid,
            2,
            &mut Cursor::new(p2_data.clone()),
            Some(alg),
            Some(&key),
        )
        .unwrap();
    // 分片 checksum = 明文值(加密在前则值不同)
    assert_eq!(
        p1.checksum.as_ref().unwrap().value,
        fs3_core::checksum_one_shot(alg, &p1_data),
        "分片 checksum 落明文值"
    );
    let m = e
        .complete_multipart(
            "b1",
            "mp-ck",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            Some(&key),
        )
        .unwrap();
    // 对象级 FullObject 值 = alg(拼接明文),非密文值
    let mut expect = p1_data.clone();
    expect.extend_from_slice(&p2_data);
    assert_eq!(
        m.checksum,
        Some(ChecksumInfo {
            algorithm: alg,
            value: fs3_core::checksum_one_shot(alg, &expect),
        }),
        "对象级 checksum 为明文语义(ADR-12 checksum 决策)"
    );
    assert_eq!(m.part_checksums.len(), 2, "逐分片 checksum 随对象持久化");
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, expect);
    e.close().unwrap();
}

// ─────────────────────────── M11 E1-5:copy 加密语义(DE3) ───────────────────────────

/// M11 E1-5(DE3):CopyObject 四象限——明文→加密(数据路径)/加密→同密钥
/// (COW 直灌)/加密→异密钥(解密重加密)/加密→未指定(InvalidRequest);
/// 缺 copy-source 密钥 → InvalidRequest;错源密钥 → Corrupt。内联/extent
/// 两形态各过一遍。
#[test]
fn sse_c_copy_matrix() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let ka = sse_test_key();
    let kb = sse_test_key_b();
    let small = rnd(1_000, 51); // 内联形态
    let big = rnd(200_000, 53); // extent 形态
    for (name, data) in [("small", &small), ("big", &big)] {
        e.put_with_meta(
            "b1",
            &format!("plain-{name}"),
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
        e.put_with_meta(
            "b1",
            &format!("enc-{name}"),
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&ka)),
        )
        .unwrap();
        // —— 象限 1:源未加密 + 目标 SSE-C → 数据路径加密写 ——
        let m = e
            .copy_object_version_for(
                "b1",
                &format!("plain-{name}"),
                None,
                "b1",
                &format!("q1-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                None,
                Some(&fs3_core::SseWriteKey::SseC(&ka)),
            )
            .unwrap();
        let sse = m.sse.as_ref().expect("q1 encrypted");
        assert_eq!(sse.kind, fs3_core::SseKind::SseC);
        assert_ne!(m.etag, md5::Md5::digest(data).as_slice(), "ETag = 密文 MD5");
        let mut out = Vec::new();
        e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&ka)).unwrap();
        assert_eq!(&out, data, "q1 带密钥读回 = 源明文");
        let mut out = Vec::new();
        let err = e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRequest(_)),
            "q1 无密钥显式拒绝: {err}"
        );

        // —— 象限 2:源 SSE-C + 同密钥 COW 直灌(SseInfo 原样继承)——
        let src = e.head("b1", &format!("enc-{name}")).unwrap().unwrap();
        let m = e
            .copy_object_version_for(
                "b1",
                &format!("enc-{name}"),
                None,
                "b1",
                &format!("q2-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                Some(&ka),
                Some(&fs3_core::SseWriteKey::SseC(&ka)),
            )
            .unwrap();
        assert_eq!(m.sse, src.sse, "同密钥 COW:SseInfo 原样继承(零数据搬运)");
        assert_eq!(m.etag, src.etag, "COW 不动密文,ETag 不变");
        let mut out = Vec::new();
        e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&ka)).unwrap();
        assert_eq!(&out, data, "q2 带密钥读回 = 源明文");

        // —— 象限 3:源 SSE-C + 异密钥 → 解密重加密(数据路径)——
        let m = e
            .copy_object_version_for(
                "b1",
                &format!("enc-{name}"),
                None,
                "b1",
                &format!("q3-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                Some(&ka),
                Some(&fs3_core::SseWriteKey::SseC(&kb)),
            )
            .unwrap();
        let sse3 = m.sse.as_ref().expect("q3 encrypted");
        assert_ne!(
            sse3.nonce_base,
            src.sse.as_ref().unwrap().nonce_base,
            "iter {name}"
        );
        let mut out = Vec::new();
        e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&kb)).unwrap();
        assert_eq!(&out, data, "q3 新密钥读回 = 源明文");
        let mut out = Vec::new();
        let err = e
            .get_to_meta(&m, 0..u64::MAX, &mut out, Some(&ka))
            .unwrap_err();
        assert!(matches!(err, Error::Corrupt(_)), "q3 旧密钥不再可读: {err}");

        // —— 象限 4:源 SSE-C + 目标未指定加密 → InvalidRequest(DE3)——
        let err = e
            .copy_object_version_for(
                "b1",
                &format!("enc-{name}"),
                None,
                "b1",
                &format!("q4-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                Some(&ka),
                None,
            )
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)), "{err}");

        // 源 SSE-C + 目标 SSE-C 但缺 copy-source 密钥 → InvalidRequest
        let err = e
            .copy_object_version_for(
                "b1",
                &format!("enc-{name}"),
                None,
                "b1",
                &format!("q5-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                None,
                Some(&fs3_core::SseWriteKey::SseC(&kb)),
            )
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)), "{err}");

        // 异密钥路径 + 错源密钥 → 解密验 tag 失败 Corrupt(不泄漏)
        let err = e
            .copy_object_version_for(
                "b1",
                &format!("enc-{name}"),
                None,
                "b1",
                &format!("q6-{name}"),
                None,
                None,
                None,
                None,
                VersioningState::Off,
                Some(&kb),
                Some(&fs3_core::SseWriteKey::SseC(&ka)),
            )
            .unwrap_err();
        assert!(matches!(err, Error::Corrupt(_)), "{err}");
    }
    e.close().unwrap();
}

/// M11 E1-5:UploadPartCopy 矩阵——明文源灌入 SSE 会话(加密 part)、SSE
/// 源同密钥灌入(解密→重加密)、SSE 源 + 明文会话 → InvalidRequest、SSE
/// 源缺 copy-source 密钥 → InvalidRequest、会话缺目标密钥 → InvalidRequest;
/// range 直灌与 Complete 后整对象读回。
#[test]
fn sse_c_upload_part_copy_matrix() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    // 源:5MiB+100B(extent;range 跨 64KiB 网格非对齐),SSE-C 加密与明文各一
    let src_data = rnd(5 * 1024 * 1024 + 200_000, 57);
    e.put_with_meta(
        "b1",
        "upc-enc",
        &mut Cursor::new(src_data.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    e.put_with_meta(
        "b1",
        "upc-plain",
        &mut Cursor::new(src_data.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        None,
    )
    .unwrap();

    // SSE 会话:明文源 + 目标密钥 → part 加密(非对齐 range)
    let uid = create_sse_upload(&mut e, "upc-dst");
    let range = 60_000..(5 * 1024 * 1024 + 200_000);
    let expect: Vec<u8> = src_data[range.start as usize..].to_vec();
    let p = e
        .upload_part_copy(&uid, 1, "b1", "upc-plain", range.clone(), None, Some(&key))
        .unwrap();
    let psse = p.sse.as_ref().expect("part encrypted");
    assert_eq!(p.size, expect.len() as u64);
    assert_eq!(
        p.etag,
        md5::Md5::digest(sse_reencrypt(&expect, psse)).as_slice(),
        "part ETag = 密文 MD5"
    );
    // SSE 源 + 同密钥 → 解密后重加密为 part 网格(part2 = 末片,无 5MiB 门槛)
    let p2 = e
        .upload_part_copy(&uid, 2, "b1", "upc-enc", 0..100, Some(&key), Some(&key))
        .unwrap();
    assert!(p2.sse.is_some());
    // Complete 后整对象读回 = 两段明文拼接
    let m = e
        .complete_multipart(
            "b1",
            "upc-dst",
            &uid,
            &[cp(1, p.etag_hex()), cp(2, p2.etag_hex())],
            None,
            Some(&key),
        )
        .unwrap();
    let mut expect_all = expect.clone();
    expect_all.extend_from_slice(&src_data[..100]);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(out, expect_all, "UploadPartCopy 混合源 Complete 后读回一致");

    // —— 错误路径 ——
    // SSE 源 + 明文会话(目标未加密)→ InvalidRequest(DE3 防静默解密落盘)
    let uid2 = e
        .create_multipart(
            "b1",
            "upc-e1",
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .unwrap();
    let err = e
        .upload_part_copy(&uid2, 1, "b1", "upc-enc", 0..100, Some(&key), None)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // SSE 源 + SSE 会话但缺 copy-source 密钥 → InvalidRequest
    let uid3 = create_sse_upload(&mut e, "upc-e2");
    let err = e
        .upload_part_copy(&uid3, 1, "b1", "upc-enc", 0..100, None, Some(&key))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // SSE 会话缺目标密钥 → InvalidRequest(会话一致性)
    let err = e
        .upload_part_copy(&uid3, 1, "b1", "upc-plain", 0..100, None, None)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    // 明文会话带目标密钥 → InvalidRequest(不静默加密)
    let err = e
        .upload_part_copy(&uid2, 1, "b1", "upc-plain", 0..100, None, Some(&key))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    e.close().unwrap();
}

// ─────────────────────────── M11 K1-1:SSE-S3 KEK/DEK 引擎链路 ───────────────────────────

/// SSE-S3 写密钥签发(引擎 mint;当前代包裹)并包装为写路径枚举借用。
fn s3_write_key(e: &Engine) -> fs3_core::SseS3WriteKey {
    e.sse_s3_mint_write_key().unwrap()
}

/// SSE-S3 会话创建(会话级 DEK 包裹值随会话落盘;DEK 明文零落盘)。
fn create_s3_upload(e: &mut Engine, key: &str) -> String {
    let wk = e.sse_s3_mint_write_key().unwrap();
    e.create_multipart(
        "b1",
        key,
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(fs3_meta::SessionSseS3 {
            kek_id: wk.kek_id(),
            wrapped_dek: wk.wrapped_dek().to_vec(),
        }),
    )
    .unwrap()
}

/// K1-1:内联 + extent 两臂 SSE-S3 写读往返;SseInfo 形态(kind/kek_id/
/// wrapped_dek 60B/key_md5 恒零);**读侧零客户头**(引擎自持解包);
/// wrapped_dek 损坏 → Corrupt(数据不可读)。
#[test]
fn sse_s3_put_get_roundtrip() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);

    // —— 内联臂(≤32KiB,恒单 chunk)——
    let small = rnd(1_000, 3);
    let wk = s3_write_key(&e);
    let wk_ref = fs3_core::SseWriteKey::SseS3(&wk);
    let m = e
        .put_with_meta(
            "b1",
            "s3-small",
            &mut Cursor::new(small.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&wk_ref),
        )
        .unwrap();
    let sse = m.sse.as_ref().expect("sse meta");
    assert_eq!(sse.kind, fs3_core::SseKind::SseS3);
    assert_eq!(sse.kek_id, 1, "初始代 = 1");
    assert_eq!(
        sse.wrapped_dek.len(),
        fs3_core::SSE_S3_WRAPPED_DEK_LEN,
        "nonce‖ct‖tag = 60B"
    );
    assert_eq!(sse.key_md5, [0u8; 16], "SSE-S3 校验子恒零(D-E5)");
    assert_eq!(sse.chunk_tags.len(), 1, "内联恒单 chunk");
    let ct = m.inline.as_ref().expect("inline ciphertext");
    assert_ne!(*ct, small, "落盘为密文");
    assert_eq!(ct.len(), small.len(), "密文等长");
    // 持久化往返
    let got = e.head("b1", "s3-small").unwrap().unwrap();
    assert_eq!(got.sse, m.sse);
    // 读侧**零密钥**(服务端自持解包):流式读回 = 明文
    let mut buf = vec![0u8; small.len()];
    let n = e
        .read_at_version_for(
            "b1",
            "s3-small",
            None,
            0,
            &mut buf,
            VersioningState::Off,
            None,
        )
        .unwrap();
    assert_eq!(&buf[..n], &small[..]);

    // —— extent 臂(200_000B = 3 满 chunk + 尾块)——
    let big = rnd(200_000, 9);
    let wk = s3_write_key(&e);
    let wk_ref = fs3_core::SseWriteKey::SseS3(&wk);
    let m = e
        .put_with_meta(
            "b1",
            "s3-big",
            &mut Cursor::new(big.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&wk_ref),
        )
        .unwrap();
    assert!(m.inline.is_none() && !m.extents.is_empty());
    assert_eq!(m.sse.as_ref().unwrap().chunk_tags.len(), 4);
    // 每对象随机 DEK:同明文两对象密文不同
    let wk2 = s3_write_key(&e);
    let wk2_ref = fs3_core::SseWriteKey::SseS3(&wk2);
    let m2 = e
        .put_with_meta(
            "b1",
            "s3-big2",
            &mut Cursor::new(big.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&wk2_ref),
        )
        .unwrap();
    assert_ne!(m.etag, m2.etag, "随机 DEK ⇒ 同明文异密文(DS1)");
    assert_ne!(
        m.sse.as_ref().unwrap().wrapped_dek,
        m2.sse.as_ref().unwrap().wrapped_dek
    );
    // 无密钥整体读回 + Range 读回
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(out, big, "get_to_meta SSE-S3 臂解密往返(零客户头)");
    let mut out = Vec::new();
    e.get_to_meta(&m, 60_000..140_000, &mut out, None).unwrap();
    assert_eq!(out, big[60_000..140_000], "Range 跨 chunk 解密一致");

    // wrapped_dek 损坏 → 读 = Corrupt(数据不可读,不静默)
    let mut bad = m.clone();
    let mut sse_bad = bad.sse.clone().unwrap();
    sse_bad.wrapped_dek[20] ^= 1;
    bad.sse = Some(sse_bad);
    let mut out = Vec::new();
    let err = e
        .get_to_meta(&bad, 0..u64::MAX, &mut out, None)
        .unwrap_err();
    assert!(
        matches!(err, Error::Corrupt(_)),
        "unwrap 失败 → Corrupt: {err}"
    );
    e.close().unwrap();
}

/// K1-1(DS1):轮换 = 新 KEK 代 + 后台重包裹;两代对象重包裹前后恒可读,
/// rewrap 后 kek_id 收敛到当前代,rewrap_done_gen 落盘,幂等重跑。
#[test]
fn sse_s3_rotation_and_rewrap() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    // 初始代状态:gen 1(惰性)
    let st = e.sse_s3_kek_state().unwrap();
    assert_eq!((st.gen, st.rewrap_done_gen), (1, 1));

    // 代 1 两个对象(内联 + extent)
    let d1 = rnd(1_000, 5);
    let d2 = rnd(150_000, 6);
    for (k, d) in [("g1-a", &d1), ("g1-b", &d2)] {
        let wk = s3_write_key(&e);
        let r = fs3_core::SseWriteKey::SseS3(&wk);
        e.put_with_meta(
            "b1",
            k,
            &mut Cursor::new(d.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&r),
        )
        .unwrap();
    }
    // 轮换 → 代 2;rewrap 待办标记拉开
    let st = e.sse_s3_rotate_kek().unwrap();
    assert_eq!(st.gen, 2);
    assert!(st.last_rotated_at > 0);
    assert_eq!(st.rewrap_done_gen, 1, "轮换后重包裹待办");
    // 代 2 新对象
    let d3 = rnd(2_000, 7);
    let wk = s3_write_key(&e);
    assert_eq!(wk.kek_id(), 2, "mint 恒用当前代");
    let r = fs3_core::SseWriteKey::SseS3(&wk);
    e.put_with_meta(
        "b1",
        "g2-a",
        &mut Cursor::new(d3.clone()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&r),
    )
    .unwrap();

    // 重包裹前:两代对象恒可读(旧代 KEK 由 seed 确定性派生)
    for (k, d) in [("g1-a", &d1), ("g1-b", &d2), ("g2-a", &d3)] {
        let mut out = Vec::new();
        let m = e.head("b1", k).unwrap().unwrap();
        e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
        assert_eq!(&out, d, "{k} 轮换后旧代可读");
    }
    assert_eq!(
        e.head("b1", "g1-a").unwrap().unwrap().sse.unwrap().kek_id,
        1
    );

    // 后台重包裹(直调一轮,同工作线程主体)
    let progress = e.sse_s3_rewrap_progress();
    crate::run_sse_s3_rewrap(e.meta(), &progress).unwrap();
    let p = progress.lock().unwrap().clone();
    assert_eq!(p.rewrapped, 2, "两个代 1 对象被重包裹");
    assert_eq!(p.errors, 0);
    assert_eq!(p.target_gen, 2);
    let st = e.sse_s3_kek_state().unwrap();
    assert_eq!(st.rewrap_done_gen, 2, "完成收敛落盘");
    for k in ["g1-a", "g1-b", "g2-a"] {
        let m = e.head("b1", k).unwrap().unwrap();
        assert_eq!(m.sse.as_ref().unwrap().kek_id, 2, "{k} kek_id 收敛");
    }
    // 重包裹后全部仍可读
    for (k, d) in [("g1-a", &d1), ("g1-b", &d2), ("g2-a", &d3)] {
        let mut out = Vec::new();
        let m = e.head("b1", k).unwrap().unwrap();
        e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
        assert_eq!(&out, d, "{k} 重包裹后可读");
    }
    // 幂等重跑:无待办 → 零重写
    crate::run_sse_s3_rewrap(e.meta(), &progress).unwrap();
    assert_eq!(progress.lock().unwrap().rewrapped, 2, "幂等(无新增)");
    e.close().unwrap();
}

/// K1-1(DS1/DE3):copy 象限——SSE-S3→SSE-S3 同代 COW(SseInfo 逐字节
/// 继承);异代 COW + 元数据级重包裹(数据零搬运,kek_id 收敛);SSE-S3→
/// SSE-C / SSE-C→SSE-S3 换密钥 = 解密重加密;SSE-S3 源 + 目标未加密 →
/// InvalidRequest(DS3 同口径)。
#[test]
fn sse_s3_copy_matrix() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let data = rnd(100_000, 21); // extent 臂
    let wk = s3_write_key(&e);
    let r = fs3_core::SseWriteKey::SseS3(&wk);
    let src = e
        .put_with_meta(
            "b1",
            "s3-src",
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&r),
        )
        .unwrap();
    let ssec = sse_test_key();
    let ckey = fs3_core::SseWriteKey::SseC(&ssec);

    // 象限 A:同代 SSE-S3 → SSE-S3 COW(SseInfo 逐字节继承,零数据搬运)
    let w2 = s3_write_key(&e);
    let w2r = fs3_core::SseWriteKey::SseS3(&w2);
    let m = e
        .copy_object_version_for(
            "b1",
            "s3-src",
            None,
            "b1",
            "s3-cow",
            None,
            None,
            None,
            None,
            VersioningState::Off,
            None,
            Some(&w2r),
        )
        .unwrap();
    assert_eq!(m.sse, src.sse, "同代 COW:SseInfo 逐字节继承");
    assert_eq!(m.etag, src.etag, "COW 不动密文");
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(out, data, "COW 目标无密钥读回 = 明文");

    // 象限 B:轮换后异代 SSE-S3 → SSE-S3 = COW + 元数据重包裹
    e.sse_s3_rotate_kek().unwrap(); // gen 2
    let w3 = s3_write_key(&e);
    assert_eq!(w3.kek_id(), 2);
    let w3r = fs3_core::SseWriteKey::SseS3(&w3);
    let m = e
        .copy_object_version_for(
            "b1",
            "s3-src",
            None,
            "b1",
            "s3-cow2",
            None,
            None,
            None,
            None,
            VersioningState::Off,
            None,
            Some(&w3r),
        )
        .unwrap();
    let sse = m.sse.as_ref().unwrap();
    assert_eq!(sse.kek_id, 2, "异代 copy:kek_id 收敛到目标代");
    assert_ne!(
        sse.wrapped_dek,
        src.sse.as_ref().unwrap().wrapped_dek,
        "重包裹 = 新包裹值"
    );
    assert_eq!(
        sse.nonce_base,
        src.sse.as_ref().unwrap().nonce_base,
        "元数据级重包裹:数据面零触碰(nonce_base/网格共享)"
    );
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(out, data, "异代 COW 目标读回 = 明文");

    // 象限 C:SSE-S3 源 → SSE-C 目标 = 解密重加密(换密钥,数据路径)
    let m = e
        .copy_object_version_for(
            "b1",
            "s3-src",
            None,
            "b1",
            "s3-to-c",
            None,
            None,
            None,
            None,
            VersioningState::Off,
            None,
            Some(&ckey),
        )
        .unwrap();
    let sse = m.sse.as_ref().unwrap();
    assert_eq!(sse.kind, fs3_core::SseKind::SseC);
    assert_eq!(sse.key_md5, ssec.key_md5());
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&ssec))
        .unwrap();
    assert_eq!(out, data, "SSE-S3→SSE-C 重加密后按客户密钥读回");
    assert_ne!(m.etag, src.etag, "重加密 ETag 变(密文变)");

    // 象限 D:SSE-C 源 → SSE-S3 目标 = 解密重加密
    let csrc = e
        .put_with_meta(
            "b1",
            "c-src",
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&ckey),
        )
        .unwrap();
    let w4 = s3_write_key(&e);
    let w4r = fs3_core::SseWriteKey::SseS3(&w4);
    let m = e
        .copy_object_version_for(
            "b1",
            "c-src",
            None,
            "b1",
            "c-to-s3",
            None,
            None,
            None,
            None,
            VersioningState::Off,
            Some(&ssec),
            Some(&w4r),
        )
        .unwrap();
    let sse = m.sse.as_ref().unwrap();
    assert_eq!(sse.kind, fs3_core::SseKind::SseS3);
    assert_eq!(sse.key_md5, [0u8; 16]);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(out, data, "SSE-C→SSE-S3 重加密后无密钥读回");
    assert_ne!(m.etag, csrc.etag);

    // 象限 E:SSE-S3 源 + 目标未指定加密 → InvalidRequest(DS3 同 DE3 口径)
    let err = e
        .copy_object_version_for(
            "b1",
            "s3-src",
            None,
            "b1",
            "s3-q4",
            None,
            None,
            None,
            None,
            VersioningState::Off,
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    e.close().unwrap();
}

/// K1-1(D-E4 复用):SSE-S3 multipart 端到端——Create 会话级 DEK;
/// UploadPart 无客户头(会话 DEK 现解);part 重传幂等(D-E6 确定性
/// nonce);Complete 零头(新签发对象级 DEK);对象读回 = 明文;会话
/// 混用 SSE-C 密钥显式拒绝。
#[test]
fn sse_s3_multipart_e2e() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = create_s3_upload(&mut e, "s3-mp");
    // 两个 extent 分片(≥5MiB 非末片门槛)+ 内联尾片
    let p1d = rnd(5 * 1024 * 1024 + 100, 31);
    let p2d = rnd(1000, 32);
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(p1d.clone()), None, None)
        .unwrap();
    let psse = p1.sse.as_ref().expect("SSE-S3 分片产物");
    assert_eq!(psse.kind, fs3_core::SseKind::SseS3);
    assert_eq!(psse.kek_id, 1);
    assert_eq!(psse.wrapped_dek.len(), fs3_core::SSE_S3_WRAPPED_DEK_LEN);
    // 重传同内容 ⇒ 同 ETag(D-E6 确定性 nonce,会话级 DEK)
    let p1r = e
        .upload_part(&uid, 1, &mut Cursor::new(p1d.clone()), None, None)
        .unwrap();
    assert_eq!(p1r.etag, p1.etag, "分片重传幂等(ETag 稳定)");
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(p2d.clone()), None, None)
        .unwrap();
    // Complete:零客户头(服务端自持)
    let m = e
        .complete_multipart(
            "b1",
            "s3-mp",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    let sse = m.sse.as_ref().unwrap();
    assert_eq!(
        sse.kind,
        fs3_core::SseKind::SseS3,
        "D-E4:对象级 SseInfo 单网格同形态"
    );
    let mut expect = p1d.clone();
    expect.extend_from_slice(&p2d);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(out, expect, "Complete 后整对象无密钥读回 = 拼接明文");

    // 会话混用:SSE-S3 会话 UploadPart 携带 SSE-C 密钥 → InvalidRequest
    let uid2 = create_s3_upload(&mut e, "s3-mp2");
    let key = sse_test_key();
    let err = e
        .upload_part(&uid2, 1, &mut Cursor::new(rnd(10, 1)), None, Some(&key))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    let err = e
        .complete_multipart(
            "b1",
            "s3-mp2",
            &uid2,
            &[cp(1, "x".into())],
            None,
            Some(&key),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
    e.close().unwrap();
}

/// M11 G-2:SSE-S3 Complete 后同一开放 extent 上的遗留 multipart 分片
/// 不得从水位 0 覆写已提交对象(crash-enc rnd4 m11-enc-s3/b3:extent 7
/// 上 leftover part 与 completed 对象段重叠 → GCM 失败)。
#[test]
fn sse_s3_complete_then_leftover_parts_do_not_tear() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let p1d = rnd(5 * 1024 * 1024, 41);
    let p2d = rnd(400_000, 42);
    let uid = create_s3_upload(&mut e, "s3-mp-pack");
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(p1d.clone()), None, None)
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(p2d.clone()), None, None)
        .unwrap();
    let m = e
        .complete_multipart(
            "b1",
            "s3-mp-pack",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    for i in 0..2 {
        let uid_l = e
            .create_multipart(
                "b1",
                &format!("leftover-{i}"),
                None,
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            )
            .unwrap();
        e.upload_part(
            &uid_l,
            1,
            &mut Cursor::new(vec![0xABu8; 300_000]),
            None,
            None,
        )
        .unwrap();
    }
    let mut expect = p1d.clone();
    expect.extend_from_slice(&p2d);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(
        out, expect,
        "leftover parts must not tear completed SSE object"
    );
    e.close().unwrap();
}

/// 模拟 `dec_live` 误清位图后重分配:open_new_extent 必须垫高水位,
/// 不得从 0 覆写仍被元数据引用的打包段(G-2 rnd4 SSE-S3 GCM)。
#[test]
fn open_new_extent_floors_watermark_when_meta_still_holds() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let a = rnd(100_000, 21);
    e.put("b1", "keep", &mut Cursor::new(a.clone())).unwrap();
    let m = e.head("b1", "keep").unwrap().unwrap();
    assert!(!m.extents.is_empty());
    e.debug_false_free_segments(&m.extents);
    let b = rnd(80_000, 22);
    e.put("b1", "next", &mut Cursor::new(b.clone())).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "keep", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(
        out, a,
        "predecessor must survive reallocation of its extent"
    );
    out.clear();
    e.get_to("b1", "next", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, b);
    e.close().unwrap();
}

/// 同上,SSE-C 网格:从水位 0 覆写会表现为 GCM tag 失败。
#[test]
fn open_new_extent_floors_watermark_sse_c() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let a = rnd(200_000, 23);
    let m = e
        .put_with_meta(
            "b1",
            "keep",
            &mut Cursor::new(a.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
        )
        .unwrap();
    e.debug_false_free_segments(&m.extents);
    let b = rnd(80_000, 24);
    e.put_with_meta(
        "b1",
        "next",
        &mut Cursor::new(b),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, Some(&key))
        .unwrap();
    assert_eq!(
        out, a,
        "SSE predecessor must survive false-free reallocation"
    );
    e.close().unwrap();
}

/// Complete 后再误清已提交对象所在 extent:遗留 multipart 分片不得撕裂。
#[test]
fn leftover_parts_after_false_free_do_not_tear_sse() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let p1d = rnd(5 * 1024 * 1024, 41);
    let p2d = rnd(400_000, 42);
    let uid = create_s3_upload(&mut e, "s3-mp-ff");
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(p1d.clone()), None, None)
        .unwrap();
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(p2d.clone()), None, None)
        .unwrap();
    let m = e
        .complete_multipart(
            "b1",
            "s3-mp-ff",
            &uid,
            &[cp(1, p1.etag_hex()), cp(2, p2.etag_hex())],
            None,
            None,
        )
        .unwrap();
    e.debug_false_free_segments(&m.extents);
    for i in 0..2 {
        let uid_l = e
            .create_multipart(
                "b1",
                &format!("leftover-ff-{i}"),
                None,
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
            )
            .unwrap();
        e.upload_part(
            &uid_l,
            1,
            &mut Cursor::new(vec![0xABu8; 300_000]),
            None,
            None,
        )
        .unwrap();
    }
    let mut expect = p1d.clone();
    expect.extend_from_slice(&p2d);
    let mut out = Vec::new();
    e.get_to_meta(&m, 0..u64::MAX, &mut out, None).unwrap();
    assert_eq!(
        out, expect,
        "leftover parts after false-free must not tear completed SSE object"
    );
    e.close().unwrap();
}

/// Corrupt,不得挂死。HTTP 层在承诺 200+Content-Length 前探测同一路径,
/// 避免客户端 Raw ReadTimeout(crash-enc rnd64 m11-enc-c/a0)。
#[test]
fn sse_c_corrupt_ciphertext_fails_fast() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let key = sse_test_key();
    let plain = rnd(200_000, 9);
    e.put_with_meta(
        "b1",
        "sse-torn",
        &mut Cursor::new(plain),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        Some(&fs3_core::SseWriteKey::SseC(&key)),
    )
    .unwrap();
    let meta = e.head("b1", "sse-torn").unwrap().unwrap();
    let seg = meta.extents.first().expect("extent-backed SSE-C object");
    let off = e.extent_data_offset(seg.extent_id as u64) + seg.offset as u64;
    let aligned = off - (off % SECTOR_SIZE);
    let mut buf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize).unwrap();
    e.superblock();
    e.device_slots()[0]
        .dev
        .pread_aligned(buf.as_mut_slice(), aligned)
        .unwrap();
    let skip = (off - aligned) as usize;
    buf.as_mut_slice()[skip] ^= 0xff;
    e.device_slots()[0]
        .dev
        .pwrite_aligned(buf.as_slice(), aligned)
        .unwrap();

    let t0 = std::time::Instant::now();
    let mut out = vec![0u8; 1024];
    let err = e
        .read_at_version_for(
            "b1",
            "sse-torn",
            None,
            0,
            &mut out,
            VersioningState::Off,
            Some(&key),
        )
        .unwrap_err();
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "torn SSE-C read must not hang: {:?}",
        t0.elapsed()
    );
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    e.close().unwrap();
}

// ─────────────────────────── M13 M1-2 多设备池 ───────────────────────────

#[test]
fn multi_device_pool_put_get_checkpoint_roundtrip() -> Result<()> {
    // 双设备(不同规格):自举清单 → 双设备分配 → 每设备检查点 → 重开一致
    // 设备 0 仅 16MiB(≈3 个 extent):批量写入必然跨到设备 1
    let (_d, _imgs, cfg) = setup_multi(&[16 * 1024 * 1024, 64 * 1024 * 1024]);
    let mut e = open_engine(&cfg);

    // 池清单:双元素 + uuid 与设备超块绑定
    let pool = e.meta().load_pool()?.expect("pool manifest");
    assert_eq!(pool.devices.len(), 2);
    assert_eq!(pool.devices[0].uuid, e.device_slots()[0].sb.uuid);
    assert_eq!(pool.devices[1].uuid, e.device_slots()[1].sb.uuid);
    assert_eq!(
        pool.devices[0].extent_count,
        e.device_slots()[0].extent_count
    );
    assert_eq!(
        pool.devices[1].extent_count,
        e.device_slots()[1].extent_count
    );
    // 推导式映射基址
    assert_eq!(e.device_slots()[1].base, e.device_slots()[0].extent_count);

    // 写一批落 extent 的对象(1MiB > 32KiB 内联阈值;12MiB > 设备 0 容量)
    let data = vec![7u8; 1024 * 1024];
    for i in 0..12 {
        e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
            .unwrap();
    }
    // 加权轮转(DM2):两设备都应有分配;权重按剩余空间(设备 1 更大 → 更多)
    let d0 = &e.device_slots()[0];
    let d1 = &e.device_slots()[1];
    let on_d0 = (d0.base..d0.base + d0.extent_count)
        .filter(|&id| e.allocator().test_bit(id))
        .count();
    let on_d1 = (d1.base..d1.base + d1.extent_count)
        .filter(|&id| e.allocator().test_bit(id))
        .count();
    assert!(on_d0 >= 1, "weighted rotation must reach device 0");
    assert!(
        on_d1 > on_d0,
        "larger device must take the larger share (d0={on_d0}, d1={on_d1})"
    );
    e.checkpoint()?;

    // 读回一致
    for i in 0..12 {
        let mut out = Vec::new();
        e.get_to("b1", &format!("k{i}"), 0..data.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, data);
    }
    e.close().unwrap();
    drop(e);

    // 重开:每设备检查点合并恢复 + 数据完好 + 零泄漏
    let e2 = open_engine(&cfg);
    for i in 0..12 {
        let mut out = Vec::new();
        e2.get_to("b1", &format!("k{i}"), 0..data.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, data);
    }
    {
        let report = e2.check_report().unwrap();
        if !report.leaks.is_empty() {
            let bits: Vec<u64> = (0..18).filter(|&i| e2.allocator().test_bit(i)).collect();
            panic!(
                "leaks after multi-device reopen: {:?} (d1 base={}, count={}; allocated={:?}; live={})",
                report.leaks,
                e2.device_slots()[1].base,
                e2.device_slots()[1].extent_count,
                bits,
                e2.allocator().live_bytes_total(),
            );
        }
    }
    e2.abort();
    Ok(())
}

#[test]
fn multi_device_crash_recovery() -> Result<()> {
    // 崩溃(不 close)后重开:每设备开放 extent 续写 + a: 重放幂等
    let (_d, _imgs, cfg) = setup_multi(&[64 * 1024 * 1024, 64 * 1024 * 1024]);
    let mut e = open_engine(&cfg);
    let data = vec![9u8; 300 * 1024];
    for i in 0..6 {
        e.put("b1", &format!("c{i}"), &mut Cursor::new(data.clone()))
            .unwrap();
    }
    e.abort(); // 模拟 kill -9
    let mut e2 = open_engine(&cfg);
    for i in 0..6 {
        let mut out = Vec::new();
        e2.get_to("b1", &format!("c{i}"), 0..data.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, data);
    }
    assert!(e2.check_report().unwrap().leaks.is_empty());
    // 崩溃后继续写入 + 正常关闭(续写路径走设备感知寻址)
    let more = vec![5u8; 100 * 1024];
    e2.put("b1", "after-crash", &mut Cursor::new(more.clone()))
        .unwrap();
    e2.close().unwrap();
    drop(e2);
    let e3 = open_engine(&cfg);
    let mut out = Vec::new();
    e3.get_to("b1", "after-crash", 0..more.len() as u64, &mut out)
        .unwrap();
    assert_eq!(out, more);
    e3.abort();
    Ok(())
}

#[test]
fn multi_device_extra_disk_without_manifest_rejected() -> Result<()> {
    // 已入池单盘 + 配置塞入「已初始化但未入池」的盘 → 拒绝(必须 device-add)
    let (_d, mut imgs, cfg) = setup_multi(&[64 * 1024 * 1024]);
    let mut e = open_engine(&cfg);
    e.put("b1", "k", &mut Cursor::new(vec![1u8; 100])).unwrap();
    e.close().unwrap();
    drop(e);

    let extra = _d.path().join("extra.img");
    std::fs::File::create(&extra)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&extra, 4 * 1024 * 1024, 0, false).unwrap();
    imgs.push(extra);
    let bad_cfg = EngineConfig {
        devices: imgs,
        ..cfg.clone()
    };
    let err = match Engine::open(&bad_cfg) {
        Ok(_) => panic!("open must reject an un-pooled disk in config"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("not in the pool") || msg.contains("device-add"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[test]
fn multi_device_manifest_uuid_mismatch_rejected() -> Result<()> {
    // 换盘(force 重建 = 新 uuid)→ 打开必须拒绝(清单 uuid 绑定)
    let (_d, _imgs, cfg) = setup_multi(&[64 * 1024 * 1024, 64 * 1024 * 1024]);
    let mut e = open_engine(&cfg);
    e.put("b1", "k", &mut Cursor::new(vec![2u8; 100])).unwrap();
    e.close().unwrap();
    drop(e);

    let dev1 = &cfg.devices[1];
    fs3_device::init_device(dev1, 4 * 1024 * 1024, 0, true).unwrap(); // force 重建
    let err = match Engine::open(&cfg) {
        Ok(_) => panic!("open must reject uuid mismatch (wrong disk)"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("uuid does not match") || msg.contains("InvalidLayout"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[test]
fn per_device_open_extents_retained_across_rotation() -> Result<()> {
    // M13 M2-1:等权重双盘,对象交替落盘;两盘开放 extent 同时存活
    let (_d, _imgs, cfg) = setup_multi(&[64 * 1024 * 1024, 64 * 1024 * 1024]);
    let mut e = open_engine(&cfg);
    let data = vec![11u8; 300 * 1024];
    for i in 0..8 {
        e.put("b1", &format!("o{i}"), &mut Cursor::new(data.clone()))
            .unwrap();
    }
    // SWRR 等权重 → 交替落盘;每设备开放 extent 跨对象存活
    let snap = e.open_extent_snapshot();
    assert!(
        snap[0].is_some(),
        "device 0 open extent must survive rotation"
    );
    assert!(snap[1].is_some(), "device 1 open extent must be active");
    assert_ne!(
        snap[0].map(|s| s.0),
        snap[1].map(|s| s.0),
        "open extents must be distinct per device"
    );
    for i in 0..8 {
        let mut out = Vec::new();
        e.get_to("b1", &format!("o{i}"), 0..data.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, data);
    }
    e.checkpoint()?;
    e.close().unwrap();
    drop(e);
    // 重开:全部设备开放 extent 有头;数据一致零泄漏
    let e2 = open_engine(&cfg);
    for i in 0..8 {
        let mut out = Vec::new();
        e2.get_to("b1", &format!("o{i}"), 0..data.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, data);
    }
    assert!(e2.check_report().unwrap().leaks.is_empty());
    e2.abort();
    Ok(())
}

#[test]
fn full_device_excluded_from_weighted_rotation() -> Result<()> {
    // M13 M2-1:等尺寸双盘(各 1 extent);大对象填满设备 0 后,后续分配
    // 全部落设备 1(剩余空间权重 = 0 的盘不再被轮转选为目标)
    let (_d, _imgs, cfg) = setup_multi(&[8 * 1024 * 1024, 8 * 1024 * 1024]);
    let mut e = open_engine(&cfg);
    // 4MiB 对象 > 单 extent 容量(4MiB-4KiB)→ 溢出 4KiB 到第二盘
    let big = vec![3u8; 4 * 1024 * 1024];
    e.put("b1", "fill", &mut Cursor::new(big.clone())).unwrap();
    let d0_base = e.device_slots()[0].base;
    let d0_count = e.device_slots()[0].extent_count;
    let d1_base = e.device_slots()[1].base;
    let d1_count = e.device_slots()[1].extent_count;
    let on = |alloc: &Allocator, base: u64, count: u64| {
        (base..base + count).filter(|&i| alloc.test_bit(i)).count()
    };
    assert_eq!(
        on(e.allocator(), d0_base, d0_count),
        1,
        "device 0 must be full"
    );
    assert_eq!(
        on(e.allocator(), d1_base, d1_count),
        1,
        "spill must land on device 1"
    );

    let more = vec![4u8; 1024 * 1024];
    for i in 0..3 {
        e.put("b1", &format!("m{i}"), &mut Cursor::new(more.clone()))
            .unwrap();
    }
    // 设备 0 已满 → 不再获得新分配;设备 1 继续吃进
    assert_eq!(
        on(e.allocator(), d0_base, d0_count),
        1,
        "full device must not receive new extents"
    );
    // 设备 1 开放 extent 剩余 ~4MiB 吸收全部新数据(4096+3MiB < 容量),
    // 无需新分配——剩余空间权重把开放 extent 空间计入(字节口径)
    assert_eq!(
        on(e.allocator(), d1_base, d1_count),
        1,
        "device 1 open extent must absorb the new data"
    );

    // 数据完整
    for i in 0..3 {
        let mut out = Vec::new();
        e.get_to("b1", &format!("m{i}"), 0..more.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, more);
    }
    let mut out = Vec::new();
    e.get_to("b1", "fill", 0..big.len() as u64, &mut out)
        .unwrap();
    assert_eq!(out, big);
    e.abort();
    Ok(())
}
