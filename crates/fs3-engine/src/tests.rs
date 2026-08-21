//! 引擎测试:基础回归(段模型适配)+ ADR-9 打包语义 + 属性测试。
//! 压缩专项测试见 compaction.rs。

use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;

fn test_cfg(dev: &Path, meta_dir: &Path) -> EngineConfig {
    EngineConfig {
        device: dev.to_path_buf(),
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
    // 未提交事务:对象不可见,extent 全部回滚,开放 extent 水位回退
    assert!(e.head("b1", "partial").unwrap().is_none());
    assert_eq!(e.allocator().allocated_count(), 0);
    // 回退后可继续写入(孤儿区被覆盖)
    let d = vec![9u8; 100_000];
    e.put("b1", "after", &mut Cursor::new(d.clone())).unwrap();
    let mut out = Vec::new();
    e.get_to("b1", "after", 0..u64::MAX, &mut out).unwrap();
    assert_eq!(out, d);
    assert_eq!(e.allocator().allocated_count(), 1);
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
    e.close().unwrap();
}

#[test]
fn verify_reads_detects_corruption() {
    let (_d, cfg) = setup();
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let img_path = cfg.device.clone();
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
        )
        .unwrap();
    assert_eq!(m.content_type, "text/plain");
    assert_eq!(m.user_meta, vec![("x-amz-meta-foo".into(), "bar".into())]);
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
        )
        .unwrap();
    assert_eq!(uid.len(), 32);

    // 分片 1:5MiB(内联阈值 32KiB 之上 → extent);分片 2:小内联
    let part1 = vec![0x11u8; 5 * 1024 * 1024];
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(part1.clone()))
        .unwrap();
    assert_eq!(p1.size, part1.len() as u64);
    assert!(p1.inline.is_none() && !p1.extents.is_empty());
    let part2 = vec![0x22u8; 1000];
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(part2.clone()))
        .unwrap();
    assert!(p2.inline.is_some());

    // ListParts 升序
    let parts = e.list_parts(&uid).unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].0, 1);
    assert_eq!(parts[1].0, 2);

    // 完成:混合路径(数据组合)
    let m = e
        .complete_multipart("b1", "big", &uid, &[(1, p1.etag_hex()), (2, p2.etag_hex())])
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
        .complete_multipart("b1", "big", &uid, &[(1, p1.etag_hex()), (2, p2.etag_hex())])
        .unwrap();
    assert_eq!(m2.etag, m.etag);
    assert_eq!(m2.size, m.size);

    // 会话仍在(重传分片可 reactivate)
    let p_new = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0x33u8; 100]))
        .unwrap();
    let m3 = e
        .complete_multipart("b1", "big", &uid, &[(1, p_new.etag_hex())])
        .unwrap();
    assert_eq!(m3.size, 100);
    let mut out3 = Vec::new();
    e.get_to("b1", "big", 0..100, &mut out3).unwrap();
    assert_eq!(out3, vec![0x33u8; 100]);
    e.close().unwrap();
}

/// 零数据搬运:全部大分片段直接拼接(对象段引用 == 分片之和)。
#[test]
fn multipart_extent_concat_no_copy() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e.create_multipart("b1", "big", None, vec![]).unwrap();
    let mut total_refs = 0usize;
    let mut parts_meta = Vec::new();
    for i in 0..3 {
        let data = vec![i as u8; 5 * 1024 * 1024];
        let p = e.upload_part(&uid, i + 1, &mut Cursor::new(data)).unwrap();
        total_refs += p.extents.len();
        parts_meta.push((i + 1, p.etag_hex()));
    }
    let m = e
        .complete_multipart("b1", "big", &uid, &parts_meta)
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
    let uid = e.create_multipart("b1", "big", None, vec![]).unwrap();
    // 5MiB 分片:独占整块 + 尾段(1028KiB,开放)
    let p1 = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]))
        .unwrap();
    // 普通对象进入同一开放 extent(打包)
    e.put("b1", "plain", &mut Cursor::new(vec![2u8; 100_000]))
        .unwrap();
    // 末分片(无大小约束)继续打包
    let p2 = e
        .upload_part(&uid, 2, &mut Cursor::new(vec![3u8; 100_000]))
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
        .complete_multipart("b1", "big", &uid, &[(1, p1.etag_hex()), (2, p2.etag_hex())])
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
    let uid = e.create_multipart("b1", "k", None, vec![]).unwrap();

    // 未知会话
    assert!(matches!(
        e.upload_part("nope", 1, &mut Cursor::new(vec![1u8; 10])),
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        e.complete_multipart("b1", "k", "nope", &[(1, "x".into())]),
        Err(Error::NoSuchUpload(_))
    ));
    // 分片 ETag 不匹配 → InvalidPart
    let p = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0u8; 1]))
        .unwrap();
    assert!(matches!(
        e.complete_multipart(
            "b1",
            "k",
            &uid,
            &[(1, "ffffffffffffffffffffffffffffffff".into())]
        ),
        Err(Error::InvalidPart(_))
    ));
    // 列出不存在的分片号 → InvalidPart(s3-tests missing_part)
    let p2 = e
        .upload_part(&uid, 3, &mut Cursor::new(vec![0u8; 1]))
        .unwrap();
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[(9999, p.etag_hex())]),
        Err(Error::InvalidPart(_))
    ));
    // 非最后分片 < 5MiB → PartTooSmall(part 1 非最后且 < 5MiB)
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[(1, p.etag_hex()), (3, p2.etag_hex())]),
        Err(Error::PartTooSmall(_))
    ));
    // 乱序 part_no:map 语义(仅校验存在 + ETag);part 1 非最后且 < 5MiB → PartTooSmall
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[(3, p2.etag_hex()), (1, p.etag_hex())]),
        Err(Error::PartTooSmall(_))
    ));
    // 重复 part_no:最后一次生效(resend_first_finishes_last 语义)
    let big = e
        .upload_part(&uid, 1, &mut Cursor::new(vec![0x55u8; 5 * 1024 * 1024]))
        .unwrap();
    let m = e
        .complete_multipart("b1", "k", &uid, &[(1, p.etag_hex()), (1, big.etag_hex())])
        .unwrap();
    assert_eq!(m.size, 5 * 1024 * 1024);
    let mut out = Vec::new();
    e.get_to("b1", "k", 0..m.size, &mut out).unwrap();
    assert!(out.iter().all(|&b| b == 0x55));
    // 空列表 → InvalidArgument(服务层映射 MalformedXML)
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[]),
        Err(Error::InvalidArgument(_))
    ));
    e.close().unwrap();
}

#[test]
fn multipart_abort_frees_extents() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e.create_multipart("b1", "k", None, vec![]).unwrap();
    e.upload_part(&uid, 1, &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]))
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
    let c = e.copy_object("b1", "src", "b1", "dst", None, None).unwrap();
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
        )
        .unwrap();
    assert_eq!(c2.content_type, "text/x");
    assert_eq!(c2.user_meta, vec![("m".into(), "n".into())]);
    // 源不存在 → NotFound
    assert!(matches!(
        e.copy_object("b1", "nope", "b1", "x", None, None),
        Err(Error::NotFound(_))
    ));
    e.close().unwrap();
}

/// 会话过期回收(TTL=0 → 立即过期)。
#[test]
fn multipart_sweep_expired() {
    let (_d, cfg) = setup();
    let mut e = open_engine(&cfg);
    let uid = e.create_multipart("b1", "k", None, vec![]).unwrap();
    e.upload_part(&uid, 1, &mut Cursor::new(vec![1u8; 5 * 1024 * 1024]))
        .unwrap();
    let n = e.sweep_expired_sessions(0).unwrap();
    assert_eq!(n, 1);
    assert!(matches!(
        e.complete_multipart("b1", "k", &uid, &[]),
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
    e.copy_object("b1", "a", "b1", "a2", None, None).unwrap();
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
        let dev = fs3_device::open_device(&cfg.device, false).unwrap();
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
        let dev = fs3_device::open_device(&cfg.device, false).unwrap();
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
    let img = cfg.device.clone();
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
                        e.copy_object("b1", &key, "b1", &dst, None, None).unwrap();
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
        device: img,
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
