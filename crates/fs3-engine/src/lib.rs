//! FastS3 存储引擎:PUT/GET/DELETE 全链路、崩溃恢复、检查点策略。
//!
//! 时序保证(DESIGN §4.5):数据先落盘(O_DIRECT 写返回)、元数据后提交
//! (sled 事务 + 组提交);客户端中断 → 不提交事务,extent 回滚释放。
//! 启动恢复(DESIGN §4.10):超级块 → sled → 检查点 → a: 重放 → 引用计数
//! 可达性重建 → 泄漏报告。

pub mod io;

use md5::Digest;
use std::io::{Read, Write};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use fs3_alloc::{Allocator, Checkpointer};
use fs3_core::{
    align_up, random_bytes, BucketMeta, BucketStats, Error, ExtentHeader, ExtentRef, ObjectMeta,
    Result, CHECKPOINT_ALLOC_DELTA, SECTOR_SIZE,
};
use fs3_device::{open_device, BlockDevice};
use fs3_meta::keys::part_key;
use fs3_meta::{
    AllocDraft, MetaConfig, MetaStore, MultipartSession, PartMeta, StatsDelta, SyncMode,
};

use crate::io::{fsync, open_io_engine, read_exact, write_all, IoEngine};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 数据设备路径(裸设备或镜像文件)。
    pub device: std::path::PathBuf,
    /// sled 元数据目录。
    pub meta_dir: std::path::PathBuf,
    pub sync_mode: SyncMode,
    pub group_commit_ms: u64,
    /// 检查点时间触发间隔(秒)。
    pub checkpoint_interval_secs: u64,
    /// 读校验开关(默认关)。
    pub verify_reads: bool,
    /// 优先 io_uring(失败自动降级 pread/pwrite)。
    pub io_uring: bool,
    /// 只读打开(供 check 等只读工具)。
    pub read_only: bool,
    /// 小对象内联阈值(E3;≤ 该值的数据存元数据,零设备 I/O)。
    pub small_object_limit: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            device: std::path::PathBuf::new(),
            meta_dir: std::path::PathBuf::new(),
            sync_mode: SyncMode::Group,
            group_commit_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            checkpoint_interval_secs: fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS,
            verify_reads: false,
            io_uring: true,
            read_only: false,
            small_object_limit: fs3_core::SMALL_OBJECT_LIMIT,
        }
    }
}

/// 检查点状态(触发策略:C2:时间间隔 / 分配增量)。
#[derive(Debug, Default)]
struct CheckpointState {
    /// 最近检查点覆盖到的 seq。
    seq: u64,
    /// 自最近检查点以来分配的 extent 数(64MB 增量触发)。
    alloc_since: u64,
    /// 是否有未检查点的变更(时间 tick 到来时避免空转)。
    dirty: bool,
}

/// 只读摘要(check 命令)。
#[derive(Debug, Default)]
pub struct CheckReport {
    pub device: String,
    pub device_capacity: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    pub allocated_extents: u64,
    pub buckets: usize,
    pub objects: usize,
    pub total_bytes: u64,
    pub leaks: Vec<u64>,
    pub io_engine: &'static str,
    pub checkpoint_seq: u64,
    pub last_seq: u64,
}

pub struct Engine {
    device: Box<dyn BlockDevice>,
    /// 零拷贝专用 fd(无 O_DIRECT;sendfile/splice 用;None = 不可用)。
    zc_fd: Option<i32>,
    sb: fs3_core::SuperBlock,
    alloc: Arc<Allocator>,
    meta: MetaStore,
    /// Mutex 包装:使 Engine 满足 Sync(服务层 RwLock 共享;锁内无竞争,
    /// 引擎写锁已互斥)。
    io: std::sync::Mutex<Box<dyn IoEngine>>,
    chunk_size: usize,
    verify_reads: bool,
    read_only: bool,
    small_object_limit: usize,
    checkpoint: std::sync::Mutex<CheckpointState>,
    checkpoint_tick: std::sync::Mutex<Receiver<()>>,
    _checkpoint_thread: Option<std::thread::JoinHandle<()>>,
    closed: bool,
}

impl Engine {
    /// 打开引擎(含完整恢复流程);设备未初始化返回 NotInitialized。
    pub fn open(cfg: &EngineConfig) -> Result<Self> {
        let device = open_device(&cfg.device, cfg.read_only)?;
        // 零拷贝 fd(尽力而为;失败则禁用零拷贝读路径)
        let zc_fd = if cfg.read_only {
            None
        } else {
            fs3_device::open_zerocopy_fd(&cfg.device).ok()
        };
        let sb = fs3_device::read_superblock(device.as_ref())?;

        let alloc = Arc::new(Allocator::new(sb.extent_count()));

        // 1. 加载检查点(有效且代数最大的槽)
        let checkpointer = Checkpointer::new(device.as_ref(), &sb);
        let cp = checkpointer
            .load_latest()?
            .ok_or_else(|| Error::Corrupt("no valid checkpoint found".into()))?;
        alloc.restore_bitmap(&cp.bitmap);
        alloc.restore_stats(cp.total_alloc, cp.total_free);

        // 2. 打开 sled(其自身 WAL 恢复)
        let meta_cfg = MetaConfig {
            flush_every_ms: cfg.group_commit_ms,
            sync_mode: cfg.sync_mode,
            cache_capacity: None,
        };
        let meta = MetaStore::open(&cfg.meta_dir, &meta_cfg)?;

        // 3. 重放 seq > 检查点序号的 a: 记录 → 恢复位图
        let recs = meta.list_alloc_records(cp.seq)?;
        if !recs.is_empty() {
            tracing::info!(
                "replaying {} alloc records after checkpoint seq {}",
                recs.len(),
                cp.seq
            );
        }
        for rec in &recs {
            alloc.apply_record(rec);
        }

        // 4. 引用计数 = 元数据可达性重建(mark;位图 vs 元数据核对 = 泄漏扫描)
        let leaks = rebuild_refcounts(&meta, alloc.as_ref());
        if !leaks.is_empty() {
            tracing::warn!(
                "recovery found {} leaked extents (allocated but unreachable)",
                leaks.len()
            );
        }

        let io = open_io_engine(cfg.io_uring)?;

        // 5. 检查点定时线程(时间触发策略)
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = std::time::Duration::from_secs(cfg.checkpoint_interval_secs.max(1));
        let thread = std::thread::spawn(move || loop {
            if tx.send(()).is_err() {
                break;
            }
            std::thread::sleep(interval);
        });

        let last_seq = meta.last_seq()?;
        Ok(Engine {
            zc_fd,
            device,
            sb,
            alloc,
            meta,
            io: std::sync::Mutex::new(io),
            chunk_size: fs3_core::DEFAULT_CHUNK_SIZE,
            verify_reads: cfg.verify_reads,
            read_only: cfg.read_only,
            small_object_limit: cfg.small_object_limit,
            checkpoint: std::sync::Mutex::new(CheckpointState {
                seq: cp.seq.max(last_seq),
                alloc_since: 0,
                dirty: false,
            }),
            checkpoint_tick: std::sync::Mutex::new(rx),
            _checkpoint_thread: Some(thread),
            closed: false,
        })
    }

    pub fn superblock(&self) -> &fs3_core::SuperBlock {
        &self.sb
    }

    pub fn meta(&self) -> &MetaStore {
        &self.meta
    }

    pub fn allocator(&self) -> &Allocator {
        &self.alloc
    }

    pub fn io_engine_name(&self) -> &'static str {
        self.io.lock().unwrap().name()
    }

    /// 每个写操作后调用:处理检查点定时 tick 与分配增量触发。
    fn maybe_checkpoint(&mut self) -> Result<()> {
        let due = {
            let mut st = self.checkpoint.lock().unwrap();
            let tick = matches!(self.checkpoint_tick.lock().unwrap().try_recv(), Ok(()));
            let delta = st.alloc_since * self.sb.extent_size >= CHECKPOINT_ALLOC_DELTA;
            let timed = tick && st.dirty;
            if delta || timed {
                st.alloc_since = 0;
                st.dirty = false;
                true
            } else {
                false
            }
        };
        if due {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn note_alloc(&self, n: u64) {
        let mut st = self.checkpoint.lock().unwrap();
        st.alloc_since += n;
        st.dirty = true;
    }

    /// 立即写检查点(位图 + 统计 + seq)。
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let seq = self.meta.last_seq()?;
        let cp = self.alloc.checkpoint_data(seq);
        let checkpointer = Checkpointer::new(self.device.as_ref(), &self.sb);
        let gen = checkpointer.save(&cp)?;
        let mut st = self.checkpoint.lock().unwrap();
        st.seq = seq;
        st.alloc_since = 0;
        st.dirty = false;
        tracing::debug!("checkpoint saved: gen {gen}, seq {seq}");
        Ok(())
    }

    /// 模拟崩溃(kill -9):跳过最终检查点直接释放资源。
    /// sled 自身 WAL 仍会落盘;位图恢复依赖 a: 重放。
    pub fn abort(mut self) {
        self.closed = true;
    }

    /// 优雅关闭:最终检查点 + 元数据 flush。
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(fd) = self.zc_fd.take() {
            // SAFETY: fd 由 open_zerocopy_fd 打开。
            unsafe { libc::close(fd) };
        }
        self.checkpoint()?;
        self.meta.flush()?;
        Ok(())
    }

    /// 确保桶存在(不存在则创建;M0 CLI 便捷路径)。
    pub fn ensure_bucket(&mut self, name: &str) -> Result<()> {
        if self.meta.get_bucket(name)?.is_some() {
            return Ok(());
        }
        let meta = BucketMeta {
            created: now_ts(),
            owner: "default".into(),
            stats: BucketStats::default(),
            quota: None,
        };
        self.meta.commit_bucket_put(name, &meta)?;
        Ok(())
    }

    /// 列出桶。
    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        self.meta.list_buckets()
    }

    /// 列出桶内对象(前缀扫描)。
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        self.meta.list_objects(bucket, prefix)
    }

    /// 分页列举(前缀 + delimiter 分组 + 游标;ListObjectsV1/V2 用)。
    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<fs3_meta::ListPage> {
        self.meta
            .list_objects_page(bucket, prefix, delimiter, after, max)
    }

    // ─────────────────────────── PUT ───────────────────────────

    /// 流式 PUT(便捷入口:默认无自定义头)。
    pub fn put(&mut self, bucket: &str, key: &str, reader: &mut dyn Read) -> Result<ObjectMeta> {
        self.put_with_meta(bucket, key, reader, None, Vec::new())
    }

    /// PUT 全路径:先读前缀判定内联(E3);超过阈值走 extent 流水线。
    ///
    /// 时序保证(DESIGN §4.5):数据先落盘、元数据后提交;任何错误回滚
    /// 已暂存分配;客户端中断 → 不提交事务、extent 释放。
    pub fn put_with_meta(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
    ) -> Result<ObjectMeta> {
        if self.meta.get_bucket(bucket)?.is_none() {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        }
        let old = self.meta.get_object(bucket, key)?;
        let old_ids: Vec<u64> = old
            .as_ref()
            .map(|o| o.extents.iter().map(|e| e.extent_id).collect())
            .unwrap_or_default();
        let old_size = old.as_ref().map(|o| o.size as i64).unwrap_or(0);

        // 1) 读前缀(≤ small_object_limit+1 字节)判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            if prefix.len() > limit {
                break;
            }
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            prefix.extend_from_slice(&buf[..n]);
            if prefix.len() > limit {
                break;
            }
        }

        if prefix.len() <= limit {
            // —— 内联路径(E3):零设备 I/O,一条 sled 事务 ——
            let size = prefix.len() as u64;
            let etag: [u8; 16] = md5::Md5::digest(&prefix).into();
            let meta = ObjectMeta {
                size,
                etag,
                mtime: now_ts(),
                extents: Vec::new(),
                content_type: content_type
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                user_meta,
                inline: Some(prefix),
                parts: vec![],
            };
            if !old_ids.is_empty() {
                self.alloc.release(&old_ids);
            }
            let draft = self.alloc.take_draft();
            let delta = StatsDelta {
                objects: if old_ids.is_empty() { 1 } else { 0 },
                bytes: size as i64 - old_size,
            };
            return match self.meta.commit_object_put(
                bucket,
                key,
                &meta,
                to_alloc_draft(&draft),
                delta,
            ) {
                Ok(_) => {
                    self.alloc.confirm_draft();
                    self.maybe_checkpoint()?;
                    Ok(meta)
                }
                Err(e) => {
                    self.alloc.rollback_draft(&draft);
                    Err(e)
                }
            };
        }

        // —— extent 路径:前缀先写入,再续流 ——
        let mut prefixed = PrefixedReader {
            prefix,
            pos: 0,
            inner: reader,
        };
        let result = self.put_stream(PutCtx {
            bucket,
            key,
            reader: &mut prefixed,
            old_size,
            old_ids,
            content_type,
            user_meta,
        });
        match result {
            Ok(meta) => {
                self.alloc.confirm_draft();
                self.maybe_checkpoint()?;
                Ok(meta)
            }
            Err(e) => {
                let draft = self.alloc.take_draft();
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// extent 写路径流水线:64KiB chunk 攒批 → O_DIRECT 写;数据先落盘。
    ///
    /// 输入流按 extent/chunk 边界切分:chunk(64KiB)是 CRC 单元,与 extent
    /// 载荷 64KiB 对齐;跨 extent 的输入在边界拆分,绝不超过 extent 容量
    /// (防越界写坏下一个 extent 的头)。
    fn put_stream(&mut self, ctx: PutCtx) -> Result<ObjectMeta> {
        let PutCtx {
            bucket,
            key,
            reader,
            old_size,
            old_ids,
            content_type,
            user_meta,
        } = ctx;
        let (extents, size, etag) = self.stream_to_extents(reader)?;

        let mtime = now_ts();
        let meta = ObjectMeta {
            size,
            etag,
            mtime,
            extents,
            content_type: content_type
                .unwrap_or("application/octet-stream")
                .to_string(),
            user_meta,
            inline: None,
            parts: vec![],
        };

        // 覆盖:旧 extent 释放记录进同一事务
        if !old_ids.is_empty() {
            self.alloc.release(&old_ids);
        }
        let draft = self.alloc.take_draft();
        // 覆盖语义:对象数不变(用 old_ids 是否为空判断;空对象覆盖也算覆盖);
        // 字节数 = 新大小 - 旧大小。
        let delta = StatsDelta {
            objects: if old_ids.is_empty() { 1 } else { 0 },
            bytes: size as i64 - old_size,
        };
        self.meta
            .commit_object_put(bucket, key, &meta, to_alloc_draft(&draft), delta)?;
        Ok(meta)
    }

    /// 数据流 → extent 流水线(64KiB chunk 攒批 → O_DIRECT 写;CRC 入 extent
    /// 头)。返回 (extents, size, md5)。分配/写错误自动回滚已暂存分配;
    /// 不提交任何元数据(由调用方决定提交形式:对象 / 分片 / 组合)。
    fn stream_to_extents(
        &mut self,
        reader: &mut dyn Read,
    ) -> Result<(Vec<ExtentRef>, u64, [u8; 16])> {
        let mut writer = ExtentWriter::new(self.chunk_size)?;
        let mut inbuf = fs3_device::AlignedBuffer::new(self.chunk_size)?;
        loop {
            let n = read_up_to(reader, inbuf.as_mut_slice())?;
            if n == 0 {
                break;
            }
            writer.feed(self, &inbuf.as_slice()[..n])?;
        }
        writer.finish(self)
    }

    /// 把 chunk 累积缓冲(acc[..fill])写入当前 extent,并记 CRC。
    /// 写满 chunk 或 extent 将满时调用;写入补零到 4KiB 对齐。
    fn flush_extent_chunk(
        &mut self,
        st: &mut WriteState,
        acc: &mut fs3_device::AlignedBuffer,
        fill: &mut usize,
        hasher: &mut md5::Md5,
    ) -> Result<()> {
        if *fill == 0 {
            return Ok(());
        }
        let crc = fs3_core::crc32c::crc32c(&acc.as_slice()[..*fill], 0);
        let write_len = align_up(*fill as u64, SECTOR_SIZE) as usize;
        if write_len > *fill {
            acc.as_mut_slice()[*fill..write_len].fill(0);
        }
        let dev_off = self.extent_data_offset(st.extent_id) + st.written as u64;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            &acc.as_slice()[..write_len],
            dev_off,
        )?;
        st.written += *fill as u32;
        st.chunk_crcs.push(crc);
        hasher.update(&acc.as_slice()[..*fill]);
        *fill = 0;
        Ok(())
    }

    /// 写 extent 头(含全部 chunk CRC;数据之后写,防撕裂),返回对象引用。
    fn finalize_extent(
        &mut self,
        st: &WriteState,
        owner_id: [u8; 16],
        chunk_size: u32,
    ) -> Result<ExtentRef> {
        let header = ExtentHeader {
            generation: self.alloc.generation(st.extent_id),
            owner_id,
            object_offset: 0, // 对象内偏移由元数据 extent 列表隐含
            chunk_size,
            chunk_crcs: st.chunk_crcs.clone(),
        };
        let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
        hbuf.as_mut_slice().copy_from_slice(&header.encode());
        let off = self.sb.data_start + st.extent_id * self.sb.extent_size;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            hbuf.as_slice(),
            off,
        )?;
        Ok(ExtentRef {
            extent_id: st.extent_id,
            offset: 0,
            len: st.written,
        })
    }

    /// extent 数据区在设备上的偏移。
    fn extent_data_offset(&self, extent_id: u64) -> u64 {
        self.sb.data_start + extent_id * self.sb.extent_size + fs3_core::EXTENT_HEADER_SIZE
    }

    // ─────────────────────────── GET ───────────────────────────

    /// 读对象内容到 out(支持 Range;verify_reads 时逐 chunk 校验)。
    pub fn get_to(
        &mut self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
    ) -> Result<u64> {
        let meta = self
            .meta
            .get_object(bucket, key)?
            .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;

        let start = range.start.min(meta.size);
        let end = range.end.min(meta.size);
        if start >= end {
            return Ok(0);
        }

        // 内联对象:直接拷贝(E3)
        if let Some(inline) = &meta.inline {
            let len = (end - start) as usize;
            out.write_all(&inline[start as usize..start as usize + len])?;
            return Ok(len as u64);
        }

        let mut written = 0u64;
        // 对象内累积偏移:extent 数据按序拼接
        let mut obj_pos = 0u64;
        for ext in &meta.extents {
            let ext_begin = obj_pos;
            let ext_end = obj_pos + ext.len as u64;
            obj_pos = ext_end;
            let seg_start = ext_begin.max(start);
            let seg_end = ext_end.min(end);
            if seg_start >= seg_end {
                continue;
            }
            // extent 数据区内的偏移
            let payload_off = seg_start - ext_begin;
            let dev_off = self.extent_data_offset(ext.extent_id) + payload_off;
            let len = (seg_end - seg_start) as usize;

            if self.verify_reads {
                self.read_verified_chunks(ext, payload_off, len, out, &mut written)?;
            } else {
                let mut done = 0usize;
                while done < len {
                    let cur_off = dev_off + done as u64;
                    // 对齐到 4KiB 块边界读取,再裁剪
                    let block_off = cur_off - (cur_off % SECTOR_SIZE);
                    let skip = (cur_off - block_off) as usize;
                    let want = ((len - done) + skip).min(self.chunk_size);
                    let block_len = align_up(want as u64, SECTOR_SIZE) as usize;
                    let mut rbuf = fs3_device::AlignedBuffer::new(block_len)?;
                    read_exact(
                        &mut **self.io.lock().unwrap(),
                        self.device.raw_fd(),
                        rbuf.as_mut_slice(),
                        block_off,
                    )?;
                    let usable = &rbuf.as_slice()[skip..skip + (want - skip).min(len - done)];
                    out.write_all(usable)?;
                    done += usable.len();
                    written += usable.len() as u64;
                }
            }
        }
        Ok(written)
    }

    /// verify_reads:按 chunk 读取并校验 CRC(开销约 3~5%,DESIGN §4.6)。
    fn read_verified_chunks(
        &mut self,
        ext: &ExtentRef,
        payload_off: u64,
        len: usize,
        out: &mut dyn Write,
        written: &mut u64,
    ) -> Result<()> {
        // 读 extent 头(含 chunk CRC 表)
        let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
        let hdr_off = self.sb.data_start + ext.extent_id * self.sb.extent_size;
        read_exact(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            hbuf.as_mut_slice(),
            hdr_off,
        )?;
        let header = ExtentHeader::decode(hbuf.as_slice())?;
        let chunk_size = header.chunk_size as u64;

        let mut pos = payload_off;
        let end = payload_off + len as u64;
        while pos < end {
            let chunk_idx = (pos / chunk_size) as usize;
            let chunk_start = chunk_idx as u64 * chunk_size;
            let chunk_len = ((chunk_start + chunk_size).min(ext.len as u64) - chunk_start) as usize;
            let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
            let mut cbuf = fs3_device::AlignedBuffer::new(read_len)?;
            let dev_off = self.extent_data_offset(ext.extent_id) + chunk_start;
            read_exact(
                &mut **self.io.lock().unwrap(),
                self.device.raw_fd(),
                cbuf.as_mut_slice(),
                dev_off,
            )?;
            let data = &cbuf.as_slice()[..chunk_len];
            if !header.verify_chunk(chunk_idx, data) {
                return Err(Error::Corrupt(format!(
                    "chunk {chunk_idx} crc mismatch in extent {}",
                    ext.extent_id
                )));
            }
            let skip = (pos - chunk_start) as usize;
            let usable = &data[skip..(end - chunk_start).min(chunk_len as u64) as usize];
            out.write_all(usable)?;
            *written += usable.len() as u64;
            pos += usable.len() as u64;
        }
        Ok(())
    }

    /// 读对象元数据。
    pub fn head(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        self.meta.get_object(bucket, key)
    }

    /// 对象顺序读原语:从 `offset` 读至多 `buf.len()` 字节,返回实际字节数。
    ///
    /// 内联对象直接拷贝;extent 对象按 4KiB 对齐块读取后裁剪。供 HTTP
    /// 层边读边发(每 chunk 上锁,见 fs3-s3/fs3-http)。verify_reads 校验
    /// 走 get_to(整段路径)。
    pub fn read_at(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        let meta = self
            .meta
            .get_object(bucket, key)?
            .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
        if offset >= meta.size || buf.is_empty() {
            return Ok(0);
        }
        let want = ((meta.size - offset) as usize).min(buf.len());

        if let Some(inline) = &meta.inline {
            let start = offset as usize;
            buf[..want].copy_from_slice(&inline[start..start + want]);
            return Ok(want);
        }

        // extent 路径:定位 offset 所在 extent(对象内偏移连续)
        let mut obj_pos = 0u64;
        let mut done = 0usize;
        for ext in &meta.extents {
            let ext_begin = obj_pos;
            let ext_end = obj_pos + ext.len as u64;
            obj_pos = ext_end;
            if offset >= ext_end || offset < ext_begin {
                continue;
            }
            let in_ext = offset - ext_begin;
            let avail = (ext_end - offset) as usize;
            let take = want.min(avail);
            let dev_base = self.extent_data_offset(ext.extent_id) + in_ext;
            let mut got = 0usize;
            while got < take {
                let cur = dev_base + got as u64;
                let block_off = cur - (cur % SECTOR_SIZE);
                let skip = (cur - block_off) as usize;
                let step = (take - got + skip).min(self.chunk_size);
                let read_len = align_up(step as u64, SECTOR_SIZE) as usize;
                let mut rbuf = fs3_device::AlignedBuffer::new(read_len)?;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device.raw_fd(),
                    rbuf.as_mut_slice(),
                    block_off,
                )?;
                let usable = &rbuf.as_slice()[skip..skip + (take - got).min(step - skip)];
                buf[done..done + usable.len()].copy_from_slice(usable);
                got += usable.len();
                done += usable.len();
            }
            break;
        }
        Ok(done)
    }

    // ─────────────────────────── DELETE ───────────────────────────

    /// 删除对象:元数据 + 释放记录同事务;refcount 归零的 extent 立即回位图。
    pub fn delete(&mut self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        let ids: Vec<u64> = meta.extents.iter().map(|e| e.extent_id).collect();
        self.alloc.release(&ids);
        let draft = self.alloc.take_draft();
        let delta = StatsDelta {
            objects: -1,
            bytes: -(meta.size as i64),
        };
        match self
            .meta
            .commit_object_delete(bucket, key, to_alloc_draft(&draft), delta)
        {
            Ok(_) => {
                self.alloc.confirm_draft();
                self.maybe_checkpoint()?;
                Ok(Some(meta))
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// 删除桶(必须为空,或 force 时连同对象删除)。
    pub fn delete_bucket(&mut self, name: &str, force: bool) -> Result<()> {
        let objects = self.meta.list_objects(name, "")?;
        if !objects.is_empty() && !force {
            return Err(Error::InvalidArgument(format!(
                "bucket {name} not empty ({} objects)",
                objects.len()
            )));
        }
        for (key, meta) in objects {
            let ids: Vec<u64> = meta.extents.iter().map(|e| e.extent_id).collect();
            self.alloc.release(&ids);
            let draft = self.alloc.take_draft();
            let delta = StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
            };
            match self
                .meta
                .commit_object_delete(name, &key, to_alloc_draft(&draft), delta)
            {
                Ok(_) => self.alloc.confirm_draft(),
                Err(e) => {
                    self.alloc.rollback_draft(&draft);
                    return Err(e);
                }
            }
        }
        self.meta.commit_bucket_delete(name)?;
        Ok(())
    }

    // ─────────────────────────── multipart(F5) ───────────────────────────

    /// 创建分片上传会话;返回 128 位随机 uploadId(hex)。
    pub fn create_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
    ) -> Result<String> {
        if self.meta.get_bucket(bucket)?.is_none() {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        }
        let mut raw = [0u8; 16];
        random_bytes(&mut raw)?;
        let upload_id = hex::encode(raw);
        let session = MultipartSession::new(
            bucket,
            key,
            content_type.unwrap_or("application/octet-stream"),
            user_meta,
        );
        self.meta.create_multipart(&upload_id, &session)?;
        Ok(upload_id)
    }

    /// 上传分片:数据写 extent(小分片内联),元数据挂 `p:` 会话下。
    /// 时序保证同 PUT:数据先落盘、分片记录后提交;失败回滚已暂存分配。
    pub fn upload_part(
        &mut self,
        upload_id: &str,
        part_no: u32,
        reader: &mut dyn Read,
    ) -> Result<PartMeta> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        // 与 PUT 一致:读前缀判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            prefix.extend_from_slice(&buf[..n]);
            if prefix.len() > limit {
                break;
            }
        }
        let mtime = now_ts();
        let (size, etag, extents, inline) = if prefix.len() <= limit {
            let etag: [u8; 16] = md5::Md5::digest(&prefix).into();
            (prefix.len() as u64, etag, Vec::new(), Some(prefix))
        } else {
            let mut prefixed = PrefixedReader {
                prefix,
                pos: 0,
                inner: reader,
            };
            let (extents, size, etag) = self.stream_to_extents(&mut prefixed)?;
            (size, etag, extents, None)
        };
        let part = PartMeta {
            size,
            etag,
            mtime,
            extents,
            inline,
        };
        // 分片重传会清 completed 标记(reactivate;resend_first_finishes_last)
        let draft = self.alloc.take_draft();
        let seq = self
            .meta
            .put_part(upload_id, part_no, &part, to_alloc_draft(&draft));
        match seq {
            Ok(_) => {
                self.alloc.confirm_draft();
                if self
                    .meta
                    .get_multipart(upload_id)?
                    .map(|s| s.completed)
                    .unwrap_or(false)
                {
                    self.meta.touch_multipart(upload_id)?;
                }
                self.maybe_checkpoint()?;
                Ok(part)
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// 分片复制(UploadPartCopy):源对象 range 直灌分片流水线(边读边写,
    /// 无整段内存缓冲);返回分片元数据(ETag = 复制字节的 MD5)。
    pub fn upload_part_copy(
        &mut self,
        upload_id: &str,
        part_no: u32,
        src_bucket: &str,
        src_key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<PartMeta> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let src = self
            .meta
            .get_object(src_bucket, src_key)?
            .ok_or_else(|| Error::NotFound(format!("object {src_bucket}/{src_key}")))?;
        let start = range.start.min(src.size);
        let end = range.end.min(src.size);
        if start >= end {
            return Err(Error::InvalidArgument("copy source range is empty".into()));
        }
        let len = end - start;
        let mut writer = ExtentWriter::new(self.chunk_size)?;
        // 内联源:直接灌入
        if let Some(inline) = &src.inline {
            let data = &inline[start as usize..end as usize];
            writer.feed(self, data)?;
        } else {
            // extent 源:逐段读取(4KiB 对齐裁剪)直灌
            let mut obj_pos = 0u64;
            let mut remain = len;
            for ext in &src.extents {
                if remain == 0 {
                    break;
                }
                let ext_begin = obj_pos;
                let ext_end = obj_pos + ext.len as u64;
                obj_pos = ext_end;
                let seg_start = ext_begin.max(start);
                let seg_end = ext_end.min(end);
                if seg_start >= seg_end {
                    continue;
                }
                let payload_off = seg_start - ext_begin;
                let dev_off = self.extent_data_offset(ext.extent_id) + payload_off;
                let mut done = 0usize;
                let seg_len = (seg_end - seg_start) as usize;
                while done < seg_len {
                    let cur_off = dev_off + done as u64;
                    let block_off = cur_off - (cur_off % SECTOR_SIZE);
                    let skip = (cur_off - block_off) as usize;
                    let want = (seg_len - done + skip).min(self.chunk_size);
                    let block_len = align_up(want as u64, SECTOR_SIZE) as usize;
                    let mut rbuf = fs3_device::AlignedBuffer::new(block_len)?;
                    read_exact(
                        &mut **self.io.lock().unwrap(),
                        self.device.raw_fd(),
                        rbuf.as_mut_slice(),
                        block_off,
                    )?;
                    let usable = &rbuf.as_slice()[skip..skip + (want - skip).min(seg_len - done)];
                    writer.feed(self, usable)?;
                    done += usable.len();
                    remain -= usable.len() as u64;
                }
            }
            debug_assert_eq!(remain, 0);
        }
        let (extents, size, etag) = writer.finish(self)?;
        debug_assert_eq!(size, len);
        let part = PartMeta {
            size,
            etag,
            mtime: now_ts(),
            extents,
            inline: None,
        };
        let draft = self.alloc.take_draft();
        match self
            .meta
            .put_part(upload_id, part_no, &part, to_alloc_draft(&draft))
        {
            Ok(_) => {
                self.alloc.confirm_draft();
                if self
                    .meta
                    .get_multipart(upload_id)?
                    .map(|s| s.completed)
                    .unwrap_or(false)
                {
                    self.meta.touch_multipart(upload_id)?;
                }
                self.maybe_checkpoint()?;
                Ok(part)
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// 完成上传:校验分片(存在 + ETag + 顺序 + 大小)→ 零数据搬运组合
    /// (extent 列表按序拼接;全内联则拼数据;混合走数据路径)。
    /// 返回最终对象元数据;二次 Complete 幂等返回(completed 快照)。
    pub fn complete_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        client_parts: &[(u32, String)],
    ) -> Result<ObjectMeta> {
        let session = self
            .meta
            .get_multipart(upload_id)?
            .ok_or_else(|| Error::NoSuchUpload(upload_id.to_string()))?;
        if client_parts.is_empty() {
            return Err(Error::InvalidArgument("empty parts list".into()));
        }
        // 幂等:已 completed 且无分片记录 → 重放当前对象
        if session.completed && self.meta.list_parts(upload_id)?.is_empty() {
            if let Some(m) = self.meta.get_object(bucket, key)? {
                return Ok(m);
            }
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }

        let stored = self.meta.list_parts(upload_id)?;
        let mut by_no: std::collections::HashMap<u32, PartMeta> = std::collections::HashMap::new();
        for (no, p) in &stored {
            by_no.insert(*no, p.clone());
        }
        // 客户端列表按 part_no 建图(同名多次出现取最后,RGW 语义;
        // s3-tests test_multipart_resend_first_finishes_last 依赖),
        // 再按 part_no 升序校验:存在 + ETag 匹配。
        let mut client_map: std::collections::BTreeMap<u32, &str> =
            std::collections::BTreeMap::new();
        for (no, etag_hex) in client_parts {
            if *no == 0 || *no > fs3_core::MAX_PARTS {
                return Err(Error::InvalidPart(format!("part number {no} out of range")));
            }
            client_map.insert(*no, etag_hex);
        }
        let mut total: u64 = 0;
        let mut max_no = 0u32;
        for (no, etag_hex) in &client_map {
            max_no = max_no.max(*no);
            let stored_meta = by_no
                .get(no)
                .ok_or_else(|| Error::InvalidPart(format!("part {no} not found")))?;
            if !stored_meta.etag_hex().eq_ignore_ascii_case(etag_hex) {
                return Err(Error::InvalidPart(format!(
                    "part {no} etag mismatch (stored {}, given {etag_hex})",
                    stored_meta.etag_hex()
                )));
            }
            total += stored_meta.size;
        }
        if total > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        // 非最后分片 ≥ 5MiB(AWS EntityTooSmall)
        for (no, p) in &stored {
            if *no < max_no && p.size < fs3_core::MIN_PART_SIZE {
                return Err(Error::PartTooSmall(format!(
                    "part {no} size {} < {}",
                    p.size,
                    fs3_core::MIN_PART_SIZE
                )));
            }
        }

        // 组合策略
        let all_inline = stored.iter().all(|(_, p)| p.inline.is_some());
        let all_extent = stored.iter().all(|(_, p)| p.inline.is_none());
        let total_size = total;
        let part_sizes: Vec<u64> = (0..max_no)
            .map(|i| by_no.get(&(i + 1)).map(|p| p.size).unwrap_or(0))
            .collect();
        let etag: [u8; 16] = {
            let mut concat = String::new();
            for (no, p) in &stored {
                if *no <= max_no {
                    concat.push_str(&p.etag_hex());
                }
            }
            md5::Md5::digest(concat.as_bytes()).into()
        };
        let mtime = now_ts();

        let old = self.meta.get_object(bucket, key)?;
        let old_ids: Vec<u64> = old
            .as_ref()
            .map(|o| o.extents.iter().map(|e| e.extent_id).collect())
            .unwrap_or_default();

        let (meta, extra_ref_dec) = if all_inline && total_size <= self.small_object_limit as u64 {
            // 全内联:拼接数据,零设备 I/O
            let mut data = Vec::with_capacity(total_size as usize);
            for (no, p) in &stored {
                if *no <= max_no {
                    if let Some(d) = &p.inline {
                        data.extend_from_slice(d);
                    }
                }
            }
            (
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents: Vec::new(),
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: Some(data),
                    parts: part_sizes,
                },
                Vec::new(),
            )
        } else if all_extent {
            // 零数据搬运:extent 列表按序拼接(所有权从分片转移给对象)
            let mut extents: Vec<ExtentRef> = Vec::new();
            for (no, p) in &stored {
                if *no <= max_no {
                    extents.extend_from_slice(&p.extents);
                }
            }
            (
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents,
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: None,
                    parts: part_sizes,
                },
                Vec::new(),
            )
        } else {
            // 混合(小分片 + 大分片):数据路径组合
            let mut sink = Vec::with_capacity(total_size.min(64 * 1024 * 1024) as usize);
            for (no, p) in &stored {
                if *no <= max_no {
                    self.read_part_to(p, &mut sink)?;
                }
            }
            let (extents, size, _) = self.stream_to_extents(&mut std::io::Cursor::new(sink))?;
            debug_assert_eq!(size, total_size);
            let mut part_ids: Vec<u64> = Vec::new();
            for (no, p) in &stored {
                if *no <= max_no {
                    part_ids.extend(p.extents.iter().map(|e| e.extent_id));
                }
            }
            (
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents,
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: None,
                    parts: part_sizes,
                },
                part_ids,
            )
        };

        // 释放旧对象 + (混合路径)分片 extent
        if !old_ids.is_empty() {
            self.alloc.release(&old_ids);
        }
        if !extra_ref_dec.is_empty() {
            self.alloc.release(&extra_ref_dec);
        }
        let draft = self.alloc.take_draft();
        let part_keys: Vec<Vec<u8>> = stored
            .iter()
            .map(|(no, _)| part_key(upload_id, *no))
            .collect();
        let delta = StatsDelta {
            objects: if old.is_some() { 0 } else { 1 },
            bytes: total_size as i64 - old.as_ref().map(|o| o.size as i64).unwrap_or(0),
        };
        match self.meta.complete_multipart(
            bucket,
            key,
            upload_id,
            &meta,
            &part_keys,
            to_alloc_draft(&draft),
            delta,
        ) {
            Ok(_) => {
                self.alloc.confirm_draft();
                self.maybe_checkpoint()?;
                Ok(meta)
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// 中止上传:删除会话与全部分片,释放 extent(204)。
    pub fn abort_multipart(&mut self, upload_id: &str) -> Result<()> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let parts = self.meta.list_parts(upload_id)?;
        let mut ids: Vec<u64> = Vec::new();
        for (_, p) in &parts {
            ids.extend(p.extents.iter().map(|e| e.extent_id));
        }
        if !ids.is_empty() {
            self.alloc.release(&ids);
        }
        let draft = self.alloc.take_draft();
        let part_keys: Vec<Vec<u8>> = parts
            .iter()
            .map(|(no, _)| part_key(upload_id, *no))
            .collect();
        match self
            .meta
            .abort_multipart(upload_id, &part_keys, to_alloc_draft(&draft))
        {
            Ok(_) => {
                self.alloc.confirm_draft();
                self.maybe_checkpoint()?;
                Ok(())
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    /// 列出分片(ListParts)。
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartMeta)>> {
        self.meta.list_parts(upload_id)
    }

    /// 桶内未完成上传(ListMultipartUploads)。
    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max: usize,
    ) -> Result<Vec<(String, MultipartSession)>> {
        self.meta
            .list_bucket_sessions(bucket, prefix, key_marker.zip(upload_id_marker), max)
    }

    /// 过期会话回收(默认 7 天;启动与周期任务调用)。
    pub fn sweep_expired_sessions(&mut self, ttl_secs: i64) -> Result<usize> {
        let now = now_ts();
        let expired: Vec<String> = self
            .meta
            .list_all_sessions()?
            .into_iter()
            .filter(|(_, s)| now - s.created >= ttl_secs)
            .map(|(uid, _)| uid)
            .collect();
        let mut n = 0usize;
        for uid in expired {
            if self.abort_multipart(&uid).is_ok() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// 分片数据读出(内联直接拷贝;extent 按段读取)。
    fn read_part_to(&mut self, part: &PartMeta, out: &mut dyn Write) -> Result<()> {
        if let Some(inline) = &part.inline {
            out.write_all(inline)?;
            return Ok(());
        }
        let mut written = 0u64;
        for ext in &part.extents {
            let dev_off = self.extent_data_offset(ext.extent_id);
            let mut done = 0usize;
            let len = ext.len as usize;
            while done < len {
                let cur_off = dev_off + done as u64;
                let block_off = cur_off - (cur_off % SECTOR_SIZE);
                let skip = (cur_off - block_off) as usize;
                let want = (len - done + skip).min(self.chunk_size);
                let block_len = align_up(want as u64, SECTOR_SIZE) as usize;
                let mut rbuf = fs3_device::AlignedBuffer::new(block_len)?;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device.raw_fd(),
                    rbuf.as_mut_slice(),
                    block_off,
                )?;
                let usable = &rbuf.as_slice()[skip..skip + (want - skip).min(len - done)];
                out.write_all(usable)?;
                done += usable.len();
                written += usable.len() as u64;
            }
        }
        debug_assert_eq!(written, part.size);
        Ok(())
    }

    // ─────────────────────────── CopyObject(F6,COW) ───────────────────────────

    /// 服务端复制:同设备 = 元数据操作(extent 引用计数 +1,零数据 I/O)。
    /// `REPLACE` 指令传新 content_type/user_meta;`COPY` 传 None(沿用源)。
    pub fn copy_object(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        replace_content_type: Option<&str>,
        replace_user_meta: Option<&[(String, String)]>,
    ) -> Result<ObjectMeta> {
        let src = self
            .meta
            .get_object(src_bucket, src_key)?
            .ok_or_else(|| Error::NotFound(format!("object {src_bucket}/{src_key}")))?;
        let old = self.meta.get_object(dst_bucket, dst_key)?;
        let old_ids: Vec<u64> = old
            .as_ref()
            .map(|o| o.extents.iter().map(|e| e.extent_id).collect())
            .unwrap_or_default();

        let mut meta = src.clone();
        meta.mtime = now_ts();
        if let Some(ct) = replace_content_type {
            meta.content_type = ct.to_string();
        }
        if let Some(um) = replace_user_meta {
            meta.user_meta = um.to_vec();
        }
        // 源为内联 → 数据拷贝进新内联;否则共享 extent 列表(ref_inc)
        if src.inline.is_none() {
            self.alloc
                .inc_ref(&meta.extents.iter().map(|e| e.extent_id).collect::<Vec<_>>());
        }
        if !old_ids.is_empty() {
            self.alloc.release(&old_ids);
        }
        let draft = self.alloc.take_draft();
        let delta = StatsDelta {
            objects: if old.is_some() { 0 } else { 1 },
            bytes: meta.size as i64 - old.as_ref().map(|o| o.size as i64).unwrap_or(0),
        };
        match self
            .meta
            .commit_object_put(dst_bucket, dst_key, &meta, to_alloc_draft(&draft), delta)
        {
            Ok(_) => {
                self.alloc.confirm_draft();
                self.maybe_checkpoint()?;
                Ok(meta)
            }
            Err(e) => {
                self.alloc.rollback_draft(&draft);
                Err(e)
            }
        }
    }

    // ─────────────────────────── 零拷贝读路径(B3/D2) ───────────────────────────

    /// 对象数据段(设备偏移 + 长度),裁剪到 [offset, offset+length) 响应区间;
    /// 内联/空对象返回 Some(vec![])。零拷贝读路径用(B3/D2)。
    pub fn object_segments(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<Segment>>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        if meta.inline.is_some() || meta.extents.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let start = offset.min(meta.size);
        let end = (offset + length).min(meta.size);
        let mut segs = Vec::new();
        let mut obj_pos = 0u64;
        for ext in &meta.extents {
            let ext_begin = obj_pos;
            let ext_end = obj_pos + ext.len as u64;
            obj_pos = ext_end;
            let s = ext_begin.max(start);
            let e = ext_end.min(end);
            if s >= e {
                continue;
            }
            segs.push(Segment {
                dev_offset: self.extent_data_offset(ext.extent_id) + (s - ext_begin),
                len: e - s,
            });
        }
        Ok(Some(segs))
    }

    /// 设备 fd(零拷贝 sendfile/splice 用)。
    pub fn device_fd(&self) -> i32 {
        self.device.raw_fd()
    }

    /// 读校验开关(开启时禁零拷贝)。
    pub fn verify_reads_enabled(&self) -> bool {
        self.verify_reads
    }

    /// 零拷贝 fd(sendfile/splice;None = 不可用)。
    pub fn zc_fd(&self) -> Option<i32> {
        self.zc_fd
    }

    /// 设备是否为普通文件(决定 sendfile vs splice)。
    pub fn device_is_file(&self) -> bool {
        self.device.is_file()
    }

    // ─────────────────────────── CHECK ───────────────────────────

    /// 只读一致性摘要(位图 vs 元数据核对;泄漏修复留 M3 check 工具)。
    pub fn check_report(&self) -> Result<CheckReport> {
        let leaks = self.alloc.leaks();
        let buckets = self.meta.list_buckets()?;
        let mut objects = 0usize;
        let mut total_bytes = 0u64;
        for (name, _) in &buckets {
            for (_, m) in self.meta.list_objects(name, "")? {
                objects += 1;
                total_bytes += m.size;
            }
        }
        let last_seq = self.meta.last_seq()?;
        let cp_seq = self.checkpoint.lock().unwrap().seq;
        Ok(CheckReport {
            device: self.device.path().display().to_string(),
            device_capacity: self.device.capacity(),
            extent_size: self.sb.extent_size,
            extent_count: self.sb.extent_count(),
            allocated_extents: self.alloc.allocated_count(),
            buckets: buckets.len(),
            objects,
            total_bytes,
            leaks,
            io_engine: self.io.lock().unwrap().name(),
            checkpoint_seq: cp_seq,
            last_seq,
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close();
        }
    }
}

/// put_stream 参数包(避免超长参数列表)。
struct PutCtx<'a> {
    bucket: &'a str,
    key: &'a str,
    reader: &'a mut dyn Read,
    old_size: i64,
    old_ids: Vec<u64>,
    content_type: Option<&'a str>,
    user_meta: Vec<(String, String)>,
}

/// 前缀 + 原 reader 拼接(内联判定后的流续接)。
struct PrefixedReader<'a> {
    prefix: Vec<u8>,
    pos: usize,
    inner: &'a mut dyn Read,
}

impl Read for PrefixedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }
        self.inner.read(buf)
    }
}

/// 零拷贝读段(设备偏移 + 长度;B3/D2)。
#[derive(Debug, Clone, Copy)]
pub struct Segment {
    pub dev_offset: u64,
    pub len: u64,
}

/// 进行中的 extent 写状态。
struct WriteState {
    extent_id: u64,
    /// 数据区已写字节(不含 4KiB 头)。
    written: u32,
    chunk_crcs: Vec<u32>,
}

/// extent 流水线状态机:feed(数据块,引擎借用) → finish(extents, size, md5)。
/// 供普通 PUT(reader 循环)与 UploadPartCopy(源段直灌)复用。
struct ExtentWriter {
    chunk_size: usize,
    capacity: u64,
    acc: fs3_device::AlignedBuffer,
    hasher: md5::Md5,
    fill: usize,
    st: Option<WriteState>,
    extents: Vec<ExtentRef>,
    size: u64,
    owner_id: [u8; 16],
}

impl ExtentWriter {
    fn new(chunk_size: usize) -> Result<Self> {
        let mut owner_id = [0u8; 16];
        random_bytes(&mut owner_id)?;
        Ok(ExtentWriter {
            chunk_size,
            capacity: 0, // feed 首轮经 ensure_extent 设置
            acc: fs3_device::AlignedBuffer::new(chunk_size)?,
            hasher: md5::Md5::new(),
            fill: 0,
            st: None,
            extents: Vec::new(),
            size: 0,
            owner_id,
        })
    }

    fn feed(&mut self, engine: &mut Engine, data: &[u8]) -> Result<()> {
        if self.capacity == 0 {
            self.capacity = engine.sb.extent_capacity();
        }
        let chunk_size = self.chunk_size;
        let capacity = self.capacity;
        let mut off = 0usize;
        let n = data.len();
        while off < n {
            // extent 满(或尚无)→ 封口 + 申请新 extent
            if self.st.is_none() || self.st.as_ref().unwrap().written as u64 >= capacity {
                if let Some(mut s) = self.st.take() {
                    engine.flush_extent_chunk(
                        &mut s,
                        &mut self.acc,
                        &mut self.fill,
                        &mut self.hasher,
                    )?;
                    self.extents.push(engine.finalize_extent(
                        &s,
                        self.owner_id,
                        chunk_size as u32,
                    )?);
                }
                let id = engine.alloc.allocate(1)?.remove(0);
                engine.note_alloc(1);
                self.st = Some(WriteState {
                    extent_id: id,
                    written: 0,
                    chunk_crcs: Vec::new(),
                });
            }
            let need_flush = {
                let s = self.st.as_ref().unwrap();
                self.fill == chunk_size || s.written as u64 + self.fill as u64 >= capacity
            };
            if need_flush {
                let s = self.st.as_mut().unwrap();
                engine.flush_extent_chunk(s, &mut self.acc, &mut self.fill, &mut self.hasher)?;
                continue; // 重新评估(extent 可能恰好写满)
            }
            let s = self.st.as_mut().unwrap();
            // 剩余空间必须扣除 acc 中已积累但未落盘的 fill 字节,
            // 否则跨输入 chunk 的累积会越过 extent 容量写坏下一个 extent 头
            let space = (capacity - s.written as u64 - self.fill as u64) as usize;
            let take = (n - off).min(space).min(chunk_size - self.fill);
            self.acc.as_mut_slice()[self.fill..self.fill + take]
                .copy_from_slice(&data[off..off + take]);
            self.fill += take;
            off += take;
        }
        self.size += n as u64;
        if self.size > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        Ok(())
    }

    /// 流结束:flush 剩余 chunk,封口最后 extent;返回 (extents, size, md5)。
    fn finish(mut self, engine: &mut Engine) -> Result<(Vec<ExtentRef>, u64, [u8; 16])> {
        if let Some(mut s) = self.st.take() {
            engine.flush_extent_chunk(&mut s, &mut self.acc, &mut self.fill, &mut self.hasher)?;
            self.extents
                .push(engine.finalize_extent(&s, self.owner_id, self.chunk_size as u32)?);
        }
        // sync_mode=full:数据 fsync 后再提交元数据
        if engine.meta.sync_mode() == SyncMode::Full {
            fsync(&mut **engine.io.lock().unwrap(), engine.device.raw_fd())?;
        }
        let etag: [u8; 16] = self.hasher.finalize().into();
        Ok((self.extents, self.size, etag))
    }
}

/// 读取至多 buf.len() 字节(处理 Read 短读)。
fn read_up_to(r: &mut dyn Read, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = r.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn to_alloc_draft(staged: &fs3_alloc::Staged) -> AllocDraft {
    AllocDraft {
        alloc: staged.alloc.clone(),
        ref_inc: staged.ref_inc.clone(),
        ref_dec: staged.ref_dec.clone(),
    }
}

/// 引用计数重建:扫描全部对象元数据,统计每个 extent 的被引用次数;
/// 返回"位图已分配但元数据不可达"的泄漏列表(只报告,不回收)。
fn rebuild_refcounts(meta: &MetaStore, alloc: &Allocator) -> Vec<u64> {
    let mut counts: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
    let mut seen_extents = 0usize;
    if let Ok(buckets) = meta.list_buckets() {
        for (name, _) in buckets {
            if let Ok(objects) = meta.list_objects(&name, "") {
                for (_, m) in objects {
                    for e in &m.extents {
                        *counts.entry(e.extent_id).or_insert(0) += 1;
                        seen_extents += 1;
                    }
                }
            }
        }
    }
    for (id, n) in &counts {
        alloc.set_refcount(*id, *n);
    }
    tracing::debug!(
        "refcount rebuild: {} extents referenced by metadata",
        seen_extents
    );
    alloc.leaks()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::Path;

    fn test_cfg(dev: &Path, meta_dir: &Path) -> EngineConfig {
        EngineConfig {
            device: dev.to_path_buf(),
            meta_dir: meta_dir.to_path_buf(),
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

    #[test]
    fn put_get_delete_roundtrip() {
        let (_d, cfg) = setup();
        let mut e = open_engine(&cfg);

        // 空对象
        let m = e.put("b1", "empty", &mut Cursor::new(Vec::new())).unwrap();
        assert_eq!(m.size, 0);
        assert_eq!(m.extents.len(), 0);

        // 小对象(单 chunk 内)
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let m = e
            .put("b1", "small", &mut Cursor::new(data.clone()))
            .unwrap();
        assert_eq!(m.size, data.len() as u64);
        assert_eq!(m.extents.len(), 1);
        let mut out = Vec::new();
        e.get_to("b1", "small", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);

        // 大对象(跨 extent:4MiB extent,数据容量 4MiB-4KiB)
        let big: Vec<u8> = (0..(5 * 1024 * 1024u32)).map(|i| (i % 253) as u8).collect();
        let m = e.put("b1", "big", &mut Cursor::new(big.clone())).unwrap();
        assert_eq!(m.size, big.len() as u64);
        assert!(m.extents.len() >= 2, "expected >=2 extents");
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
    fn overwrite_releases_old_extents() {
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
        // 未提交事务:对象不可见,extent 全部回滚
        assert!(e.head("b1", "partial").unwrap().is_none());
        assert_eq!(e.allocator().allocated_count(), 0);
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
    fn recovery_without_close_rolls_forward() {
        let (_d, cfg) = setup();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        {
            let mut e = open_engine(&cfg);
            e.put("b1", "a", &mut Cursor::new(data.clone())).unwrap();
            // 显式 flush 使 sled 落盘(模拟组提交窗口已过)
            e.meta().flush().unwrap();
            e.abort(); // 模拟 kill -9:跳过最终检查点
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
        } // 释放 sled 锁

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
        assert_eq!(out, data, "3-extent object must roundtrip exactly");
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

        // 分片 1:3MiB(内联阈值 32KiB 之上 → extent);分片 2:小内联
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

    /// 零数据搬运:全部大分片 extent 直接拼接(对象 extent 引用 == 分片之和)。
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
        let ext_id = src.extents[0].extent_id;

        // COW 复制:共享 extent,无新分配
        let before = e.alloc.allocated_count();
        let c = e.copy_object("b1", "src", "b1", "dst", None, None).unwrap();
        assert_eq!(e.alloc.allocated_count(), before);
        assert_eq!(c.extents, src.extents);
        // 内容一致
        let mut out = Vec::new();
        e.get_to("b1", "dst", 0..c.size, &mut out).unwrap();
        assert_eq!(out, data);
        // 引用计数 = 2
        let cnt = e.alloc.refcount(ext_id);
        assert_eq!(cnt, 2);
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
}
