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
use fs3_meta::{AllocDraft, MetaConfig, MetaStore, StatsDelta, SyncMode};

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
    sb: fs3_core::SuperBlock,
    alloc: Arc<Allocator>,
    meta: MetaStore,
    io: Box<dyn IoEngine>,
    chunk_size: usize,
    verify_reads: bool,
    read_only: bool,
    small_object_limit: usize,
    checkpoint: std::sync::Mutex<CheckpointState>,
    checkpoint_tick: Receiver<()>,
    _checkpoint_thread: Option<std::thread::JoinHandle<()>>,
    closed: bool,
}

impl Engine {
    /// 打开引擎(含完整恢复流程);设备未初始化返回 NotInitialized。
    pub fn open(cfg: &EngineConfig) -> Result<Self> {
        let device = open_device(&cfg.device, cfg.read_only)?;
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
            device,
            sb,
            alloc,
            meta,
            io,
            chunk_size: fs3_core::DEFAULT_CHUNK_SIZE,
            verify_reads: cfg.verify_reads,
            read_only: cfg.read_only,
            small_object_limit: cfg.small_object_limit,
            checkpoint: std::sync::Mutex::new(CheckpointState {
                seq: cp.seq.max(last_seq),
                alloc_since: 0,
                dirty: false,
            }),
            checkpoint_tick: rx,
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
        self.io.name()
    }

    /// 每个写操作后调用:处理检查点定时 tick 与分配增量触发。
    fn maybe_checkpoint(&mut self) -> Result<()> {
        let due = {
            let mut st = self.checkpoint.lock().unwrap();
            let tick = matches!(self.checkpoint_tick.try_recv(), Ok(()));
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
        let mut owner_id = [0u8; 16];
        random_bytes(&mut owner_id)?;
        let mut prefixed = PrefixedReader {
            prefix,
            pos: 0,
            inner: reader,
        };
        let result = self.put_stream(PutCtx {
            bucket,
            key,
            reader: &mut prefixed,
            owner_id,
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
            owner_id,
            old_size,
            old_ids,
            content_type,
            user_meta,
        } = ctx;
        let chunk_size = self.chunk_size;
        let mut inbuf = fs3_device::AlignedBuffer::new(chunk_size)?;
        let mut acc = fs3_device::AlignedBuffer::new(chunk_size)?;
        let mut hasher = md5::Md5::new();
        let mut fill: usize = 0; // acc 内已积累字节
        let capacity = self.sb.extent_capacity();

        let mut extents: Vec<ExtentRef> = Vec::new();
        let mut st: Option<WriteState> = None;
        let mut size: u64 = 0;

        loop {
            let n = read_up_to(reader, inbuf.as_mut_slice())?;
            if n == 0 {
                break;
            }
            let mut off = 0usize;
            while off < n {
                // extent 满(或尚无)→ 封口 + 申请新 extent
                if st.is_none() || st.as_ref().unwrap().written as u64 >= capacity {
                    if let Some(mut s) = st.take() {
                        // 写满时 fill 必为 0(见下方 flush 条件);此处防御性 flush
                        self.flush_extent_chunk(&mut s, &mut acc, &mut fill, &mut hasher)?;
                        extents.push(self.finalize_extent(&s, owner_id, chunk_size as u32)?);
                    }
                    let id = self.alloc.allocate(1)?.remove(0);
                    self.note_alloc(1);
                    st = Some(WriteState {
                        extent_id: id,
                        written: 0,
                        chunk_crcs: Vec::new(),
                    });
                }
                let need_flush = {
                    let s = st.as_ref().unwrap();
                    fill == chunk_size || s.written as u64 + fill as u64 >= capacity
                };
                if need_flush {
                    let s = st.as_mut().unwrap();
                    self.flush_extent_chunk(s, &mut acc, &mut fill, &mut hasher)?;
                    continue; // 重新评估(extent 可能恰好写满)
                }
                let s = st.as_mut().unwrap();
                // 剩余空间必须扣除 acc 中已积累但未落盘的 fill 字节,
                // 否则跨输入 chunk 的累积会越过 extent 容量写坏下一个 extent 头
                let space = (capacity - s.written as u64 - fill as u64) as usize;
                let take = (n - off).min(space).min(chunk_size - fill);
                acc.as_mut_slice()[fill..fill + take]
                    .copy_from_slice(&inbuf.as_slice()[off..off + take]);
                fill += take;
                off += take;
            }
            size += n as u64;
            if size > fs3_core::MAX_OBJECT_SIZE {
                return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
            }
        }
        // 流结束:flush 剩余 chunk,封口最后 extent(空对象无 extent)
        if let Some(mut s) = st.take() {
            self.flush_extent_chunk(&mut s, &mut acc, &mut fill, &mut hasher)?;
            extents.push(self.finalize_extent(&s, owner_id, chunk_size as u32)?);
        }

        // sync_mode=full:数据 fsync 后再提交元数据
        if self.meta.sync_mode() == SyncMode::Full {
            fsync(&mut *self.io, self.device.raw_fd())?;
        }

        let mtime = now_ts();
        let etag: [u8; 16] = hasher.finalize().into();
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
        match self
            .meta
            .commit_object_put(bucket, key, &meta, to_alloc_draft(&draft), delta)
        {
            Ok(_) => Ok(meta),
            Err(e) => Err(e),
        }
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
            &mut *self.io,
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
        write_all(&mut *self.io, self.device.raw_fd(), hbuf.as_slice(), off)?;
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
                        &mut *self.io,
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
            &mut *self.io,
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
                &mut *self.io,
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
                    &mut *self.io,
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
            io_engine: self.io.name(),
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
    owner_id: [u8; 16],
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

/// 进行中的 extent 写状态。
struct WriteState {
    extent_id: u64,
    /// 数据区已写字节(不含 4KiB 头)。
    written: u32,
    chunk_crcs: Vec<u32>,
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
}
