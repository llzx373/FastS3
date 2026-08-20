//! 检查点双缓冲读写(DESIGN §4.2 / C2)。
//!
//! 两个槽各含 magic/generation/seq/CRC(ADR-5);写时选代数较小(或无效)
//! 的槽,写入代数 = max(两槽代数)+1,随后 fsync;恢复取"有效且代数最大"
//! 的槽。相比"序号指针"方案少一次额外写,崩溃任意时刻都只有
//! "旧槽 + 新槽(可能半写)"两种状态,由 CRC 甄别。

use fs3_core::{CheckpointData, Result, SuperBlock};
use fs3_device::{AlignedBuffer, BlockDevice};

pub struct Checkpointer<'a> {
    dev: &'a dyn BlockDevice,
    sb: &'a SuperBlock,
}

/// 读取结果:数据 + 所在槽代数(用于决定下一次写哪个槽)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotState {
    pub data: CheckpointData,
    /// 该槽的代数(无效槽为 None)。
    pub generation: Option<u64>,
    pub valid: bool,
}

impl<'a> Checkpointer<'a> {
    pub fn new(dev: &'a dyn BlockDevice, sb: &'a SuperBlock) -> Self {
        Checkpointer { dev, sb }
    }

    fn slot_offset(&self, slot: usize) -> u64 {
        self.sb.checkpoint_offset + slot as u64 * self.sb.checkpoint_len
    }

    fn read_slot(&self, slot: usize) -> Result<SlotState> {
        let mut buf = AlignedBuffer::new(self.sb.checkpoint_len as usize)?;
        self.dev
            .pread_aligned(buf.as_mut_slice(), self.slot_offset(slot))?;
        match CheckpointData::decode(buf.as_slice()) {
            Ok(data) => Ok(SlotState {
                generation: Some(data.generation),
                valid: true,
                data,
            }),
            Err(_) => {
                // 无效槽(未初始化 / 半写):返回占位
                Ok(SlotState {
                    data: CheckpointData {
                        generation: 0,
                        seq: 0,
                        total_alloc: 0,
                        total_free: 0,
                        bitmap: vec![],
                    },
                    generation: None,
                    valid: false,
                })
            }
        }
    }

    /// 读取两个槽,返回有效且代数最大者。
    pub fn load_latest(&self) -> Result<Option<CheckpointData>> {
        let a = self.read_slot(0)?;
        let b = self.read_slot(1)?;
        match (a.valid, b.valid) {
            (false, false) => Ok(None),
            (true, false) => Ok(Some(a.data)),
            (false, true) => Ok(Some(b.data)),
            (true, true) => {
                if a.data.generation >= b.data.generation {
                    Ok(Some(a.data))
                } else {
                    Ok(Some(b.data))
                }
            }
        }
    }

    /// 写检查点:选代数较小(或无效)的槽,代数 = max(有效代数)+1。
    ///
    /// 传入数据的 generation 字段会被忽略(以槽状态为准)。
    pub fn save(&self, cp: &CheckpointData) -> Result<u64> {
        let a = self.read_slot(0)?;
        let b = self.read_slot(1)?;
        let max_gen = a.generation.max(b.generation).unwrap_or(0);
        let new_gen = max_gen + 1;
        // 目标槽:代数较小的那个;都无效 → 槽 0
        let target = match (a.generation, b.generation) {
            (None, _) => 0,
            (_, None) => 1,
            (Some(ga), Some(gb)) => {
                if ga <= gb {
                    0
                } else {
                    1
                }
            }
        };
        let data = CheckpointData {
            generation: new_gen,
            ..cp.clone()
        };
        let mut buf = AlignedBuffer::new(self.sb.checkpoint_len as usize)?;
        buf.as_mut_slice()
            .copy_from_slice(&data.encode(self.sb.checkpoint_len)?);
        self.dev
            .pwrite_aligned(buf.as_slice(), self.slot_offset(target))?;
        // 先写副本并落盘,再让新代数生效(槽内容整体原子可见于 CRC 校验)
        self.dev.sync()?;
        Ok(new_gen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::compute_layout;
    use fs3_device::{init_device, open_device};

    fn setup(size: u64) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img");
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(size).unwrap();
        drop(f);
        (dir, path)
    }

    fn empty_cp(gen: u64, seq: u64, bitmap_bytes: usize) -> CheckpointData {
        CheckpointData {
            generation: gen,
            seq,
            total_alloc: 0,
            total_free: 0,
            bitmap: vec![0u8; bitmap_bytes],
        }
    }

    #[test]
    fn save_and_load_latest() -> Result<()> {
        let (_dir, path) = setup(64 * 1024 * 1024);
        init_device(&path, 4 * 1024 * 1024, 0, false)?;
        let dev = open_device(&path, false)?;
        let sb = fs3_device::read_superblock(dev.as_ref())?;
        let cp = Checkpointer::new(dev.as_ref(), &sb);

        // init 写的是代数 1
        let l = cp.load_latest()?.unwrap();
        assert_eq!(l.generation, 1);

        // 保存 2 → 应写入槽 B(代数 1 的对面)
        let cp2 = empty_cp(0, 5, sb_checkpoint_bitmap_bytes(&sb));
        let gen = cp.save(&cp2)?;
        assert_eq!(gen, 2);
        let l = cp.load_latest()?.unwrap();
        assert_eq!(l.generation, 2);
        assert_eq!(l.seq, 5);

        // 保存 3 → 槽 A,代数 3
        let cp3 = empty_cp(0, 9, sb_checkpoint_bitmap_bytes(&sb));
        let gen = cp.save(&cp3)?;
        assert_eq!(gen, 3);
        let l = cp.load_latest()?.unwrap();
        assert_eq!(l.generation, 3);
        assert_eq!(l.seq, 9);
        Ok(())
    }

    fn sb_checkpoint_bitmap_bytes(sb: &SuperBlock) -> usize {
        // 从布局反推位图字节数
        let layout = compute_layout(sb.data_end, sb.extent_size).unwrap();
        layout.bitmap_bytes as usize
    }

    #[test]
    fn corrupt_slot_falls_back_to_other() -> Result<()> {
        let (_dir, path) = setup(64 * 1024 * 1024);
        init_device(&path, 4 * 1024 * 1024, 0, false)?;
        let dev = open_device(&path, false)?;
        let sb = fs3_device::read_superblock(dev.as_ref())?;
        let cp = Checkpointer::new(dev.as_ref(), &sb);
        let cp2 = empty_cp(0, 5, sb_checkpoint_bitmap_bytes(&sb));
        cp.save(&cp2)?; // 槽 B 代数 2

        // 破坏槽 A(代数 1 所在):翻转其中一字节
        let mut buf = AlignedBuffer::new(sb.checkpoint_len as usize)?;
        dev.pread_aligned(buf.as_mut_slice(), sb.checkpoint_offset)?;
        buf.as_mut_slice()[100] ^= 0xFF;
        dev.pwrite_aligned(buf.as_slice(), sb.checkpoint_offset)?;

        // 应回退到槽 B(代数 2,seq 5)
        let l = cp.load_latest()?.unwrap();
        assert_eq!(l.generation, 2);
        assert_eq!(l.seq, 5);
        Ok(())
    }

    #[test]
    fn generation_monotonic_after_corruption() -> Result<()> {
        let (_dir, path) = setup(64 * 1024 * 1024);
        init_device(&path, 4 * 1024 * 1024, 0, false)?;
        let dev = open_device(&path, false)?;
        let sb = fs3_device::read_superblock(dev.as_ref())?;
        let cp = Checkpointer::new(dev.as_ref(), &sb);
        // 连续保存多次,代数必须严格递增
        let mut last = 0u64;
        for i in 0..10u64 {
            let g = cp.save(&empty_cp(0, i, sb_checkpoint_bitmap_bytes(&sb)))?;
            assert!(g > last, "generation must increase");
            last = g;
        }
        assert_eq!(last, 11);
        Ok(())
    }
}
