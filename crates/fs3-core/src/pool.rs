//! 池清单与全局 extent id 推导式映射(M13 DM1/DM1',ADR-15)。
//!
//! 池清单 = 系统键 `s:pool`(rocksdb,值 = postcard(PoolManifest));设备序
//! = 数组序,**扩容只追加、移除只允许尾部**(防 id 推导错乱)。
//!
//! 推导式映射(不落额外账本):设备 i 的本地 extent `l` 的全局 id =
//! `Σ(设备 0..i−1 的 extent 数) + l`。`extent_count` 在清单中冗余存储,
//! 启动时与设备超块(`compute_layout(capacity, extent_size)` 反推)校验;
//! 校验通过后映射即由本模块的纯函数确定性给出,`Segment` 零改动。

use crate::{Error, Result};

/// 池清单条目(设备序 = 数组序;仅尾部增删)。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeviceEntry {
    /// 设备超块 uuid(启动校验绑定;缺盘/换盘 → 只读降级)。
    pub uuid: [u8; 16],
    /// 打开路径(设备 add 时记录;仅诊断/重放提示用,不作权威绑定)。
    pub path: String,
    /// 设备容量字节(与超块 data_end 校验)。
    pub capacity: u64,
    /// 该设备 extent 总数(冗余;与超块 `compute_layout` 反推值校验)。
    pub extent_count: u64,
    /// 分配权重(剩余空间加权轮转的静态基准,DM2;默认 1)。
    pub weight: u64,
    /// 加入池时间(unix 秒)。
    pub added_at: i64,
}

/// 池清单(M13 DM1';键 `s:pool`;值与键同在元数据,设备超块只持 uuid)。
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct PoolManifest {
    pub devices: Vec<DeviceEntry>,
}

impl PoolManifest {
    /// 全部设备 extent 总数(全局 id 空间上限)。
    pub fn total_extents(&self) -> u64 {
        self.devices.iter().map(|d| d.extent_count).sum()
    }

    /// 设备 `idx` 的全局 id 基址 = Σ(设备 0..idx 的 extent 数)。
    pub fn device_base(&self, idx: usize) -> Result<u64> {
        let dev = self
            .devices
            .get(idx)
            .ok_or_else(|| Error::InvalidArgument(format!("device index {idx} out of range")))?;
        let mut base = 0u64;
        for d in self.devices.iter().take(idx) {
            base = base
                .checked_add(d.extent_count)
                .ok_or_else(|| Error::InvalidArgument("pool extent count overflow".into()))?;
        }
        if dev.extent_count == 0 {
            return Err(Error::InvalidArgument(format!(
                "device {idx} has zero extents"
            )));
        }
        Ok(base)
    }

    /// 全局 id 上限(最后一个设备的基址 + 其 extent 数)。
    pub fn global_limit(&self) -> Result<u64> {
        if self.devices.is_empty() {
            return Ok(0);
        }
        let last = self.devices.len() - 1;
        let base = self.device_base(last)?;
        base.checked_add(self.devices[last].extent_count)
            .ok_or_else(|| Error::InvalidArgument("pool extent count overflow".into()))
    }

    /// 推导映射:本地 extent → 全局 id(越界返回 InvalidArgument)。
    pub fn global_extent_id(&self, idx: usize, local: u64) -> Result<u64> {
        let base = self.device_base(idx)?;
        let dev = &self.devices[idx];
        if local >= dev.extent_count {
            return Err(Error::InvalidArgument(format!(
                "local extent {local} out of range for device {idx} ({} extents)",
                dev.extent_count
            )));
        }
        base.checked_add(local)
            .ok_or_else(|| Error::InvalidArgument("global extent id overflow".into()))
    }

    /// 反推映射:全局 id → (设备序, 本地 id);越界返回 InvalidArgument。
    pub fn resolve(&self, global: u64) -> Result<(usize, u64)> {
        let mut base = 0u64;
        for (idx, d) in self.devices.iter().enumerate() {
            if d.extent_count == 0 {
                return Err(Error::InvalidArgument(format!(
                    "device {idx} has zero extents"
                )));
            }
            if global < base + d.extent_count {
                return Ok((idx, global - base));
            }
            base += d.extent_count;
        }
        Err(Error::InvalidArgument(format!(
            "global extent id {global} out of pool range (limit {})",
            self.total_extents()
        )))
    }

    /// 校验:设备非空、每设备 extent 数 > 0、总量不溢出 u32 上限
    /// (全局 extent id 空间 ← u32 Segment.extent_id;≈16PiB 池容量上限)。
    pub fn validate(&self) -> Result<()> {
        if self.devices.is_empty() {
            return Err(Error::InvalidArgument("pool manifest is empty".into()));
        }
        let mut total = 0u64;
        for (i, d) in self.devices.iter().enumerate() {
            if d.extent_count == 0 {
                return Err(Error::InvalidArgument(format!(
                    "device {i} has zero extents"
                )));
            }
            if d.weight == 0 {
                return Err(Error::InvalidArgument(format!(
                    "device {i} weight must be > 0"
                )));
            }
            total = total
                .checked_add(d.extent_count)
                .ok_or_else(|| Error::InvalidArgument("pool extent count overflow".into()))?;
        }
        if total > u32::MAX as u64 {
            return Err(Error::InvalidArgument(format!(
                "pool exceeds u32 extent id space ({total} > {})",
                u32::MAX
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(idx: u64, count: u64) -> DeviceEntry {
        DeviceEntry {
            uuid: [idx as u8; 16],
            path: format!("/dev/nvme{idx}n1"),
            capacity: count * 4 * 1024 * 1024,
            extent_count: count,
            weight: 1,
            added_at: 0,
        }
    }

    fn manifest() -> PoolManifest {
        PoolManifest {
            devices: vec![entry(0, 100), entry(1, 200), entry(2, 50)],
        }
    }

    #[test]
    fn derived_mapping_forward_and_back() -> Result<()> {
        let m = manifest();
        assert_eq!(m.total_extents(), 350);
        assert_eq!(m.global_limit()?, 350);
        // 设备基址:0 / 100 / 300
        assert_eq!(m.device_base(0)?, 0);
        assert_eq!(m.device_base(1)?, 100);
        assert_eq!(m.device_base(2)?, 300);
        // 正向
        assert_eq!(m.global_extent_id(0, 99)?, 99);
        assert_eq!(m.global_extent_id(1, 0)?, 100);
        assert_eq!(m.global_extent_id(1, 199)?, 299);
        assert_eq!(m.global_extent_id(2, 49)?, 349);
        // 反推
        assert_eq!(m.resolve(0)?, (0, 0));
        assert_eq!(m.resolve(99)?, (0, 99));
        assert_eq!(m.resolve(100)?, (1, 0));
        assert_eq!(m.resolve(299)?, (1, 199));
        assert_eq!(m.resolve(349)?, (2, 49));
        Ok(())
    }

    #[test]
    fn out_of_range_rejected() -> Result<()> {
        let m = manifest();
        assert!(m.global_extent_id(0, 100).is_err());
        assert!(m.global_extent_id(3, 0).is_err());
        assert!(m.resolve(350).is_err());
        // 尾部移除后:全局 id 295 不再属于设备 2
        let mut m2 = m.clone();
        m2.devices.pop();
        assert!(m2.resolve(349).is_err());
        assert_eq!(m2.resolve(299)?, (1, 199));
        assert_eq!(m2.global_limit().unwrap(), 300);
        Ok(())
    }

    #[test]
    fn validate_rejects_bad_manifest() {
        let m = manifest();
        m.validate().unwrap();
        let mut empty = PoolManifest::default();
        assert!(empty.validate().is_err());
        empty.devices.push(entry(0, 0));
        assert!(empty.validate().is_err());
        let mut w0 = manifest();
        w0.devices[0].weight = 0;
        assert!(w0.validate().is_err());
    }

    #[test]
    fn serde_roundtrip() -> Result<()> {
        let m = manifest();
        let bytes = postcard::to_allocvec(&m).map_err(|e| Error::Meta(e.to_string()))?;
        let m2: PoolManifest =
            postcard::from_bytes(&bytes).map_err(|e| Error::Meta(e.to_string()))?;
        assert_eq!(m, m2);
        Ok(())
    }
}
