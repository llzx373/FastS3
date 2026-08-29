//! 后端描述符(M20/A4;ADR-29 KR5.4):vault/bao 差异收敛点。
//!
//! 二进制名 / 常见路径 / 版本探测在此一处定义;transit 调用面(client.rs /
//! 托管监督)共用同一 descriptor。版本探测不兼容时**显式报错**,不静默。

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::KmsError;

/// KMS 后端 flavor(ADR-29 KR1:Vault / OpenBao transit,API 同构)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Flavor {
    Vault,
    OpenBao,
}

impl Flavor {
    pub fn parse(s: &str) -> Result<Self, KmsError> {
        match s.to_ascii_lowercase().as_str() {
            "vault" => Ok(Flavor::Vault),
            "openbao" | "bao" => Ok(Flavor::OpenBao),
            other => Err(KmsError::Config(format!(
                "未知 KMS flavor '{other}'(可选:vault | openbao)"
            ))),
        }
    }

    pub fn descriptor(self) -> Descriptor {
        Descriptor::of(self)
    }
}

/// flavor 差异描述(静态;探测逻辑统一)。
#[derive(Debug, Clone, Copy)]
pub struct Descriptor {
    pub flavor: Flavor,
    /// 子进程二进制名(PATH 探测名)。
    pub bin_name: &'static str,
    /// 展示名(文档/控制台)。
    pub display: &'static str,
    /// 默认监听端口(与 deploy/vault/config.hcl 一致)。
    pub default_port: u16,
    /// 兼容最低版本(显式报错下限;AAD 强制自检仍是权威门)。
    pub min_version: (u32, u32),
}

impl Descriptor {
    pub const VAULT: Descriptor = Descriptor {
        flavor: Flavor::Vault,
        bin_name: "vault",
        display: "HashiCorp Vault",
        default_port: 8200,
        // BUSL 期 transit API 稳定;AAD 自检兜底(实测 2.0.4)
        min_version: (1, 14),
    };
    pub const OPENBAO: Descriptor = Descriptor {
        flavor: Flavor::OpenBao,
        bin_name: "bao",
        display: "OpenBao",
        default_port: 8200,
        min_version: (2, 0),
    };

    pub fn of(flavor: Flavor) -> Descriptor {
        match flavor {
            Flavor::Vault => Descriptor::VAULT,
            Flavor::OpenBao => Descriptor::OPENBAO,
        }
    }

    /// 二进制解析:显式路径(存在且可执行)→ PATH → 常见安装路径。
    pub fn resolve_binary(&self, explicit: Option<&Path>) -> Result<PathBuf, KmsError> {
        if let Some(p) = explicit {
            if p.is_file() {
                return Ok(p.to_path_buf());
            }
            return Err(KmsError::Config(format!(
                "{} 二进制不存在: {}",
                self.display,
                p.display()
            )));
        }
        // PATH
        if let Some(path) = std::env::var_os("PATH") {
            for dir in std::env::split_paths(&path) {
                let cand = dir.join(self.bin_name);
                if cand.is_file() {
                    return Ok(cand);
                }
            }
        }
        // 常见路径(离线预置场景:~/.local/bin)
        if let Some(home) = std::env::var_os("HOME") {
            let cand = PathBuf::from(home).join(".local/bin").join(self.bin_name);
            if cand.is_file() {
                return Ok(cand);
            }
        }
        Err(KmsError::Config(format!(
            "{} 二进制 '{}' 未找到(PATH 与常见路径均无;用 [kms.deploy] binary 显式指定)",
            self.display, self.bin_name
        )))
    }

    /// `<bin> version` 输出解析:"Vault v2.0.4 (…)" / "Bao v2.1.0" → (2,0,4)。
    pub fn probe_version(&self, bin: &Path) -> Result<(u32, u32, u32), KmsError> {
        let out = Command::new(bin)
            .arg("version")
            .output()
            .map_err(|e| KmsError::Config(format!("执行 {} version 失败: {e}", bin.display())))?;
        if !out.status.success() {
            return Err(KmsError::Config(format!(
                "{} version 退出码非 0: {}",
                bin.display(),
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        parse_semver(&text).ok_or_else(|| {
            KmsError::Config(format!(
                "无法从 '{}' version 输出解析版本: {text:?}",
                bin.display()
            ))
        })
    }

    /// 版本兼容校验:不兼容显式报错(不静默,ADR-29 KR5.4)。
    pub fn assert_compatible(&self, ver: (u32, u32, u32), bin: &Path) -> Result<(), KmsError> {
        if (ver.0, ver.1) < self.min_version {
            return Err(KmsError::Config(format!(
                "{} 版本过旧 {}.{}.{}(要求 ≥ {}.{});SSE-KMS 上下文绑定语义无法保证,拒绝启动",
                self.display, ver.0, ver.1, ver.2, self.min_version.0, self.min_version.1
            )));
        }
        let _ = bin;
        Ok(())
    }

    /// 解析 + 版本探测 + 兼容校验一条龙(托管管理器/CLI 共用)。
    pub fn resolve_and_check(&self, explicit: Option<&Path>) -> Result<PathBuf, KmsError> {
        let bin = self.resolve_binary(explicit)?;
        let ver = self.probe_version(&bin)?;
        self.assert_compatible(ver, &bin)?;
        Ok(bin)
    }
}

/// 从任意文本提取首个 `vMAJOR.MINOR.PATCH`(容忍 `Vault v2.0.4 (hash)` /
/// `OpenBao v2.1.0+ent` / 日期后缀);v 必须处于标识符边界(前一字符非
/// 字母/数字),候选解析失败则继续扫描。
pub fn parse_semver(text: &str) -> Option<(u32, u32, u32)> {
    let bytes = text.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] != b'v' && bytes[i] != b'V' {
            continue;
        }
        // 边界检查:v 不处于更长标识符中间(如 "hv2" 不算)
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
            continue;
        }
        if let Some(v) = parse_at(&text[i + 1..]) {
            return Some(v);
        }
    }
    None
}

/// 解析 `MAJOR.MINOR.PATCH` 前缀(容忍 `+ent` / ` (hash)` 后缀)。
fn parse_at(s: &str) -> Option<(u32, u32, u32)> {
    let end_of = |s: &str| -> usize { s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()) };
    let first_end = end_of(s);
    if first_end == 0 {
        return None;
    }
    let maj: u32 = s[..first_end].parse().ok()?;
    let rest = &s[first_end..];
    let rest = rest.strip_prefix('.')?;
    let second_end = end_of(rest);
    if second_end == 0 {
        return None;
    }
    let min: u32 = rest[..second_end].parse().ok()?;
    let rest = &rest[second_end..];
    let rest = rest.strip_prefix('.')?;
    let third_end = end_of(rest);
    if third_end == 0 {
        return None;
    }
    let pat: u32 = rest[..third_end].parse().ok()?;
    Some((maj, min, pat))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_flavor_diff_only_binary_for_same_config() {
        // A4 用例口径:同一 [kms] 配置切换 flavor 仅改 binary(端口/调用面不变)
        let v = Descriptor::VAULT;
        let b = Descriptor::OPENBAO;
        assert_eq!(v.bin_name, "vault");
        assert_eq!(b.bin_name, "bao");
        assert_eq!(v.default_port, b.default_port);
        assert_ne!(v.min_version, b.min_version);
    }

    #[test]
    fn semver_parse_variants() {
        assert_eq!(
            parse_semver(
                "Vault v2.0.4 (c9e9d1d4ddd4b55aae79a8949adffa9e96338720), built 2026-08-03"
            ),
            Some((2, 0, 4))
        );
        assert_eq!(parse_semver("Bao v2.1.0+ent"), Some((2, 1, 0)));
        assert_eq!(parse_semver("OpenBao v2.0.0"), Some((2, 0, 0)));
        assert_eq!(parse_semver("garbage"), None);
        // 不吞 'hv2' 之类中间 v
        assert_eq!(parse_semver("xhv2 v1.2.3"), Some((1, 2, 3)));
    }

    #[test]
    fn incompatible_version_fails_loudly() {
        let d = Descriptor::OPENBAO;
        assert!(d.assert_compatible((1, 9, 0), Path::new("/x/bao")).is_err());
        assert!(d.assert_compatible((2, 0, 0), Path::new("/x/bao")).is_ok());
        let v = Descriptor::VAULT;
        assert!(v
            .assert_compatible((1, 13, 9), Path::new("/x/vault"))
            .is_err());
        assert!(v
            .assert_compatible((1, 14, 0), Path::new("/x/vault"))
            .is_ok());
    }

    #[test]
    fn flavor_parse_rejects_unknown() {
        assert!(matches!(Flavor::parse("vault"), Ok(Flavor::Vault)));
        assert!(matches!(Flavor::parse("openbao"), Ok(Flavor::OpenBao)));
        assert!(matches!(Flavor::parse("bao"), Ok(Flavor::OpenBao)));
        assert!(Flavor::parse("kes").is_err());
    }
}
