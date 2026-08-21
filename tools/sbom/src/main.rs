//! FastS3 SBOM 生成器(M6/A5):主仓库 Cargo.lock → CycloneDX 1.5 JSON。
//!
//! 独立 crate(参照 tools/runtime-ab 先例):不进入主 workspace / 主 Cargo.lock,
//! 仅由 tools/sbom/sbom.sh 按需构建调用。
//!
//! 输出形态(CycloneDX 1.5):
//!   bomFormat "CycloneDX" / specVersion "1.5" / 随机 serialNumber(urn:uuid:v4)
//!   components[]: { type: "library", name, version, purl: "pkg:cargo/<n>@<v>",
//!                   licenses: [] }(licenses 可为空数组)
//!   -n 附加的 web 侧 package.json 组件:purl "pkg:npm/<urlencode(name)>@<v>"
//!
//! 依赖最小化:serde/serde_json/toml;时间戳(UTC RFC3339)与 UUID v4 均手写,
//! 不引入 chrono/uuid。

use std::fs;
use std::io::Read;
use std::process::ExitCode;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]   // CycloneDX 要求 camelCase 字段名
struct Bom {
    bom_format: String,
    spec_version: String,
    serial_number: String,
    version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
    components: Vec<Component>,
}

#[derive(Serialize, Debug)]
struct Metadata {
    timestamp: String,
    tools: Vec<Tool>,
}

#[derive(Serialize, Debug)]
struct Tool {
    vendor: String,
    name: String,
    version: String,
}

#[derive(Serialize, Debug, Clone)]
struct Component {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    version: String,
    purl: String,
    // 规范:每个 component 都带 licenses(可为空数组)
    licenses: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct NpmPkg {
    name: String,
    version: String,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cargo_lock: Option<String> = None;
    let mut out: Option<String> = None;
    let mut npm_pkgs: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-o" | "--out" => {
                i += 1;
                out = args.get(i).cloned();
            }
            "-n" | "--npm" => {
                i += 1;
                if let Some(p) = args.get(i) {
                    npm_pkgs.push(p.clone());
                }
            }
            "-h" | "--help" => {
                eprintln!("usage: fasts3-sbom <Cargo.lock> -o <out.json> [-n <package.json> ...]");
                return ExitCode::from(2);
            }
            a if a.starts_with('-') => {
                eprintln!("error: 未知参数 {a}");
                return ExitCode::from(2);
            }
            other => cargo_lock = Some(other.to_string()),
        }
        i += 1;
    }

    let lock = match cargo_lock {
        Some(p) => p,
        None => {
            eprintln!("error: 缺失 Cargo.lock 路径(用法见 --help)");
            return ExitCode::from(2);
        }
    };
    let out = out.unwrap_or_else(|| "SBOM.json".to_string());

    let mut components: Vec<Component> = Vec::new();

    // ── Cargo.lock → pkg:cargo 组件 ──────────────────────────────────────
    let text = match fs::read_to_string(&lock) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: 读取 {lock} 失败: {e}");
            return ExitCode::from(1);
        }
    };
    let doc: toml::Value = match toml::from_str(&text) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: 解析 {lock} 失败: {e}");
            return ExitCode::from(1);
        }
    };
    if let Some(pkgs) = doc.get("package").and_then(|v| v.as_array()) {
        for p in pkgs {
            let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let version = p.get("version").and_then(|v| v.as_str()).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            components.push(Component {
                kind: "library".into(),
                name: name.to_string(),
                version: version.to_string(),
                purl: format!("pkg:cargo/{name}@{version}"),
                licenses: Vec::new(),
            });
        }
    }

    // ── package.json → pkg:npm 组件(name/version;pnpm-lock 不展开)───────
    for p in &npm_pkgs {
        let raw = match fs::read_to_string(p) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("warn: 读取 {p} 失败: {e}(跳过)");
                continue;
            }
        };
        let parsed: NpmPkg = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("warn: 解析 {p} 失败: {e}(跳过)");
                continue;
            }
        };
        let enc = urlencode_npm_name(&parsed.name);
        components.push(Component {
            kind: "library".into(),
            name: parsed.name.clone(),
            version: parsed.version.clone(),
            purl: format!("pkg:npm/{enc}@{v}", v = parsed.version),
            licenses: Vec::new(),
        });
    }

    let bom = Bom {
        bom_format: "CycloneDX".into(),
        spec_version: "1.5".into(),
        serial_number: format!("urn:uuid:{}", uuid_v4()),
        version: 1,
        metadata: Some(Metadata {
            timestamp: now_rfc3339_utc(),
            tools: vec![Tool {
                vendor: "FastS3 Project".into(),
                name: "fasts3-sbom".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            }],
        }),
        components,
    };

    let json = serde_json::to_string_pretty(&bom).expect("serialize bom");
    if let Err(e) = fs::write(&out, json + "\n") {
        eprintln!("error: 写 {out} 失败: {e}");
        return ExitCode::from(1);
    }
    let n = bom.components.len();
    println!("SBOM 写入 {out}:{n} components (cargo={} npm={})", n.saturating_sub(npm_pkgs.len()), npm_pkgs.len());
    ExitCode::SUCCESS
}

/// npm scope 的 URL 编码(purl 规范):`@` → `%40`,`/` → `%2F`。
fn urlencode_npm_name(name: &str) -> String {
    name.replace('@', "%40").replace('/', "%2F")
}

/// 随机 UUID v4(从 /dev/urandom 取 16 字节;按 RFC 4122 置 version/variant)。
fn uuid_v4() -> String {
    let mut b = [0u8; 16];
    if let Ok(mut f) = fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut b);
    }
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant 10xx
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

/// 当前 UTC 时间,RFC3339(手写;无 chrono)。
fn now_rfc3339_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, mo, d) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        mo,
        d,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant civil_from_days:天数 → (年,月,日)。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}