//! CLI 端到端测试(M4 覆盖门禁):运行真实 `fasts3d` 二进制,覆盖
//! main/init/put/get/ls/check/bench/stress/doctor/serve 全命令与配置层。

use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_fasts3d");

fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run fasts3d")
}

/// M6:init 向导非交互(--yes)初始化;配置/数据落在临时目录。
fn init_img(dir: &Path, name: &str, size: &str, meta_dir: &Path) -> PathBuf {
    let img = dir.join(name);
    let out = run(&[
        "init",
        "--yes",
        "--no-tls",
        "--device",
        img.to_str().unwrap(),
        "--size",
        size,
        "--meta-dir",
        meta_dir.to_str().unwrap(),
        "--data-dir",
        dir.to_str().unwrap(),
        "--config",
        dir.join("fasts3.toml").to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    img
}

#[test]
fn init_put_get_ls_check_del_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let meta = dir.path().join("meta");
    let img = init_img(dir.path(), "disk.img", "64MiB", &meta);
    let b = "bkt";

    let small = dir.path().join("small.bin");
    std::fs::write(&small, vec![0x11u8; 4096]).unwrap();
    let big = dir.path().join("big.bin");
    let big_data = vec![0x22u8; 2 * 1024 * 1024];
    std::fs::write(&big, &big_data).unwrap();

    for (k, f) in [("s", &small), ("b", &big)] {
        let out = run(&[
            "put",
            "--device",
            img.to_str().unwrap(),
            "--meta-dir",
            meta.to_str().unwrap(),
            "--bucket",
            b,
            k,
            f.to_str().unwrap(),
        ]);
        assert!(out.status.success(), "put {k} failed");
    }

    let got = dir.path().join("got.bin");
    let out = run(&[
        "get",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--bucket",
        b,
        "b",
        got.to_str().unwrap(),
    ]);
    assert!(out.status.success());
    assert_eq!(std::fs::read(&got).unwrap(), big_data);

    let out = run(&[
        "ls",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--bucket",
        b,
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(text.contains('s'), "ls should list s: {text}");

    let out = run(&[
        "check",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(text.contains("leaks:        none"), "check leaks: {text}");

    let out = run(&[
        "checkpoint",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "checkpoint failed");

    let out = run(&[
        "del",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--bucket",
        b,
        "s",
    ]);
    assert!(out.status.success(), "del failed");
}

#[test]
fn meta_export_import_roundtrip() {
    // M7/E5:meta-export → 元数据丢失 → meta-import 恢复到同一布局设备,
    // 对象内容(内联 + 段)完整、位图零泄漏;负例(非空目录/布局不匹配)拒绝。
    let dir = tempfile::tempdir().unwrap();
    let meta = dir.path().join("meta");
    let img = init_img(dir.path(), "bk.img", "64MiB", &meta);
    let b = "bkt";

    let small_path = dir.path().join("small.bin");
    let small_data = vec![0xABu8; 512]; // 内联(≤ 32KiB)
    std::fs::write(&small_path, &small_data).unwrap();
    let big_path = dir.path().join("big.bin");
    let big_data: Vec<u8> = (0..(1024 * 1024 / 64))
        .flat_map(|i| (i as u32).to_le_bytes().repeat(16))
        .collect::<Vec<u8>>(); // 1MiB → 段数据
    std::fs::write(&big_path, &big_data).unwrap();
    for (k, f) in [("small", &small_path), ("big", &big_path)] {
        let out = run(&[
            "put",
            "--device",
            img.to_str().unwrap(),
            "--meta-dir",
            meta.to_str().unwrap(),
            "--bucket",
            b,
            k,
            f.to_str().unwrap(),
        ]);
        assert!(out.status.success(), "put {k} failed");
    }

    // 导出(停机窗口:CLI put 已收尾;导出文件权限 0600)
    let export = dir.path().join("meta-export.json");
    let out = run(&[
        "meta-export",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--output",
        export.to_str().unwrap(),
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "meta-export: {text}");
    assert!(text.contains("2 objects"), "export summary: {text}");
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&export).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "export file must be 0600");
    }

    // 模拟元数据卷丢失:meta 目录被毁,设备(底层数据卷)完好
    std::fs::remove_dir_all(&meta).unwrap();

    // 负例 1:非空 meta 目录无 --force 拒绝
    let meta_dirty = dir.path().join("meta-dirty");
    std::fs::create_dir_all(&meta_dirty).unwrap();
    std::fs::write(meta_dirty.join("stray"), b"x").unwrap();
    let out = run(&[
        "meta-import",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta_dirty.to_str().unwrap(),
        "--input",
        export.to_str().unwrap(),
    ]);
    assert!(!out.status.success(), "import into non-empty dir must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not empty"),
        "expected not-empty error: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 恢复:导入到全新 meta 目录
    let meta2 = dir.path().join("meta2");
    let out = run(&[
        "meta-import",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta2.to_str().unwrap(),
        "--input",
        export.to_str().unwrap(),
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "meta-import failed: {text}");
    assert!(text.contains("2 objects"), "import summary: {text}");
    assert!(text.contains("leaks=0"), "no leaks after restore: {text}");

    // 校验:ls 可见、get 内容逐字节一致
    let out = run(&[
        "ls",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta2.to_str().unwrap(),
        "--bucket",
        b,
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(text.contains("small") && text.contains("big"), "ls: {text}");

    for (k, expect) in [("small", &small_data), ("big", &big_data)] {
        let got = dir.path().join(format!("restored-{k}.bin"));
        let out = run(&[
            "get",
            "--device",
            img.to_str().unwrap(),
            "--meta-dir",
            meta2.to_str().unwrap(),
            "--bucket",
            b,
            k,
            got.to_str().unwrap(),
        ]);
        assert!(out.status.success(), "get {k} failed");
        assert_eq!(
            std::fs::read(&got).unwrap(),
            *expect,
            "content mismatch {k}"
        );
    }

    // check 一致性:位图 vs 元数据零泄漏
    let out = run(&[
        "check",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta2.to_str().unwrap(),
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(text.contains("leaks:        none"), "check leaks: {text}");

    // 负例 2:布局不匹配(不同容量设备)拒绝
    let meta3 = dir.path().join("meta3");
    let img2 = init_img(dir.path(), "other.img", "32MiB", &meta3);
    let out = run(&[
        "meta-import",
        "--device",
        img2.to_str().unwrap(),
        "--meta-dir",
        meta3.to_str().unwrap(),
        "--input",
        export.to_str().unwrap(),
    ]);
    assert!(!out.status.success(), "layout mismatch must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("layout mismatch"),
        "expected layout mismatch: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn doctor_healthy_and_uninitialized() {
    let dir = tempfile::tempdir().unwrap();
    let meta_d = dir.path().join("d-meta");
    std::fs::create_dir_all(&meta_d).unwrap();
    let img = init_img(dir.path(), "d.img", "32MiB", &meta_d);
    let cfg = dir.path().join("d.toml");
    std::fs::write(
        &cfg,
        format!(
            "[storage]\ndevices = [\"{}\"]\nmeta_dir = \"{}\"\n",
            img.display(),
            meta_d.display()
        ),
    )
    .unwrap();

    let out = run(&["doctor", "--config", cfg.to_str().unwrap()]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "doctor exit={}: {text}", out.status);
    assert!(
        text.contains("RESULT: 全绿") || text.contains("warning"),
        "doctor: {text}"
    );

    let empty = dir.path().join("uninit.img");
    std::fs::write(&empty, vec![0u8; 4096]).unwrap();
    let cfg2 = dir.path().join("u.toml");
    std::fs::write(
        &cfg2,
        format!(
            "[storage]\ndevices = [\"{}\"]\nmeta_dir = \"{}\"\n",
            empty.display(),
            dir.path().join("u-meta").display()
        ),
    )
    .unwrap();
    let out = run(&["doctor", "--config", cfg2.to_str().unwrap()]);
    assert!(!out.status.success(), "uninitialized doctor should fail");
}

#[test]
fn bench_and_stress_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let meta = dir.path().join("m");
    let img = init_img(dir.path(), "b.img", "64MiB", &meta);
    // bench 引擎基准(小规模:1s write)
    let out = run(&[
        "bench",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--rw",
        "write",
        "--duration",
        "1",
        "--block",
        "64KiB",
    ]);
    assert!(
        out.status.success(),
        "bench failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = run(&[
        "stress-insert",
        "--device",
        img.to_str().unwrap(),
        "--meta-dir",
        meta.to_str().unwrap(),
        "--count",
        "2000",
        "--size",
        "64",
        "--checkpoint-every",
        "1000",
    ]);
    assert!(
        out.status.success(),
        "stress failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("leaks=0"), "stress verify leaks: {text}");
}

#[test]
fn loadgen_smoke() {
    // 无服务端:loadgen 应报错退出(覆盖 CLI 解析与运行主体路径)。
    // REVIEW §4.6:此前用不存在的 --access/--secret/--objects 参数(clap 直接
    // 报错)且 `let _ = out;` 不断言——无论成功失败都算"通过";现用真实参数
    // (--key access:secret)并断言:连接被拒 → 非零退出。
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(BIN)
        .current_dir(dir.path())
        .args([
            "loadgen",
            "--endpoint",
            "http://127.0.0.1:1",
            "--key",
            "a:s",
            "--size",
            "1024",
            "--duration",
            "1",
        ])
        .output()
        .expect("run loadgen");
    assert!(
        !out.status.success(),
        "loadgen against unreachable endpoint must fail; stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("fail") || text.contains("error") || text.contains("拒绝"),
        "loadgen failure must be reported: {text}"
    );
}

/// 网络层 + 负载生成器覆盖:进程内起真实 HTTP server(SO_REUSEPORT worker)
/// → 子进程 loadgen 压测;同时覆盖 fs3-http 的 serve()/worker_main 与
/// loadgen(子进程 profraw 由 cargo-llvm-cov 合并)。
#[test]
fn serve_network_and_loadgen_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("n.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let engine = std::sync::Arc::new(parking_lot::RwLock::new(
        fs3_engine::Engine::open(&fs3_engine::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("m"),
            ..Default::default()
        })
        .unwrap(),
    ));
    let service = std::sync::Arc::new(fs3_s3::S3Service::new(
        engine,
        vec![fs3_s3::auth::Credentials {
            access_key: "test".into(),
            secret_key: "secret123".into(),
        }],
        "us-east-1".into(),
        false,
    ));

    // 占用一个临时端口后释放(获取空闲端口)
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let cfg = fs3_http::HttpServerConfig {
        listen: addr,
        workers: 1,
        ..Default::default()
    };
    let svc = service.clone();
    std::thread::spawn(move || {
        let _ = fs3_http::serve(svc, &cfg);
    });
    // 等服务就绪
    std::thread::sleep(std::time::Duration::from_millis(600));

    // loadgen 子进程:先 put 再 mix(建桶 + 写读)
    let out = Command::new(BIN)
        .args([
            "loadgen",
            "--endpoint",
            &format!("http://127.0.0.1:{port}"),
            "--key",
            "test:secret123",
            "--bucket",
            "lg",
            "--ops",
            "mix",
            "--size",
            "131072",
            "--concurrency",
            "2",
            "--duration",
            "2",
            "--keys",
            "8",
        ])
        .output()
        .expect("run loadgen");
    assert!(
        out.status.success(),
        "loadgen failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = out;
}
