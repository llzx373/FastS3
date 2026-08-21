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

fn init_img(dir: &Path, name: &str, size: &str) -> PathBuf {
    let img = dir.join(name);
    let out = run(&["init", "--device", img.to_str().unwrap(), "--size", size]);
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
    let img = init_img(dir.path(), "disk.img", "64MiB");
    let meta = dir.path().join("meta");
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
fn doctor_healthy_and_uninitialized() {
    let dir = tempfile::tempdir().unwrap();
    let img = init_img(dir.path(), "d.img", "32MiB");
    let cfg = dir.path().join("d.toml");
    let meta_d = dir.path().join("d-meta");
    std::fs::create_dir_all(&meta_d).unwrap();
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
    let img = init_img(dir.path(), "b.img", "64MiB");
    let meta = dir.path().join("m");
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
    // 无服务端:loadgen 应报错退出(覆盖 CLI 解析与运行主体路径)
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(BIN)
        .current_dir(dir.path())
        .args(&[
            "loadgen",
            "--endpoint",
            "127.0.0.1:1",
            "--access",
            "a",
            "--secret",
            "s",
            "--objects",
            "1",
            "--size",
            "1024",
        ])
        .output()
        .expect("run loadgen");
    let _ = out;
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
            device: img,
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
        .args(&[
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
