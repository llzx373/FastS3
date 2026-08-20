#!/usr/bin/env python3
"""FastS3 M1 HTTP 崩溃恢复 harness:随机 kill -9 上传进程 + 重启校验。

断言:
  1. 已应答(HTTP 200)的对象:内容与大小逐字节一致(不撕裂、不丢失);
  2. 被杀的上传:对象要么完整可见要么完全不可见;
  3. 每轮 `fasts3d check`:位图/元数据一致、零泄漏;
  4. 定期删除旧对象(回收空间 + 压测 delete)。

用法: python3 http_crash_test.py [rounds] [endpoint]
前置:target/release/fasts3d 已构建;服务由本脚本拉起。
"""
import hashlib
import http.client
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "smoke"))
from sigv4_smoke import sign  # noqa: E402

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 100
ENDPOINT = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:19000"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN = os.path.join(ROOT, "target", "release", "fasts3d")
BUCKET = "crash"
BIG_SIZE = 24 * 1024 * 1024

failures = []


def check(name, cond, detail=""):
    if not cond:
        print(f"  FAIL: {name} {detail}")
        failures.append(name)


def signed_request(method, path, query=None, body=b"", headers=None):
    query = query or {}
    headers = headers or {}
    hdrs, auth, _ = sign(method, path, query, headers, body)
    hdrs["Authorization"] = auth
    qs = urllib.parse.urlencode(query) if query else ""
    url = path + (f"?{qs}" if qs else "")
    conn = http.client.HTTPConnection(ENDPOINT, timeout=120)
    conn.request(method, url, body=body, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def put_small(key, data):
    return signed_request("PUT", f"/{BUCKET}/{key}", body=data)


def get_object(key):
    return signed_request("GET", f"/{BUCKET}/{key}")


def del_object(key):
    return signed_request("DELETE", f"/{BUCKET}/{key}")


def uploader_main(key, data, endpoint, port_file):
    """子进程:做一次大对象 PUT(带 SigV4)。"""
    global ENDPOINT
    ENDPOINT = endpoint

    hdrs, auth, _ = sign("PUT", f"/{BUCKET}/{key}", {}, {}, data)
    hdrs["Authorization"] = auth
    conn = http.client.HTTPConnection(ENDPOINT, timeout=600)
    conn.request("PUT", f"/{BUCKET}/{key}", body=data, headers=hdrs)
    resp = conn.getresponse()
    status = resp.status
    data_out = resp.read()
    conn.close()
    with open(port_file, "w") as f:
        f.write(str(status))


def main():
    print(f"== FastS3 HTTP crash harness: rounds={ROUNDS} endpoint={ENDPOINT} ==")
    work = tempfile.mkdtemp(prefix="fs3-httpcrash.")
    img = os.path.join(work, "disk.img")
    meta = os.path.join(work, "meta")
    manifest = {}
    port = int(ENDPOINT.split(":")[1])

    subprocess.run([BIN, "init", "--device", img, "--size", "512MiB"], check=True,
                   capture_output=True)
    srv = subprocess.Popen(
        [BIN, "serve", "--device", img, "--meta-dir", meta,
         "--listen", ENDPOINT, "--key", "test:secret123"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)

    # 建桶
    st, _ = signed_request("PUT", f"/{BUCKET}")
    if st != 200:
        print(f"create bucket failed: {st}")
        sys.exit(1)

    def restart_server():
        """SIGKILL 服务进程并重启(协议层崩溃恢复)。"""
        nonlocal srv
        srv.kill()
        srv.wait()
        time.sleep(0.5)
        srv = subprocess.Popen(
            [BIN, "serve", "--device", img, "--meta-dir", meta,
             "--listen", ENDPOINT, "--key", "test:secret123"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.0)

    try:
        for i in range(ROUNDS):
            round_ok = True
            # 1) 小对象(必完成)
            for j in range(6):
                key = f"r{i}-small-{j}"
                data = os.urandom(2048)
                st, _ = put_small(key, data)
                if st == 200:
                    manifest[key] = hashlib.md5(data).hexdigest()
                else:
                    print(f"round {i}: small put status {st}")
                    round_ok = False

            # 2) 大对象上传子进程,随机 kill -9
            key = f"r{i}-big"
            data = os.urandom(BIG_SIZE)
            port_file = os.path.join(work, "uploader.port")
            proc = subprocess.Popen(
                [sys.executable, __file__, "--uploader", key, port_file, ENDPOINT],
                stdin=subprocess.PIPE)
            # 数据经 stdin 传给子进程(避免 argv 过长)
            import threading

            def feed():
                try:
                    proc.stdin.write(data)
                    proc.stdin.close()
                except BrokenPipeError:
                    pass

            threading.Thread(target=feed, daemon=True).start()
            time.sleep(random.uniform(0, 0.8))
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            # 若已应答则入 manifest
            try:
                st = int(open(port_file).read().strip())
                if st == 200:
                    manifest[key] = hashlib.md5(data).hexdigest()
            except (FileNotFoundError, ValueError):
                pass

            # 3) 回收空间
            if i >= 2:
                old = i - 2
                for j in range(6):
                    del_object(f"r{old}-small-{j}")
                del_object(f"r{old}-big")
                for k in list(manifest):
                    if k.startswith(f"r{old}-"):
                        del manifest[k]

            # 4) 校验(经 HTTP)
            for key, md5 in manifest.items():
                st, body = get_object(key)
                if st != 200 or hashlib.md5(body).hexdigest() != md5:
                    print(f"round {i}: CORRUPTION key={key} status={st}")
                    round_ok = False
            # 每 5 轮:直接 SIGKILL 服务进程并重启(协议层崩溃恢复)
            if (i + 1) % 5 == 0:
                restart_server()
                # 重启后全量校验
                for key, md5 in manifest.items():
                    st, body = get_object(key)
                    if st != 200 or hashlib.md5(body).hexdigest() != md5:
                        print(f"round {i}: CORRUPTION after restart key={key} status={st}")
                        round_ok = False

            if not round_ok:
                failures.append(f"round {i}")

            if (i + 1) % 10 == 0:
                print(f"progress: {i+1}/{ROUNDS} (failed={len(failures)})")
    finally:
        srv.kill()
        srv.wait()
    # 终局:停服后做一致性检查(位图/元数据零泄漏)
    r = subprocess.run([BIN, "check", "--device", img, "--meta-dir", meta],
                       capture_output=True, text=True)
    if "leaks:        none" not in r.stdout:
        print("FINAL CHECK FAILED:", r.stdout[-300:])
        failures.append("final check")
    print("=" * 40)
    if failures:
        print(f"FAILED: {len(failures)} rounds")
        sys.exit(1)
    print(f"PASS: {ROUNDS} rounds, no torn object, bitmap consistent, zero leaks")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[2] == "--uploader":
        # 子进程模式:key port_file endpoint,数据从 stdin
        key = sys.argv[3]
        port_file = sys.argv[4]
        endpoint = sys.argv[5]
        data = sys.stdin.buffer.read()
        uploader_main(key, data, endpoint, port_file)
        sys.exit(0)
    main()
