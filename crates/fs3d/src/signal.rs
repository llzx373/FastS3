//! 信号处理(M6 / K4 升级回滚的前置:优雅停机)。
//!
//! SIGTERM / SIGINT → 看门狗线程置停机标志;`fs3_http::serve_with_shutdown`
//! 轮询该标志后停止接受新连接、排空在途请求(≤5s),serve 返回后主线程做
//! 引擎收尾(最终检查点 + 元数据关闭)再退出 —— 升级流程因此能拿到
//! "干净停机的设备"(数据先落盘、元数据已提交)。
//!
//! 实现:自管道(self-pipe)技巧 —— 信号处理器只做 async-signal-safe 的
//! `write()`(写 1 字节到管道),看门狗线程阻塞 `read()` 唤醒后置位并返回。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 管道写端 fd(signal handler 用;atomic 保证 handler 内读)。
static PIPE_WRITE_FD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

extern "C" fn on_signal(_sig: libc::c_int) {
    let fd = PIPE_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let b = [1u8];
        // SAFETY: fd 为已打开的管道写端;1 字节写入 async-signal-safe。
        unsafe {
            libc::write(fd, b.as_ptr() as *const libc::c_void, 1);
        }
    }
}

/// 安装 SIGTERM/SIGINT 处理器;信号到达后置位 `shutdown` 并返回(线程退出)。
pub fn install(shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
    let mut fds = [0i32; 2];
    // SAFETY: fds 长度 2;O_NONBLOCK 避免 handler 写阻塞。
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    PIPE_WRITE_FD.store(fds[1], Ordering::Relaxed);

    // SAFETY: 结构体清零后按 libc 布局填充;sa_sigaction 为函数指针 union。
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_sigaction = on_signal as *const () as usize;
    sa.sa_flags = 0; // 不带 SA_RESTART:处理器后 read 返回 EINTR 也无妨(循环处理)
    unsafe {
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }

    // 看门狗线程:阻塞读管道;收到信号 → 置停机标志。
    let read_fd = fds[0];
    std::thread::Builder::new()
        .name("fs3-signal".into())
        .spawn(move || {
            let mut b = [0u8; 64];
            loop {
                // SAFETY: read_fd 有效且非阻塞。
                let n =
                    unsafe { libc::read(read_fd, b.as_mut_ptr() as *mut libc::c_void, b.len()) };
                if n > 0 {
                    tracing::info!(
                        "shutdown signal received; stopping accept, draining in-flight (<=5s)"
                    );
                    shutdown.store(true, Ordering::SeqCst);
                    return;
                }
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    match err.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            continue;
                        }
                        std::io::ErrorKind::Interrupted => continue,
                        _ => {
                            tracing::warn!("signal pipe read error: {err}");
                            return;
                        }
                    }
                }
                return; // EOF(管道写端关闭,不应发生)
            }
        })
        .map_err(std::io::Error::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_sets_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        install(flag.clone()).unwrap();
        // 直接投递 SIGTERM(sigaction 已注册;测试进程内)
        // 只向本进程投递 SIGTERM(kill(0) 会误伤整个进程组/测试 harness)
        // SAFETY: getpid 返回本进程 pid;sigaction 已注册。
        let pid = unsafe { libc::getpid() };
        let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
        assert_eq!(rc, 0);
        // 看门狗线程应在短时间内置位
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !flag.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            flag.load(Ordering::SeqCst),
            "SIGTERM should set shutdown flag"
        );
    }
}
