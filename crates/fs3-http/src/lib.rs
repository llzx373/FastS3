//! FastS3 HTTP 接入(DESIGN §5.5 / G1):hyper + SO_REUSEPORT 每核监听、
//! HTTP/1.1 keep-alive、请求体流式接收、响应流式发送。
//!
//! M1 线程模型:每 worker 一个 tokio runtime + 一个 SO_REUSEPORT listener;
//! S3Service 共享(引擎 std Mutex 串行化)。thread-per-core 零拷贝优化在 M5。

use std::net::SocketAddr;
use std::sync::Arc;

use fs3_s3::S3Service;
use tokio::net::TcpListener;

mod admission;
mod handler;
mod timeout_io;
pub mod tls;
mod zero_copy;

pub use admission::Admission;
pub use timeout_io::DeadlinedIo;
pub use tls::{TlsConfig, TlsState};
pub use zero_copy::{probe_fd_capability, ZeroCopyIo, ZeroCtx};

/// 单连接处理(测试与内嵌复用)。
pub use handler::{serve_connection, serve_connection_tls};

/// 服务配置。
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub listen: SocketAddr,
    /// 每核 worker 数(0 = 自动 = 逻辑核数)。
    pub workers: usize,
    /// 全局在途字节上限(G3;超限 503 SlowDown + Retry-After)。
    pub max_inflight_bytes: u64,
    /// 请求头读取超时(H4;默认 30s;超时关闭连接)。
    pub header_timeout: std::time::Duration,
    /// keep-alive 空闲超时(H4;默认 60s;超时关闭连接)。
    pub idle_timeout: std::time::Duration,
    /// TLS(M4):None = 明文;Some = rustls(ALPN h2/h1;证书热加载)。
    pub tls: Option<Arc<TlsState>>,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig {
            listen: "0.0.0.0:9000".parse().unwrap(),
            workers: 0,
            max_inflight_bytes: 16 * 1024 * 1024 * 1024, // DESIGN §6.5:16GiB
            header_timeout: std::time::Duration::from_secs(30), // DESIGN §9:header 30s
            idle_timeout: std::time::Duration::from_secs(60), // DESIGN §9:idle 60s
            tls: None,
        }
    }
}

/// 启动 HTTP 服务器(阻塞;每 worker 一个 runtime + SO_REUSEPORT listener)。
/// TLS 启用时:并发起证书热加载轮询线程(5s;文件 mtime 变更即替换)。
pub fn serve(service: Arc<S3Service>, cfg: &HttpServerConfig) -> std::io::Result<()> {
    let workers = if cfg.workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        cfg.workers
    };
    let tls = cfg.tls.clone();
    if tls.is_some() {
        tracing::info!(
            "TLS enabled (rustls, {})",
            crate::tls::tls_versions().join(" / ")
        );
    }
    tracing::info!(
        "fasts3d S3 http listening on {} ({} workers, SO_REUSEPORT, tls={})",
        cfg.listen,
        workers,
        tls.is_some()
    );

    // 证书热加载轮询(每 5s 查 mtime;换证书不断连,新连接用新配置)
    if let Some(state) = &tls {
        let state = state.clone();
        std::thread::Builder::new()
            .name("fs3-tls-reload".into())
            .spawn(move || loop {
                std::thread::sleep(std::time::Duration::from_secs(5));
                state.reload_if_changed();
            })
            .map_err(std::io::Error::other)?;
    }

    let admission = Admission::new(cfg.max_inflight_bytes);
    let header_timeout = cfg.header_timeout;
    let idle_timeout = cfg.idle_timeout;
    let mut handles = Vec::new();
    for w in 0..workers {
        let service = service.clone();
        let listen = cfg.listen;
        let admission = admission.clone();
        let tls = tls.clone();
        handles.push(std::thread::spawn(move || {
            worker_main(
                service,
                listen,
                admission,
                header_timeout,
                idle_timeout,
                tls,
                w,
            );
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn worker_main(
    service: Arc<S3Service>,
    listen: SocketAddr,
    admission: Arc<Admission>,
    header_timeout: std::time::Duration,
    idle_timeout: std::time::Duration,
    tls: Option<Arc<TlsState>>,
    worker_id: usize,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_io()
        .enable_time()
        .thread_name(format!("fs3-http-{worker_id}"))
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let listener = bind_reuseport(listen).expect("bind SO_REUSEPORT");
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let service = service.clone();
                    let admission = admission.clone();
                    let tls = tls.clone();
                    tokio::spawn(async move {
                        match &tls {
                            // TLS:先握手(慢路径;失败即断开);零拷贝禁用
                            Some(state) => match state.acceptor().accept(stream).await {
                                Ok(s) => {
                                    if let Err(e) = handler::serve_connection_tls(
                                        service,
                                        admission,
                                        s,
                                        header_timeout,
                                        idle_timeout,
                                    )
                                    .await
                                    {
                                        tracing::debug!("connection {peer} ended: {e}");
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("tls handshake {peer} failed: {e}");
                                }
                            },
                            // 明文:零拷贝路径(B3/D2)
                            None => {
                                if let Err(e) = handler::serve_connection(
                                    service,
                                    admission,
                                    stream,
                                    header_timeout,
                                    idle_timeout,
                                )
                                .await
                                {
                                    tracing::debug!("connection {peer} ended: {e}");
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    });
}

/// SocketAddr → libc sockaddr_storage。
fn addr_to_sockaddr(addr: SocketAddr) -> libc::sockaddr_storage {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sa as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in>(),
                );
            }
        }
        SocketAddr::V6(v6) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: 0,
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sa as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in6>(),
                );
            }
        }
    }
    storage
}

/// SO_REUSEPORT 绑定:每 worker 共享同一端口,内核按四元组哈希分流。
fn bind_reuseport(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // SAFETY: 标准 socket 调用。
    let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let one: libc::c_int = 1;
    // SAFETY: fd 有效。
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // SAFETY: fd 有效。
        unsafe { libc::close(fd) };
        return Err(err);
    }
    // 手动 bind/listen(SO_REUSEPORT 必须在 bind 前设置)
    let sockaddr: libc::sockaddr_storage = addr_to_sockaddr(addr);
    // SAFETY: sockaddr 有效;绑定。
    let rc = unsafe {
        libc::bind(
            fd,
            &sockaddr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // SAFETY: fd 有效。
        unsafe { libc::close(fd) };
        return Err(err);
    }
    // SAFETY: fd 有效。
    let rc = unsafe { libc::listen(fd, 1024) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // SAFETY: fd 有效。
        unsafe { libc::close(fd) };
        return Err(err);
    }
    let std_listener = {
        use std::os::fd::FromRawFd;
        // SAFETY: fd 为新建 socket,交由 std 管理。
        unsafe { std::net::TcpListener::from_raw_fd(fd) }
    };
    std_listener.set_nonblocking(true)?;
    TcpListener::from_std(std_listener)
}
