//! 零拷贝读路径(B3/D2):sendfile(镜像文件)/ splice(裸设备)。
//!
//! 机制:连接建立时生成随机 nonce,先嗅探协议(h2 preface → 禁用零拷贝,
//! 因 h2 帧内嵌标记字节会损坏数据流);h1 响应体以
//! `[nonce(8) | fd(4) | off(8) | len(8)]` 28 字节"标记帧"取代数据帧;
//! 包裹 socket 的 `ZeroCopyIo` 在 `poll_write` 中**扫描**缓冲寻找 nonce,
//! 命中且 fd 可信 → 对 socket fd 直接 sendfile/splice(数据零用户态拷贝),
//! 其余字节正常透传。标记带连接 nonce(2^-64/帧)且 fd 需白名单,
//! 客户端对象数据无法伪造零拷贝指令。
//!
//! 能力探测:按 fd `fstat` 选路(普通文件 → sendfile;块设备 → splice);
//! 不可零拷贝的 fd 由渲染侧直接走缓冲路径(不产出标记)。

use std::io;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// 标记帧长度。
pub const MARKER_LEN: usize = 28;
/// 填充指令 fd 值:标记后跟 pad_count 个零字节,由包装层丢弃
/// (用于对齐 hyper 的 content-length 记账)。
pub const PAD_FD: i32 = -2;
/// h2 preface(prior-knowledge)。
pub const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// 每连接零拷贝上下文:nonce 由连接创建时生成,响应体构造侧必须使用
/// 同一 nonce(经 handle/render 下传)。
#[derive(Debug, Clone, Copy)]
pub struct ZeroCtx {
    pub nonce: [u8; 8],
}

impl ZeroCtx {
    pub fn new() -> Self {
        let mut nonce = [0u8; 8];
        let _ = fs3_core::random_bytes(&mut nonce);
        ZeroCtx { nonce }
    }

    /// 构造标记帧。
    pub fn marker(&self, fd: i32, offset: u64, len: u64) -> [u8; MARKER_LEN] {
        let mut m = [0u8; MARKER_LEN];
        m[..8].copy_from_slice(&self.nonce);
        m[8..12].copy_from_slice(&fd.to_be_bytes());
        m[12..20].copy_from_slice(&offset.to_be_bytes());
        m[20..28].copy_from_slice(&len.to_be_bytes());
        m
    }
}

impl Default for ZeroCtx {
    fn default() -> Self {
        Self::new()
    }
}

/// 设备 fd 能力探测:0=不支持,1=sendfile,2=splice。
pub fn probe_fd_capability(fd: i32) -> u8 {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        return 0;
    }
    match st.st_mode & libc::S_IFMT {
        libc::S_IFREG => 1,
        libc::S_IFBLK => 2,
        _ => 0,
    }
}

/// 包裹 socket:正常读写透传;扫描识别标记帧 → sendfile/splice。
pub struct ZeroCopyIo {
    inner: TcpStream,
    nonce: [u8; 8],
    /// 已读未交付的嗅探字节(协议判定后继续交付)。
    sniff: Vec<u8>,
    /// 协议是否已判定。
    protocol_known: bool,
    /// h2(prior-knowledge)→ 禁用零拷贝。
    is_h2: bool,
    /// 待写出的普通字节(尚未落 socket)。
    pending: Vec<u8>,
    /// 进行中的零拷贝等待(标记已消费;oneshot 保持存活以接收唤醒)。
    zc_wait: Option<tokio::sync::oneshot::Receiver<std::io::Result<()>>>,
    /// 最近一次追加的 (buf_ptr, len):Pending 重试去重。
    buf_id: Option<(usize, usize)>,
    /// 零拷贝专用 socket fd(dup;阻塞 sendfile 线程使用)。
    zfd: Option<i32>,
    /// sendfile 线程任务队列(惰性启动)。
    zc_tx: Option<std::sync::mpsc::SyncSender<ZcJob>>,
    /// 待丢弃的填充零字节(帧可能分片到达;不等齐即可消费)。
    pad_remaining: usize,
}

/// sendfile 线程任务:阻塞式发送 (fd, off, len);fd == PAD_FD 为无操作
/// (对齐 hyper 记账的填充帧)。
struct ZcJob {
    fd: i32,
    off: u64,
    len: u64,
    done: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

/// 阻塞 sendfile 线程主循环(数据零拷贝;socket 阻塞模式仅线程内独占使用)。
fn zc_sender_loop(rx: std::sync::mpsc::Receiver<ZcJob>, sock: i32) {
    tracing::debug!("zc sender thread started (sock={sock})");
    loop {
        let job = match rx.recv() {
            Ok(j) => j,
            Err(_) => return, // 所有发送端关闭 → 连接结束
        };
        tracing::debug!("zc job fd={} off={} len={}", job.fd, job.off, job.len);
        let result = if job.fd == PAD_FD {
            Ok(())
        } else {
            // 临时切阻塞(dup fd 与原 fd 共享 open file description;
            // 发送期间 hyper 对该连接已挂起,独占使用)
            let flags = unsafe { libc::fcntl(sock, libc::F_GETFL) };
            let rc = unsafe { libc::fcntl(sock, libc::F_SETFL, flags & !libc::O_NONBLOCK) };
            let r = if rc != 0 {
                Err(std::io::Error::last_os_error())
            } else {
                blocking_sendfile(sock, job.fd, job.off, job.len)
            };
            let _ = unsafe { libc::fcntl(sock, libc::F_SETFL, flags) };
            if let Err(e) = &r {
                // 客户端中途断开(EPIPE/ECONNRESET)属正常:降级为 debug
                match e.kind() {
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset => {
                        tracing::debug!("zc job aborted (client gone): {e}");
                    }
                    _ => tracing::warn!("zc job failed: {e} (fcntl={rc})"),
                }
            }
            tracing::debug!(
                "zc job done len={} -> {:?}",
                job.len,
                r.as_ref().map(|_| "ok")
            );
            r
        };
        let _ = job.done.send(result);
    }
}

/// 阻塞式 sendfile 循环(至 len 字节全部发出)。
fn blocking_sendfile(sock: i32, src: i32, mut off: u64, mut len: u64) -> std::io::Result<()> {
    let method = probe_fd_capability(src);
    match method {
        1 => {
            while len > 0 {
                let mut o = off as i64;
                let n = unsafe { libc::sendfile(sock, src, &mut o, len as usize) };
                if n < 0 {
                    let e = std::io::Error::last_os_error();
                    if e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // 写缓冲满:固定短眠后重试(部分内核/WSL 上非阻塞
                        // sendfile 的 EAGAIN 与可写事件不同步;专用线程可眠)
                        std::thread::sleep(std::time::Duration::from_micros(50));
                        continue;
                    }
                    return Err(e);
                }
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "sendfile made no progress",
                    ));
                }
                off += n as u64;
                len -= n as u64;
            }
            Ok(())
        }
        _ => splice_all(sock, src, &mut off, len),
    }
}

/// 阻塞式 splice 循环(dev → pipe → socket)。
fn splice_all(sock: i32, src: i32, off: &mut u64, mut len: u64) -> std::io::Result<()> {
    static PIPE: std::sync::Mutex<Option<(i32, i32)>> = std::sync::Mutex::new(None);
    let mut guard = PIPE.lock().unwrap();
    if guard.is_none() {
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        *guard = Some((fds[0], fds[1]));
    }
    let (pr, pw) = guard.unwrap();
    while len > 0 {
        let want = len.min(1 << 20);
        let n = unsafe {
            libc::splice(
                src,
                &mut (*off as i64),
                pw,
                std::ptr::null_mut(),
                want as usize,
                libc::SPLICE_F_MOVE,
            )
        };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "splice no progress",
            ));
        }
        let mut sent = 0usize;
        while sent < n as usize {
            let w = unsafe {
                libc::splice(
                    pr,
                    std::ptr::null_mut(),
                    sock,
                    std::ptr::null_mut(),
                    n as usize - sent,
                    0,
                )
            };
            if w < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if w == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "splice no progress",
                ));
            }
            sent += w as usize;
        }
        *off += n as u64;
        len -= n as u64;
    }
    Ok(())
}

impl ZeroCopyIo {
    pub fn new(inner: TcpStream, ctx: &ZeroCtx) -> Self {
        // dup socket fd:阻塞 sendfile 线程专用(与 inner 共享同一连接)
        let dup = unsafe { libc::dup(inner.as_raw_fd()) };
        ZeroCopyIo {
            inner,
            nonce: ctx.nonce,
            sniff: Vec::with_capacity(H2_PREFACE.len()),
            protocol_known: false,
            is_h2: false,
            pending: Vec::new(),
            zc_wait: None,
            pad_remaining: 0,
            buf_id: None,
            zfd: if dup >= 0 { Some(dup) } else { None },
            zc_tx: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn dup_fd(&self) -> Option<i32> {
        self.zfd
    }

    fn close_dup(&mut self) {
        if let Some(fd) = self.zfd.take() {
            // SAFETY: fd 由 dup() 得到,仅本结构持有。
            unsafe { libc::close(fd) };
        }
    }

    /// 派发零拷贝任务并等待完成(阻塞 sendfile 线程执行;此处仅挂起)。
    /// receiver 必须存入 self(跨 Pending 存活),否则唤醒丢失。
    fn dispatch_zc(
        &mut self,
        cx: &mut Context<'_>,
        fd: i32,
        off: u64,
        len: u64,
    ) -> Poll<std::io::Result<()>> {
        use std::future::Future as _;
        // 已有在途任务:直接轮询其完成
        if let Some(rx) = &mut self.zc_wait {
            return match std::pin::Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(r)) => {
                    self.zc_wait = None;
                    Poll::Ready(r)
                }
                Poll::Ready(Err(_)) => {
                    self.zc_wait = None;
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "zc sender dropped",
                    )))
                }
                Poll::Pending => Poll::Pending,
            };
        }
        let sock = match self.zfd {
            Some(s) => s,
            None => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "zerocopy fd unavailable",
                )))
            }
        };
        // 惰性启动发送线程
        if self.zc_tx.is_none() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<ZcJob>(8);
            let _ = std::thread::Builder::new()
                .name("fs3-zc-send".into())
                .spawn(move || zc_sender_loop(rx, sock));
            self.zc_tx = Some(tx);
        }
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<std::io::Result<()>>();
        let job = ZcJob {
            fd,
            off,
            len,
            done: done_tx,
        };
        let tx = self.zc_tx.as_ref().unwrap();
        tracing::debug!(
            "dispatch_zc fd={fd} off={off} len={len} pending={}",
            self.pending.len()
        );
        // 有界通道:满则让出重试(标记保留在 pending,重试不丢任务)
        if let Err(e) = tx.try_send(job) {
            match e {
                std::sync::mpsc::TrySendError::Full(_) => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                std::sync::mpsc::TrySendError::Disconnected(_) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "zc sender thread gone",
                    )))
                }
            }
        }
        // 发送成功:消费标记(前缀字节已由调用方 write_prefix 先行写出)
        self.pending.drain(..MARKER_LEN);
        let mut rx = done_rx;
        match std::pin::Pin::new(&mut rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "zc sender dropped",
            ))),
            Poll::Pending => {
                self.zc_wait = Some(rx);
                Poll::Pending
            }
        }
    }

    /// 尽力写 pending[..n](全部写完才返回;WouldBlock → Pending)。
    fn write_prefix(&mut self, cx: &mut Context<'_>, n: usize) -> Poll<io::Result<()>> {
        loop {
            if n == 0 {
                return Poll::Ready(Ok(()));
            }
            tracing::debug!("write_prefix n={n} pending={}", self.pending.len());
            match Pin::new(&mut self.inner)
                .poll_write(cx, &self.pending[..n.min(self.pending.len())])
            {
                Poll::Ready(Ok(w)) => {
                    self.pending.drain(..w);
                    let n = n - w;
                    if n == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// 找 pending 中第一个 nonce 位置。
    fn find_nonce(&self) -> Option<usize> {
        self.pending.windows(8).position(|w| w == self.nonce)
    }

    /// 推进状态机直至阻塞;Ready(Ok(())) = 本次输入已消费完(可能仍需更多输入)。
    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            // a) 填充零字节丢弃(分片到达,按需消费)
            if self.pad_remaining > 0 {
                let take = self.pad_remaining.min(self.pending.len());
                self.pending.drain(..take);
                self.pad_remaining -= take;
                if self.pad_remaining > 0 {
                    // 等待更多输入(下一次 poll_write 续)
                    return Poll::Ready(Ok(()));
                }
                continue;
            }
            // b) 在途零拷贝任务
            if self.zc_wait.is_some() {
                match self.dispatch_zc(cx, 0, 0, 0) {
                    Poll::Ready(Ok(())) => continue,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            // c) 扫描标记
            match self.find_nonce() {
                Some(pos) if pos + MARKER_LEN <= self.pending.len() => {
                    let fd =
                        i32::from_be_bytes(self.pending[pos + 8..pos + 12].try_into().unwrap());
                    let off =
                        u64::from_be_bytes(self.pending[pos + 12..pos + 20].try_into().unwrap());
                    let len =
                        u64::from_be_bytes(self.pending[pos + 20..pos + 28].try_into().unwrap());
                    if fd == PAD_FD && !self.is_h2 {
                        // 填充指令:写前字节 + 消费标记与已到零字节
                        match self.write_prefix(cx, pos) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                        self.pending.drain(..MARKER_LEN);
                        let pad = off as usize;
                        let take = pad.min(self.pending.len());
                        self.pending.drain(..take);
                        self.pad_remaining = pad - take;
                        tracing::debug!("pad marker pos={pos} pad={pad} take={take}");
                        if self.pad_remaining > 0 {
                            return Poll::Ready(Ok(()));
                        }
                        continue;
                    }
                    if !self.is_h2 && is_trusted_fd(fd) {
                        // 先写标记前字节(保证响应顺序)
                        match self.write_prefix(cx, pos) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                        // 派发零拷贝(发送成功后在内部消费标记)
                        match self.dispatch_zc(cx, fd, off, len) {
                            Poll::Ready(Ok(())) => continue,
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                    } else {
                        // 伪 nonce:按普通数据写出后继续扫描
                        match self.write_prefix(cx, pos + 8) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                }
                Some(pos) => {
                    // 尾部部分标记:冲刷其前全部字节,标记字节挂起等待续接
                    match self.write_prefix(cx, pos) {
                        Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                None => {
                    // 无标记:全部冲刷。hyper 单次 poll_write 交付完整缓冲,
                    // 标记不可能跨写拆分,无需保留尾部(保留会在响应结束时
                    // 无 poll_flush 触发而泄漏)。
                    match self.write_prefix(cx, self.pending.len()) {
                        Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl Drop for ZeroCopyIo {
    fn drop(&mut self) {
        self.close_dup();
    }
}

impl AsyncRead for ZeroCopyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        // 先交付嗅探缓冲
        if !this.sniff.is_empty() {
            let n = this.sniff.len().min(buf.remaining());
            buf.put_slice(&this.sniff[..n]);
            this.sniff.drain(..n);
            return Poll::Ready(Ok(()));
        }
        // 协议未判定:读满 24 字节嗅探
        if !this.protocol_known {
            let mut tmp = [0u8; H2_PREFACE.len()];
            let mut rbuf = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.inner).poll_read(cx, &mut rbuf) {
                Poll::Ready(Ok(())) => {
                    let n = rbuf.filled().len();
                    this.sniff.extend_from_slice(&tmp[..n]);
                    if n == 0 || this.sniff.len() >= H2_PREFACE.len() {
                        this.protocol_known = true;
                        this.is_h2 = this.sniff.as_slice().starts_with(H2_PREFACE);
                    }
                    if !this.sniff.is_empty() {
                        let take = this.sniff.len().min(buf.remaining());
                        buf.put_slice(&this.sniff[..take]);
                        this.sniff.drain(..take);
                    }
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        } else {
            Pin::new(&mut this.inner).poll_read(cx, buf)
        }
    }
}

impl AsyncWrite for ZeroCopyIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        tracing::debug!(
            len = buf.len(),
            waiting = this.zc_wait.is_some(),
            pending = this.pending.len(),
            "zc poll_write"
        );
        // Pending 重试去重(hyper 重试同一缓冲)
        let id = (buf.as_ptr() as usize, buf.len());
        if this.buf_id != Some(id) {
            this.pending.extend_from_slice(buf);
            this.buf_id = Some(id);
        }
        match this.drain(cx) {
            Poll::Ready(Ok(())) => {
                // 本次缓冲已全部消费(hyper 按返回长度推进)
                this.buf_id = None;
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(e)) => {
                tracing::warn!("zc poll_write error: {e}");
                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        // 丢弃残余填充零字节(不得写入客户端)
        if this.pad_remaining > 0 {
            let take = this.pad_remaining.min(this.pending.len());
            this.pending.drain(..take);
            this.pad_remaining -= take;
        }
        // 完成进行中的零拷贝
        if this.zc_wait.is_some() {
            match this.dispatch_zc(cx, 0, 0, 0) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        // 尾部保留字节(超时/响应结束)全部写出
        loop {
            if this.pending.is_empty() {
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.inner).poll_write(cx, &this.pending) {
                Poll::Ready(Ok(w)) => {
                    this.pending.drain(..w);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// 可信 fd 白名单(引擎设备 fd;serve 启动时注册)。
static TRUSTED_FDS: std::sync::Mutex<Vec<i32>> = std::sync::Mutex::new(Vec::new());

/// 注册可信 fd(引擎设备)。
pub fn register_trusted_fd(fd: i32) {
    let mut v = TRUSTED_FDS.lock().unwrap();
    if !v.contains(&fd) {
        v.push(fd);
    }
}

/// 设备移除/关闭后摘除,避免内核复用 fd 号被当成 sendfile 源。
pub fn unregister_trusted_fd(fd: i32) {
    TRUSTED_FDS.lock().unwrap().retain(|&x| x != fd);
}

#[cfg(test)]
pub(crate) fn is_trusted_fd_for_test(fd: i32) -> bool {
    is_trusted_fd(fd)
}

fn is_trusted_fd(fd: i32) -> bool {
    TRUSTED_FDS.lock().unwrap().contains(&fd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn zero_copy_io_drop_closes_dup_fd() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let ctx = ZeroCtx::new();
        let zc = ZeroCopyIo::new(server, &ctx);
        let fd = zc.dup_fd().expect("dup");
        drop(zc);
        let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(rc, -1, "dup fd must be closed on Drop");
        drop(client);
    }

    #[test]
    fn trusted_fds_deregister_on_connection_drop() {
        let fd = 424242;
        register_trusted_fd(fd);
        assert!(is_trusted_fd_for_test(fd));
        unregister_trusted_fd(fd);
        assert!(!is_trusted_fd_for_test(fd));
        register_trusted_fd(fd);
        unregister_trusted_fd(fd);
        // 复用同号:摘除后不得仍信任
        assert!(!is_trusted_fd_for_test(fd));
    }
}
