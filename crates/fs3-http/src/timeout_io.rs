//! H4 连接超时包装:header 30s / idle 60s(DESIGN §9「限额与抗滥用」)。
//!
//! hyper 1.x h1 只提供 `header_read_timeout` 一个计时点,无法区分
//! 「首请求头 30s」与「keep-alive 空闲 60s」。此包装在 socket IO 上叠加
//! 每读截止时间:连接建立后的第一次读(请求头)以 header_timeout 计,
//! 之后每次读(后续请求头 / 请求体分块)以 idle_timeout 计;超时返回
//! `ErrorKind::TimedOut` → hyper 关闭连接。
//!
//! 与 hyper 计时叠加后的净效果:
//! - 首请求头:min(包装 30s,hyper header_read_timeout 60s) = 30s;
//! - keep-alive 空闲:hyper header_read_timeout(设为 idle 60s) = 60s;
//! - 流式请求体慢客户端:每读最多 60s(包装层);
//! - h2 走 hyper 自己的 keep_alive PING(30s 间隔 + 60s 超时)。

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

/// 读截止时间包装器(读操作带超时;写操作透传)。
///
/// `I: Unpin` 即可(ZeroCopyIo<TcpStream> 满足)。
pub struct DeadlinedIo<I> {
    inner: I,
    header_timeout: Duration,
    idle_timeout: Duration,
    /// 是否已读过数据(首读用 header 截止,其后用 idle 截止)。
    data_seen: bool,
    /// 当前读操作的截止定时器。
    deadline: Option<Pin<Box<Sleep>>>,
}

impl<I> DeadlinedIo<I> {
    pub fn new(inner: I, header_timeout: Duration, idle_timeout: Duration) -> Self {
        DeadlinedIo {
            inner,
            header_timeout,
            idle_timeout,
            data_seen: false,
            deadline: None,
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for DeadlinedIo<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Pin<&mut Self> → &mut Self(Self: Unpin)
        let this = self.get_mut();
        let timeout = if this.data_seen {
            this.idle_timeout
        } else {
            this.header_timeout
        };
        // 本轮读的截止定时器(惰性创建)
        if this.deadline.is_none() {
            this.deadline = Some(Box::pin(tokio::time::sleep(timeout)));
        }
        // 先查截止:已超时 → 报错(hyper 收错即断连)
        if let Some(dl) = this.deadline.as_mut() {
            if dl.as_mut().poll(cx).is_ready() {
                this.deadline = None;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "connection read timeout (header 30s / idle 60s)",
                )));
            }
        }
        // 内层读
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                this.deadline = None;
                if buf.filled().is_empty() {
                    // EOF
                    return Poll::Ready(Ok(()));
                }
                this.data_seen = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.deadline = None;
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Poll::Pending;
                }
                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for DeadlinedIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// 带外部截止的读(测试辅助:最外层兜底,防止测试悬挂)。
#[cfg(test)]
pub async fn read_with_timeout<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut [u8],
    timeout: Duration,
) -> io::Result<usize> {
    use tokio::io::AsyncReadExt;
    let mut fut = Box::pin(async {
        let mut rb = ReadBuf::new(buf);
        Pin::new(r).read_buf(&mut rb).await?;
        Ok(rb.filled().len())
    });
    tokio::time::timeout(timeout, &mut fut)
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "outer timeout")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// 首读截止 = header 超时:慢对端(200ms 后才发)+ 100ms header 截止 → 超时
    #[tokio::test]
    async fn first_read_header_timeout_fires() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = sock.write_all(b"GET / HTTP/1.1\r\n\r\n").await;
        });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut io = DeadlinedIo::new(
            client,
            Duration::from_millis(100),
            Duration::from_millis(500),
        );
        let mut buf = [0u8; 64];
        let err = read_with_timeout(&mut io, &mut buf, Duration::from_millis(600)).await;
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    /// 首读命中后,后续读走 idle 截止(慢对端仍在 idle 内 → 正常读到)
    #[tokio::test]
    async fn subsequent_read_uses_idle_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = sock.write_all(b"first").await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = sock.write_all(b"second").await;
        });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut io = DeadlinedIo::new(
            client,
            Duration::from_millis(1000), // header 截止(首读)
            Duration::from_millis(1000), // idle 截止 1s > 300ms,读得到
        );
        let mut buf = [0u8; 5];
        let n = read_with_timeout(&mut io, &mut buf, Duration::from_millis(1500))
            .await
            .unwrap();
        assert_eq!(n, 5);
        let mut buf2 = [0u8; 6];
        let n2 = read_with_timeout(&mut io, &mut buf2, Duration::from_millis(1500))
            .await
            .unwrap();
        assert_eq!(n2, 6);
    }

    /// 后续读超过 idle 截止 → 超时
    #[tokio::test]
    async fn idle_timeout_fires_on_slow_second_phase() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = sock.write_all(b"x").await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            let _ = sock.write_all(b"y").await;
        });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut io = DeadlinedIo::new(
            client,
            Duration::from_millis(1000),
            Duration::from_millis(100), // idle 100ms < 400ms → 超时
        );
        let mut buf = [0u8; 4];
        let n = read_with_timeout(&mut io, &mut buf, Duration::from_millis(1100))
            .await
            .unwrap();
        assert_eq!(n, 1);
        let err = read_with_timeout(&mut io, &mut buf, Duration::from_millis(2000)).await;
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
}
