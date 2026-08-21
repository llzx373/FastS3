//! M7 / I5 内嵌控制台静态托管(`fasts3d serve --web-root <dist>`)。
//!
//! 数据面内嵌 Web 控制台构建产物(设计 §7.4:「控制台构建产物为纯静态资源,
//! 须可被 fasts3d --web-root 内嵌托管」)。SPA 回退:未知路径 → index.html。
//!
//! 与 S3 协议的区分由调用方(handler)完成:带 Authorization / 预签名查询
//! / 首段为既有桶的请求一律走 S3;其余 GET/HEAD 按静态资源处理。

use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Response, StatusCode};

use crate::handler;

const MIME: &[(&str, &str)] = &[
    (".html", "text/html; charset=utf-8"),
    (".js", "text/javascript"),
    (".mjs", "text/javascript"),
    (".css", "text/css"),
    (".json", "application/json"),
    (".svg", "image/svg+xml"),
    (".png", "image/png"),
    (".jpg", "image/jpeg"),
    (".jpeg", "image/jpeg"),
    (".gif", "image/gif"),
    (".webp", "image/webp"),
    (".ico", "image/x-icon"),
    (".woff", "font/woff"),
    (".woff2", "font/woff2"),
    (".ttf", "font/ttf"),
    (".txt", "text/plain; charset=utf-8"),
    (".map", "application/json"),
    (".webmanifest", "application/manifest+json"),
];

fn content_type(path: &Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default();
    let ext = format!(".{ext}");
    for (e, m) in MIME {
        if *e == ext {
            return m;
        }
    }
    "application/octet-stream"
}

/// 把 URL 路径安全地解析到 web_root 下(防目录穿越)。
/// 任何 `..` 段或绝对路径一律拒绝;返回 None 表示不合法。
fn resolve_safe(root: &Path, url_path: &str) -> Option<PathBuf> {
    let mut out = root.to_path_buf();
    let mut saw_traversal = false;
    for seg in url_path.split('/') {
        match seg {
            "" | "." => continue,
            ".." => {
                saw_traversal = true;
                break; // 不真正回退:直接拒绝(任何 .. 段都不放行)
            }
            seg => out.push(seg),
        }
    }
    if saw_traversal
        || !out.starts_with(root)
        || out.components().any(|c| matches!(c, Component::ParentDir))
    {
        return None;
    }
    Some(out)
}

/// 静态资源应答:存在 → 文件;不存在/目录 → SPA 回退 index.html。
/// 返回 None 仅当文件系统错误(调用方按 404 兜底)。
pub fn serve_static(
    root: &Path,
    url_path: &str,
    method: &str,
) -> Option<Response<handler::RespBody>> {
    fn body_bytes(b: Vec<u8>) -> handler::RespBody {
        Full::new(Bytes::from(b))
            .map_err(|e| std::io::Error::other(e.to_string()))
            .boxed()
    }
    fn plain(status: StatusCode, msg: &str) -> Response<handler::RespBody> {
        Response::builder()
            .status(status)
            .header("content-type", "text/plain; charset=utf-8")
            .body(body_bytes(msg.as_bytes().to_vec()))
            .unwrap()
    }

    let path = match resolve_safe(root, url_path) {
        Some(p) => p,
        None => return Some(plain(StatusCode::FORBIDDEN, "forbidden")),
    };
    let is_dir = match std::fs::metadata(&path) {
        Ok(m) => m.is_dir(),
        Err(_) => false,
    };
    // 目录或不存在:SPA 回退(前端路由由 index.html 接管)
    let file = if is_dir {
        path.join("index.html")
    } else if path.exists() {
        path
    } else {
        root.join("index.html")
    };
    let data = match std::fs::read(&file) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Some(plain(
                StatusCode::NOT_FOUND,
                &format!("not found: {url_path}"),
            ));
        }
        Err(e) => {
            tracing::warn!("static read {} failed: {e}", file.display());
            return Some(plain(StatusCode::INTERNAL_SERVER_ERROR, "read error"));
        }
    };
    let builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type(&file))
        .header("content-length", data.len().to_string());
    if method == "HEAD" {
        return Some(builder.body(empty_body()).unwrap());
    }
    Some(builder.body(body_bytes(data)).unwrap())
}

fn empty_body() -> handler::RespBody {
    Full::new(Bytes::new())
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_webroot() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<html>console</html>").unwrap();
        std::fs::create_dir_all(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/app.js"), "console.log(1)").unwrap();
        dir
    }

    fn collect(b: handler::RespBody) -> Vec<u8> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move { b.collect().await.unwrap().to_bytes().to_vec() })
    }

    #[test]
    fn serves_index_and_assets() {
        let dir = tmp_webroot();
        let r = serve_static(dir.path(), "/", "GET").unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(String::from_utf8_lossy(&collect(r.into_body())).contains("<html>console</html>"));

        let r = serve_static(dir.path(), "/assets/app.js", "GET").unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers().get("content-type").unwrap(), "text/javascript");
        let body = collect(r.into_body());
        assert_eq!(body, b"console.log(1)");
    }

    #[test]
    fn spa_fallback_and_head() {
        let dir = tmp_webroot();
        // 前端路由路径(如 /buckets/abc)→ index.html
        let r = serve_static(dir.path(), "/buckets/abc", "GET").unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(String::from_utf8_lossy(&collect(r.into_body())).contains("<html>console</html>"));

        // HEAD:无 body,有 content-length
        let r = serve_static(dir.path(), "/nope.js", "HEAD").unwrap();
        assert_eq!(r.headers().get("content-length").unwrap(), "20"); // index.html 字节数
    }

    #[test]
    fn rejects_traversal() {
        let dir = tmp_webroot();
        for path in ["/../etc/passwd", "/assets/../../etc/passwd", "//../x"] {
            let r = serve_static(dir.path(), path, "GET").unwrap();
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "path {path}");
        }
    }

    #[test]
    fn missing_root_404() {
        let dir = tempfile::tempdir().unwrap(); // 空目录,无 index.html
        let r = serve_static(dir.path(), "/", "GET").unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
    }
}
