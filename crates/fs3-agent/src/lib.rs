//! FastS3 纳管 agent(M14 G1-1;ADR-17 DV1)。
//!
//! 单机 `fasts3d` 的可选增值模块:**feature-gate 默认关**(fs3d 的 `agent`
//! feature 未启用时本 crate 不编译,默认二进制与 v1.x 行为/性能零差异)。
//!
//! 职责(设计 §7.1、TODO M14 G1-1):
//! - **出站 mTLS** 连接中心(agent 主动出站,节点不暴露任何入站端口;
//!   双向 TLS + 每节点独立证书,红线 DESIGN-FUTURE §9.4 #3);
//! - **心跳/健康/版本上报**(POST /v2/center/heartbeat);
//! - **指标/审计流式上报**(批量 POST /v2/center/streams;数据取自本地
//!   admin 通道 /v1/admin/status + /v1/admin/metrics + /v1/admin/audit,
//!   "agent 化 = 在 admin 通道之上加一层远程化",§7.1.1);
//! - **下发接收**(GET /v2/center/desired 拉取 per-node 下发账本,
//!   断线重连 `mode=full` 全量对账)与**本地裁决执行**(经本地 admin 通道
//!   的既有端点应用,裁决权威 = 本机引擎,ADR-17 DV1-2),结果回执
//!   (POST /v2/center/results,含 key.create 的 secret **仅一次**回显)。
//!
//! 线程模型:独立 tokio 运行时单线程("fs3-agent"),不触碰数据面热路径;
//! 上行走本 crate 自带的极小 HTTP/1.1 客户端(每请求新建连接,mTLS),
//! 不引入 reqwest/hyper-rustls 等新依赖(依赖最小化,§9.3)。

mod agent;
mod apply;
mod center;
mod config;
mod http1;
mod local;
mod sync_exec;
mod tls;

#[cfg(test)]
#[path = "test_util.rs"]
mod test_util;

pub use agent::{Agent, AgentHandle};
pub use config::AgentConfig;
pub use local::LocalAdmin;
