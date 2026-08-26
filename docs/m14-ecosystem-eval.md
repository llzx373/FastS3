# M14 生态评估:Terraform Provider 与 K8s Operator(T1-1 / T2-1)

> 依据:DESIGN-FUTURE §7.4、TODO M14/T1-1、T2-1。评估结论:**暂不立项
> (持有)**;立项条件 = **≥10 位用户明确需求**(issue 投票 / 企业 IaC 需求
> 证据,与 Beta 反馈闭环同机制)。本文件记录评估依据、范围与立项后就绪面。

## 1. Terraform Provider(T1-1)

### 1.1 现状就绪面(事实)

管理面 admin API 已具备 provider 所需的全部 CRUD(设计 §7.2):

| TF resource(拟定) | admin API(现有) | 状态 |
| --- | --- | --- |
| `fasts3_bucket`(create/read/update/delete + quota) | POST/GET/PATCH/DELETE `/v1/admin/buckets[*]` | ✅ 现成 |
| `fasts3_access_key`(create/read/delete/enable/policy) | POST/GET/PATCH/DELETE `/v1/admin/keys[*]` | ✅ 现成 |
| `fasts3_config`(热字段设置) | GET/PATCH `/v1/admin/config` | ✅ 现成(restart_required 语义需在 provider 文档化) |
| M14 扩展:`fasts3_center_node` / `fasts3_center_op` | `/v2/center/*`(管理面 ops 入账) | ✅ G2-1 现成 |

实现形态(立项时):Terraform Plugin Framework(Go 或 provider SDK),经
admin Bearer token 直连;单账号模型下无需额外身份映射;密钥 secret
仅创建时返回一次 —— provider 状态中**不得持久化 secret**(沿 G1-3 语义,
写入 `sensitive` 输出,文档明示)。

### 1.2 评估结论

- **可行性:高**(API 完备、语义简单、无状态同步问题 —— 桶/密钥为终态
  资源,天然幂等);
- **成本**:Go provider 骨架 + 3 资源 ≈ 1~1.5 人周(评估口径);
- **立项门槛**:≥10 位用户明确需求(issue 投票)。当前无需求证据 →
  **不立项,持有**;
- 追踪:本仓库 issue 模板中的「Terraform provider」投票标签;
  FAQ/文档先行:在 README 记录 admin API 可直接被 Terraform `http`
  provider / 运维脚本调用(无需等待官方 provider)。

## 2. K8s Operator(T2-1)

### 2.1 范围界定(明确)

- **不做 CSI**(容器存储接口 = 块设备语义; FastS3 是 S3 语义层,
  与 CSI 无关 —— 设计 §7.4 明示);
- Operator 范围 = **节点生命周期管理**(StatefulSet/裸机调度 +
  fasts3d 容器编排)+ **桶/密钥 CRD**(CR → 节点 admin API)
  + **监控集成**(ServiceMonitor/Prometheus scrape 已有
  `/v1/admin/metrics` 输出);
- 单机定位不变量:Operator 调度的是「每节点一个 fasts3d 数据面」,
  不引入跨节点一致性(与 v2.0 纳管平台语义一致:中心/CR 只是配置源,
  引擎本地裁决);

### 2.2 评估结论

- **可行性:中**(CRD + controller-runtime 常规工作;但价值取决于用户是否
  以 K8s 承载有状态存储 —— 与 FastS3 裸机/镜像定位存在张力);
- **成本**:CRD ×2 + controller + 监控集成 ≈ 2~3 人周(评估口径);
- **立项门槛**:≥10 位用户明确需求(K8s 部署形态反馈)。当前无需求证据 →
  **不立项,持有**;
- 已就绪面:容器镜像与 K8s 部署资产(deploy/container、deploy/config),
  `/health`/`/ready` 探针、Prometheus 指标输出(监控集成零改);

## 3. 决策记录

| 项 | 决策 | 理由 | 重开条件 |
| --- | --- | --- | --- |
| T1-1 Terraform provider | **不立项(持有)** | 无 ≥10 用户需求证据;admin API 已可直接被脚本/`http` provider 调用 | issue 投票 ≥10 |
| T2-1 K8s Operator | **不立项(持有)**;**明确不做 CSI** | 同上;价值取决于 K8s 承载诉求 | issue 投票 ≥10 / 企业 K8s 部署反馈 |

> 两份持有项随 v2.0 发布报告同步列入「生态评估」章节;需求证据出现后
> 按 DESIGN-FUTURE §11 决策流程立项(无需新 ADR,范围按 §7.4 边界)。