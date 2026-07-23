# Proposal: restructure-internal-layout

## Why

`internal/` 有 19 个顶层包,其中一堆平铺的小支撑包(account/alert/auth/bill/endpoint/secret/sqlutil/webhook)
散在根目录,比对齐蓝本 pay360 的 7 目录(bootstrap/models/module/pkg/platform/repository/runtime)明显更乱、
可见范围边界不清。按 harness architecture.md 收敛:纯工具入 `internal/pkg`,跨模块轻量 port 入 `internal/shared`。
纯结构调整,**不改任何运行行为、契约、DB**。

## What Changes

顶层包归类(仅移动 + 改 import,不改逻辑):

| 现位置 | 目标 | 理由 |
|---|---|---|
| `internal/auth` | `internal/pkg/auth` | JWT 验签,纯工具无业务 |
| `internal/sqlutil` | `internal/pkg/sqlutil` | sql.Null* 助手,纯工具 |
| `internal/webhook` | `internal/pkg/webhook` | SSRF 守卫 + HMAC 发送,基础设施工具 |
| `internal/account` | `internal/shared/account` | 跨模块支付账号 port(order/payment/refund 共用) |
| `internal/endpoint` | `internal/shared/endpoint` | 跨模块回调端点 port |
| `internal/bill` | `internal/shared/bill` | 跨模块账单源 port(reconcile 用) |
| `internal/secret` | `internal/shared/secret` | 跨模块密钥解析 port |
| `internal/alert` | `internal/shared/alert` | 跨模块告警 port(payment/refund/reconcile 用) |

保留在顶层(已对齐或属 orderd 领域层,非"乱"):`bootstrap/models/module/pkg/platform/repository/runtime`
(与 pay360 一致)+ `store`(持久化 UoW 端口)+ `paygateway`(网关 port + 适配,内聚)+ `appmeta`(常量,
guide 明列于 internal 顶层)+ `apptest`(集成测试替身,仅测试)。

结果:19 → 12 顶层包,平铺支撑包全部归入 pkg/shared。

## Impact

- **行为/契约/DB**:零改动。纯包移动 + import 路径更新 + 架构文档同步。
- **可见范围**:`pkg`=纯基础包(任何层可 import);`shared`=跨模块轻量 port/类型(guide 认可)。
- **风险**:机械改动,回归风险来自 import 遗漏——每阶段 `go build ./... + golangci-lint + go test -race` 全绿方推进。
- **分阶段**(见 tasks.md),先动纯工具(pkg)再动 port(shared),最后同步文档 + check-architecture。
