# code-layout

## ADDED Requirements

### Requirement: 支撑包按可见范围归类
系统的 `internal/` 布局 SHALL 收敛到 harness architecture.md 的分层:纯工具(无业务、无协议、无 gorm/redis 依赖)
放 `internal/pkg`;跨模块共享的轻量 port/类型放 `internal/shared`;平铺的支撑小包 MUST NOT 散落在 `internal/` 根目录。
移动 MUST 纯结构化,不得改变任何运行行为、对外契约或 DB。

#### Scenario: 纯工具归入 pkg
- **WHEN** 一个包无业务逻辑、无协议依赖(如 JWT 验签、sql 助手、SSRF/HMAC 工具)
- **THEN** 它位于 `internal/pkg/<name>`,可被任意层 import

#### Scenario: 跨模块 port 归入 shared
- **WHEN** 一个 port(接口)被多个 module 共用(如支付账号/回调端点/账单源/密钥/告警)
- **THEN** 它位于 `internal/shared/<name>`,不塞进某个 module 让其它 module 横向 import

### Requirement: 依赖红线不被破坏
重构后 SHALL 保持 architecture.md 的依赖红线:`internal/pkg` MUST NOT 反向 import `internal/module/*`;
`internal/shared` 只承载中立类型/port,MUST NOT 依赖具体基础设施(gorm/gin)或 module。

#### Scenario: pkg 不反向依赖 module
- **WHEN** 检查 `internal/pkg` 下任一包的 import
- **THEN** 不出现 `internal/module/*`
