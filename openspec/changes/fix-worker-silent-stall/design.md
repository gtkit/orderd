## Context

`QueryWorker` 与 `RefundQueryWorker` 都用"认领 → 查单 → 按退避重排"的循环收敛不确定状态。两处的重排路径缺少可观测性：

- `query.go` 的 CLOSING + 渠道报未支付分支：`_ = gw.Close(...)` 丢弃关单错误，随后无条件重排。该分支没有次数阈值（`MaxQueries` 只用在"渠道无此单"分支）。
- `refundquery.go`：`RefundQueryConfig` 没有次数字段，只要退款仍 PROCESSING 就无条件重排。渠道稳定返回"处理中"时连日志都不产生。

`Delivery` worker 已经持有 `alert.Alerter` 并在死信时上报（`worker.go:68`、`worker.go:117`），本变更沿用同一形状。`alert.Alerter` 由 runtime 注入 `loggerAlerter{}` 值类型，测试注入 `alert.Nop{}`，既有代码直接调用不做 nil 判断，本变更保持一致。

## Goals / Non-Goals

**Goals:**
- 关单失败可从日志定位到具体渠道与商户单号。
- 支付尝试长期滞留 CLOSING、退款长期滞留 PROCESSING 时产生一次告警，使人工介入成为可能。
- 告警不随轮询重复放大。

**Non-Goals:**
- 支付与退款状态机的迁移规则、DDL、proto 契约、对外 API 均不在本次范围内。
- 阈值不改变收敛路径：达阈值后仍由渠道单自然过期或人工介入收敛。

## Decisions

### 达阈值后保持状态并继续低频查询

达到阈值时上报告警，尝试/退款的状态保持不变，继续按 `MaxBackoff` 低频查询。

替代方案是达阈值后落本地终态（尝试直接 CLOSED、退款直接 FAILED），不采用：这会把"结果未知"变成"错误的确定"。支付侧本地关单后用户仍可能在渠道侧完成付款，资金入账时尝试已 CLOSED，只能落异常流水并人工退款；退款侧本地判失败时钱可能实际已退，导致账实不符。两者都是用可观测性缺口换出一个资损风险，而告警足以达成本变更的目标。

### 告警在跨过阈值那一次触发

判定用 `QueryCount + 1 == MaxQueries`（重排时 `QueryCount++`，因此该等式在整个生命周期内只成立一次），而不是 `>=`。后者会在每一轮重排都告警，把一个滞留对象变成持续告警源。

### 查单 worker 复用既有 `QueryConfig.MaxQueries`

CLOSING + 未支付分支的告警阈值复用同一字段，不新增配置项。该字段原语义是"UNKNOWN 连续查不到达此次数后关闭尝试"，与本分支的"查询多次仍未收敛"量级一致（默认 6 次），且项目基线要求单次使用的量不额外开配置项。字段注释同步说明两处用途。

`RefundQueryConfig` 无此字段，新增 `MaxQueries` 并给出与 `QueryConfig` 一致的默认值。

### 告警 kind 命名

沿用既有的下划线小写风格（`outbox_dead_letter`、`refund_amount_mismatch`）：支付侧 `attempt_close_stalled`，退款侧 `refund_query_stalled`。

## Risks / Trade-offs

- **复用 `MaxQueries` 使一个字段承担两种语义（关闭阈值 / 告警阈值）** → 注释写明双重用途；两处都是"查询多次仍未收敛"的同类判定，量级要求一致，调大调小的方向也一致。
- **阈值告警只发一次，若运维漏看则滞留对象继续沉默** → 关单错误每轮都记日志，滞留对象在日志中持续可见；告警本身承担"跨过阈值"这一次的通知职责。
- **`QueryCount` 在其它分支也会自增，可能使 CLOSING 分支的告警提前触发** → 提前告警指向的仍是同一个"多次查询未收敛"的对象，不产生误报。
