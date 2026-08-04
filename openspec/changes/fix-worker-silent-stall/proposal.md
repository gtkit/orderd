## Why

查单 worker 与退款查单 worker 在"长期无法收敛"时是静默的：支付尝试的关单错误被丢弃、退款查单没有次数上限，两者都不告警。结果是支付尝试可能长期滞留 CLOSING、退款长期滞留 PROCESSING，而运维侧没有任何信号——支付尝试滞留 CLOSING 还会因活跃尝试唯一守卫阻断该订单换渠道重新下单（`PreparePayment` 对已提交网关的活跃尝试直接拒绝）。退款场景尤其敏感：用户在等钱。

## What Changes

- 查单 worker 在 CLOSING 且渠道仍报"未支付"时，关单调用的错误 SHALL 记入日志（含渠道、商户单号），不再丢弃。
- 查单 worker 对该分支的查询次数引入阈值：达阈值时告警一次，随后继续按最大退避低频查询，尝试状态保持不变，由渠道单自然过期或人工介入收敛。
- 退款查单 worker 引入查询次数阈值（`RefundQueryConfig.MaxQueries`）：达阈值时告警一次，随后继续按最大退避低频查询，退款状态保持 PROCESSING。
- 两个 worker 注入 `alert.Alerter`（端口已存在），装配点在 `internal/runtime` / `internal/bootstrap`。

告警按"跨过阈值那一次"触发，避免每轮重复刷。

## Capabilities

### New Capabilities
- `worker-stall-observability`: 查单/退款查单 worker 在长期无法收敛时的日志与告警行为，含阈值语义与达阈值后的状态处理。

### Modified Capabilities
（无：现有 spec 未覆盖 worker 收敛行为）

## Impact

- `internal/module/event/worker/query.go`：CLOSING + 未支付分支的错误处理与阈值告警。
- `internal/module/event/worker/refundquery.go`：新增 `MaxQueries` 配置项与阈值告警。
- `internal/runtime/module/event`、`internal/bootstrap`：worker 构造签名新增 `alert.Alerter` 参数的装配。
- 支付/退款状态机、DDL、proto 契约不变；对外 API 不变。
