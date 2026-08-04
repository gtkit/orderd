# worker-stall-observability

## ADDED Requirements

### Requirement: 关单失败可从日志定位
查单 worker 在支付尝试处于 CLOSING 且渠道报"有单未支付"时会向渠道发起关单。该关单调用返回错误时，worker MUST 记录一条警告日志，包含渠道与商户单号，使失败可定位到具体尝试。关单错误 MUST NOT 改变尝试状态，尝试仍按退避重排继续观察。

#### Scenario: 关单调用失败
- **WHEN** 尝试处于 CLOSING、渠道查单返回"有单未支付"，且关单调用返回错误
- **THEN** 记录含渠道与商户单号的警告日志，尝试状态保持 CLOSING 并按退避重排

#### Scenario: 关单调用成功
- **WHEN** 尝试处于 CLOSING、渠道查单返回"有单未支付"，且关单调用成功
- **THEN** 不产生关单失败日志，尝试状态保持 CLOSING 并按退避重排

### Requirement: 支付尝试长期未收敛时告警一次
查单 worker 在 CLOSING 且渠道报"有单未支付"的分支上，当本次重排使查询次数达到配置阈值 `MaxQueries` 时，MUST 通过告警端口上报一次 `attempt_close_stalled`，字段含渠道、商户单号与已查询次数。该告警在单个尝试的生命周期内 MUST 只触发一次，后续轮次继续按最大退避低频查询而不重复告警。达阈值 MUST NOT 改变尝试状态。

#### Scenario: 查询次数跨过阈值
- **WHEN** CLOSING 尝试的查询次数因本轮重排恰好达到 `MaxQueries`
- **THEN** 上报一次 `attempt_close_stalled` 告警，尝试状态保持 CLOSING

#### Scenario: 查询次数已超过阈值
- **WHEN** CLOSING 尝试的查询次数在本轮重排后超过 `MaxQueries`
- **THEN** 不再上报告警，尝试继续按最大退避低频查询

#### Scenario: 查询次数未达阈值
- **WHEN** CLOSING 尝试的查询次数在本轮重排后仍小于 `MaxQueries`
- **THEN** 不上报告警，尝试按退避重排

### Requirement: 退款长期未收敛时告警一次
退款查单 worker SHALL 支持配置查询次数阈值 `MaxQueries`（未配置时取默认值）。当一笔退款仍处于 PROCESSING 且本次重排使查询次数达到该阈值时，worker MUST 通过告警端口上报一次 `refund_query_stalled`，字段含退款单号、订单号与已查询次数。该告警对单笔退款 MUST 只触发一次，后续轮次继续按最大退避低频查询。达阈值 MUST NOT 改变退款状态，退款保持 PROCESSING。

#### Scenario: 退款查询次数跨过阈值
- **WHEN** 退款仍为 PROCESSING 且查询次数因本轮重排恰好达到 `MaxQueries`
- **THEN** 上报一次 `refund_query_stalled` 告警，退款状态保持 PROCESSING 并继续重排

#### Scenario: 退款查询次数已超过阈值
- **WHEN** 退款仍为 PROCESSING 且查询次数在本轮重排后超过 `MaxQueries`
- **THEN** 不再上报告警，退款继续按最大退避低频查询

#### Scenario: 退款在阈值前已收敛
- **WHEN** 退款在查询次数达到 `MaxQueries` 之前变为成功或失败终态
- **THEN** 不上报告警，也不再重排

### Requirement: 告警端口由装配层注入
查单 worker 与退款查单 worker SHALL 通过构造参数接收 `alert.Alerter` 告警端口，与投递 worker 保持同一形状。运行时装配 MUST 注入与投递 worker 相同的告警实现，测试可注入空实现或记录型实现。

#### Scenario: 运行时装配注入告警端口
- **WHEN** 运行时构造查单 worker 与退款查单 worker
- **THEN** 两者接收到与投递 worker 相同的告警实现
