## 1. 查单 worker 可观测性

- [x] 1.1 `QueryWorker` 结构体新增 `alerter alert.Alerter` 字段，`NewQueryWorker` 新增该参数（与 `NewDelivery` 形状一致）
- [x] 1.2 CLOSING + 未支付分支：关单错误记警告日志（含 channel、out_trade_no），替换 `_ = gw.Close(...)`
- [x] 1.3 CLOSING + 未支付分支：`QueryCount+1 == MaxQueries` 时上报一次 `attempt_close_stalled`（含 channel、out_trade_no、query_count）
- [x] 1.4 更新 `QueryConfig.MaxQueries` 注释，说明"关闭阈值 + 滞留告警阈值"双重用途

## 2. 退款查单 worker 可观测性

- [x] 2.1 `RefundQueryConfig` 新增 `MaxQueries int32` 字段，`withDefaults` 给出与 `QueryConfig` 一致的默认值
- [x] 2.2 `RefundQueryWorker` 结构体新增 `alerter alert.Alerter` 字段，`NewRefundQueryWorker` 新增该参数
- [x] 2.3 `rescheduleIfProcessing` 在退款仍 PROCESSING 且 `prevCount+1 == MaxQueries` 时上报一次 `refund_query_stalled`（含 refund_no、order_no、query_count）

## 3. 装配

- [x] 3.1 `internal/runtime/runtime.go:138` 的 `NewQueryWorker` 调用传入 `alerter`
- [x] 3.2 `internal/runtime/runtime.go:143` 的 `NewRefundQueryWorker` 调用传入 `alerter`

## 4. 测试

- [x] 4.1 查单 worker：关单失败时状态保持 CLOSING 且产生重排（table-driven，覆盖关单成功/失败）
- [x] 4.2 查单 worker：查询次数跨过阈值告警一次；已超过阈值不再告警；未达阈值不告警（记录型 Alerter 断言调用次数）
- [x] 4.3 退款查单 worker：跨过阈值告警一次、已超过不再告警、阈值前已收敛不告警
- [x] 4.4 修正既有测试中 `NewQueryWorker` / `NewRefundQueryWorker` 的调用签名（注入 `alert.Nop{}` 或记录型实现）

## 5. 验证

- [x] 5.1 全绿：`gofmt` / `go build` / `go vet` / `make check`（govulncheck + gosec 0 issues）/ `make tool`（golangci-lint 0 issues）/ `go test -race -count=1 ./...`
- [x] 5.2 架构传感器：项目未提供 `scripts/check-architecture.sh`，改以 `.harness/guides/architecture.md` 人工核对（`shared/*` 任何层可 import，worker 未新增 repository/gorm 依赖）
- [x] 5.3 `openspec validate fix-worker-silent-stall --type change --strict --json --no-interactive` 通过
