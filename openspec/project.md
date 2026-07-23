# orderd — 项目上下文(OpenSpec)

订单 + 支付微服务。功能边界:下单/支付/回调/退款主链(对应 sleep_client 订单域),不发权益(接入方消费事件)。

约定(对齐 gtkit 服务标准 pay360-service):
- 契约 `proto/order/v1`,生成物 `pb/`(buf:go + go-grpc + grpc-gateway + openapiv2)。
- DB:`gtkit/ormx` + gorm;**无 migration 工具**,DDL 出草稿 `docs/sql/*.sql` 人工评审执行,**生产禁 AutoMigrate**。
- 入口 `cmd/server`,配置 `config/env/env.yml`。
- 分层:`internal/{models,store,repository,module/<x>/{application,transport},runtime,platform,pkg}`。
- 日志 `gtkit/logger`(禁 slog);金额单位分。
- 变更走 OpenSpec:proposal → design → tasks → 实现。
