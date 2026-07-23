# Proposal: restructure-align-pay360

## Why

orderd 当前结构(gen/ 生成物、raw database/sql + database/migrations、env 变量配置、cmd/orderd、仅 gRPC)
与团队服务标准 `pay360-service` 不一致,增加维护成本、割裂技术栈。按 pay360 约定对齐,统一到
gtkit 全家桶(ormx/logger)、pb/ 生成物、config/env yaml、grpc-gateway REST,并纳入 OpenSpec 治理。
功能边界以下单/支付/回调/退款主链(sleep_client 订单域)为准,不发权益;高级项(多 attempt/查单/超时
关单)保留,reconcile/outbox 按实际需要。

## What Changes

- **生成物 gen/ → pb/**:buf.gen 输出 `pb/`,新增 grpc-gateway + openapiv2 插件;proto 加 `google.api.http`
  注解,对外同时提供 gRPC 与 REST(+OpenAPI,便于 PHP 等非 gRPC 对接)。go_package 与全部 import 改 `pb/order/v1`。
- **DB 层 raw sql → gtkit/ormx + gorm**:`internal/platform/data` 用 `ormx.Client`;repository 改 gorm;
  唯一冲突走 `gorm.ErrDuplicatedKey`(TranslateError)。**删除 `database/migrations/`**;DDL 迁到权威草稿
  `docs/sql/schema.sql`(承载 gorm 建不出的 DB 级守卫:生成列唯一索引 active_order/active_guard、金额 CHECK、
  ascii_bin collation),人工评审执行,生产禁 AutoMigrate。
- **入口/配置**:`cmd/orderd` → `cmd/server`(flag `--config`);env 变量 → `config/env/env.yml`(+ .example),
  yaml 加载,敏感项(私钥/JWT/密钥)仍走 env/密钥系统覆盖,不写入 yaml。
- **横切与装配**:补 `internal/pkg/{interceptor,requestid,secretbox}` 等;`runtime/module/<x>/factory.go` 装配范式。
- **文档/门禁**:README/DESIGN 同步;`make gen`(buf generate 到 pb)、`make check` 保持全绿。

## Impact

- **依赖**:新增 `github.com/gtkit/ormx`、`gorm.io/gorm`、grpc-gateway、openapiv2;移除 raw 迁移相关约定。
- **数据库**:删 `database/migrations/`,改 `docs/sql/schema.sql` 草稿(含全部 DB 级守卫),需人工评审执行;
  已有 `orderd` 库不受影响(schema 等价)。
- **对外契约**:pb/ 重新生成;新增 REST 端点 + OpenAPI;gRPC 契约不变。
- **兼容**:功能行为不变(资金主链已测能力保留);仅结构/技术栈/接口形态对齐。
- **蓝本**:`pay360-service` 结构为准,`sleep_client/internal/module/order` 为订单域功能参考,均不物理搬运。

**分阶段**(见 tasks.md),每阶段保持 `make check` 绿,先做安全的重命名再动 DB 层。
