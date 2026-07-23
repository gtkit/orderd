# Tasks: restructure-align-pay360

> 分阶段;每阶段结束 `make check` 全绿。先安全重命名,再动 DB 层(最大)。

## 1. 生成物与入口对齐(安全重命名)

- [x] 1.1 `gen/` → `pb/`:buf.gen `out: ../pb`;go_package 改 `github.com/gtkit/orderd/pb/order/v1`;全仓 import 替换;`cd proto && buf generate`;`go build ./...`
- [x] 1.2 `cmd/orderd` → `cmd/server`;更新 Makefile `run`、Dockerfile、README、DESIGN(flag `--config` 留到 Stage 3 配合 yaml)
- [x] 1.3 Makefile 加 `gen` 别名(cd proto && buf generate);`clean` 清 pb;`make check` 全绿

## 2. 对外接口:gRPC + grpc-gateway REST + OpenAPI

- [x] 2.1 proto 加 `google.api.http` 注解(7 个 RPC);REST 路径 `/v1/orders*`、`/v1/refunds`、`/v1/events`;身份走 authorization 头不入 path/query
- [x] 2.2 buf.gen 加 grpc-ecosystem/gateway + openapiv2(remote 插件,沿用既有 remote 风格);buf.yaml 补 googleapis dep;buf dep update;生成 order.pb.gw.go + order.swagger.json
- [x] 2.3 bootstrap 起 REST 反向代理(newRESTGateway,回环拨号复用拦截器);挂 /v1/ 与回调 /notify 分离;authorization 自定义 header matcher 透传
- [x] 2.4 网关联调测试(gateway_test.go:REST→gRPC 路由 + query 映射 + Authorization 透传);make check 全绿

## 3. 配置:env → config/env/env.yml

- [x] 3.1 `config/env/env.yml` + `env.example.yml`;`platform/config` 改 yaml.v3 加载 + `KnownFields(true)` 严格解析;flat Config 字段集不变(消费方零改动);config_test.go 7 例过
- [x] 3.2 敏感项(JWT/支付宝私钥公钥/webhook 密钥/MySQL 口令)走 env 覆盖不入 yaml;缺 JWT/支付宝密钥 fail-closed 校验保留并测试
- [x] 3.3 `cmd/server` flag `--config`(默认 config/env/env.yml);docker-compose 改挂载 env.docker.yml + 仅敏感项 env;Dockerfile 注释 gen→pb;二进制启动冒烟验证 yaml→env→bootstrap 全链

## 4. DB 层:ormx + gorm,聚合仓储 + TxManager,删 migrations,出 docs/sql/schema.sql(最大改动)

> 最终决策(读端口后修正,两次经用户确认"选最好的"):**采用 B1 —— 保留 store.Store 工作单元(UoW)端口,
> 只换 gorm 实现**。现有 `WithinTx + Repos` 已是 guide 认可的"复杂聚合 + 单一事务边界"UoW,结构性强制
> 事务边界、不泄漏 *gorm.DB,已合规;重构成平铺 repo(B2)是在已测资金核心上做高风险改动换边际收益。
> **护栏兑现:application/memstore/全部单测零改动,资金主链逐字保留;新 gorm 实现经真实 MySQL 集成回归。**

- [x] 4.1 `platform/data/mysql.go`:`ormx.Open` + WithTranslateError + 池参数(对齐 pay360) + `TxManager.Do`;`internal/pkg/dbtx` context-tx helper;config 暴露 mysql 分字段
- [x] 4.2 models 加 `TableName()`(修 event_outbox 复数化)+ AppPayAccount/AppEndpoint `primaryKey`;`gormschema_test.go` 用 schema.Parse 对 DDL 真实列集逐字段校验(护栏)
- [x] 4.3 决策改为 B1:保留 `store.Store` UoW 端口(已合规),不重构端口;application 零改动
- [x] 4.4 `repository/gormstore`:gorm 实现 store.Store(WithinTx→ormx.WithTx+dbtx;FOR UPDATE/SKIP LOCKED 用 gorm Clauses;comma-ok 不泄漏 ErrRecordNotFound;唯一冲突→gorm.ErrDuplicatedKey);编译期断言 `_ store.Store`
- [x] 4.5 gorm AccountResolver/EndpointResolver;runtime 装配 data.NewMySQL→gormstore.New→注入(Build 加 ctx);删旧 raw `repository/mysql`
- [x] 4.6 memstore 保持不变(B1 无需改端口,资金主链单测继续护航);`make check` 全绿
- [x] 4.7 DDL 迁 `docs/sql/schema.sql` 权威草稿(11 表 + 91 处 DB 级守卫:active_order/active_guard 生成列唯一、CHECK、ascii_bin);删 `database/migrations/`;setup-db.sh/集成测试改从 schema.sql 建库
- [x] 4.8 `make check`(race)全绿;真实 MySQL 8.0 集成 3 例过(资金链事务/FOR UPDATE/生成列唯一/SKIP LOCKED 认领/dup 翻译)

## 5. 横切与装配范式

- [x] 5.1 `internal/pkg/requestid`(纯 context 传递,crypto/rand.Text)+ `internal/pkg/interceptor`(RequestIDUnary/AccessLogUnary/Observability 链);bootstrap 挂 requestid→accesslog→auth;单测 6 例过。鉴权保留在 module/order/transport(绑定该服务方法权限表)
- [x] 5.2 有意偏离:保留 `runtime.Build` 单体装配。orderd 各模块共享同一 UoW store 且构造紧耦合(payment→order),pay360 per-module factory 对其为纯 indirection;harness 允许极简装配豁免 runtime 层。Stage 6 文档记录理由
- [x] 5.3 有意偏离:不引 secretbox(orderd 密钥全走 env、不入库,无加密对象);breaker 属"高级项按需",非重构必需,暂不加。日志不打密钥(既有)

## 6. 收尾

- [x] 6.1 README 同步(pb/、REST+OpenAPI 接入段、config yaml+env 两表、schema.sql、两套 schema 谁执行、门禁 make gen/setup-db);DESIGN 配套引用改 schema.sql;`internal/pkg/interceptor`/`requestid` 有意偏离已在 tasks 记录
- [x] 6.2 功能边界复核:主链(下单/支付/回调/退款)保留;多 attempt/查单/超时关单保留;reconcile 默认关(需真实账单源)、outbox 为核心事件投递保留——均已恰当 gated,无需增删;不发权益(SDK 侧 inbox 由接入方执行)
- [x] 6.3 `make check`(race)全绿 + 真实 MySQL 集成过;补 `specs/rest-gateway` delta,`openspec validate --strict` 通过;归档待用户确认(未自动 archive/commit)
