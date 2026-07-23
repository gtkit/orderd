# orderd — 订单 + 支付微服务

下单 → 支付 → 回调 → 全额退款(V1)。支付结果通过**事件推送(主)+ 查询(兜底)**双路径触达接入方。
orderd 只做订单与资金主链,**不做交付/发权益**(接入方消费事件自行发放)。设计权威见 [`DESIGN.md`](./DESIGN.md)。

- 对外契约:`proto/order/v1/*.proto` —— 同源生成 **gRPC + REST(grpc-gateway)+ OpenAPI**(`pb/order.swagger.json`,供 PHP 等非 gRPC 接入方)
- 事件契约:`proto/order/v1/events.proto` + 接入方消费 SDK `sdk/orderevents`
- 数据模型:`docs/sql/schema.sql`(orderd **独立库**权威建表草稿,9 张核心表 + 2 张对账表;本项目无迁移工具,人工评审执行,生产禁 AutoMigrate)
- 两套 schema、两个库、两个执行方,勿混:`docs/sql/schema.sql` 由 orderd/DBA 建在 **orderd 自己的库**;`sdk/orderevents/inbox_schema.sql` 由**接入方**建在**接入方自己的业务库**(发权益幂等真相源)

---

## 目录

1. [架构与分层](#架构与分层)
2. [快速开始](#快速开始)
3. [配置](#配置)
4. [鉴权(所有 gRPC 调用必读)](#鉴权)
5. [gRPC 接入](#grpc-接入)
6. [REST 接入(grpc-gateway)](#rest-接入grpc-gateway供-php-等非-grpc-方)
7. [HTTP 接入](#http-接入)
   - [支付回调(渠道 → orderd)](#1-支付回调渠道--orderd)
   - [事件 webhook(orderd → 接入方)](#2-事件-webhook-orderd--接入方)
   - [健康检查](#3-健康检查)
8. [支付宝接入与沙箱联调](#支付宝接入与沙箱联调)
9. [后台 worker](#后台-worker)
10. [开发与门禁](#开发与门禁)
11. [现状:已接 / 未接](#现状已接--未接)

---

## 架构与分层

一个进程、一个库,内部按模块划分(边界铁律见 DESIGN §2):

```
cmd/server/                入口(flag --config)
pb/order/v1/               proto 生成物(buf:go + grpc + grpc-gateway + openapiv2)
config/env/                运行配置 yaml(env.yml / env.example.yml)
docs/sql/schema.sql        权威建表草稿(无迁移工具,人工评审执行)
internal/
  appmeta/                 常量(路径/事件头)
  models/                  持久化模型 + gorm TableName + 枚举 + 状态机 + 幂等哈希 + 金额权威 + 事件信封
  store/                   仓储端口(接口)+ memstore(测试内存实现)
  repository/gormstore/    仓储 gtkit/ormx + gorm 实现(store.Store 工作单元)
  module/order|payment|refund|event/application/   应用服务(资金主链)
  module/order/transport/grpc/                     gRPC 适配 + 鉴权拦截器
  module/payment/transport/http/                   支付回调(HTTP)
  module/event/worker/                             投递/查单/超时/退款查单/对账 worker
  paygateway/ + paygateway/{alipaygw,wechatgw}/    网关端口 + 支付宝/微信适配(gtkit/go-pay)
  pkg/{auth,sqlutil,webhook,dbtx,requestid,interceptor,idgen,ordernum}  纯工具:JWT/SQL助手/出口投递/context-tx/请求标识/拦截器/号码
  shared/{account,endpoint,bill,secret,alert}      跨模块轻量 port:账号/端点/账单源/密钥/告警
  platform/{config,data}   配置加载 + ormx 连接
  runtime/ bootstrap/      装配 + 生命周期(gRPC + REST 网关 + worker)
sdk/orderevents/           接入方事件消费 SDK(含 inbox_schema.sql)
```

---

## 快速开始

依赖:Go 1.26+、MySQL 8.0.16+、[buf](https://buf.build)(仅改 proto 时)。

```bash
# 1) 生成 proto 代码(仓库已生成 pb/;改了 proto 才需重跑)
make gen        # = cd proto && buf generate(生成 go + grpc + grpc-gateway + openapiv2)

# 2) 建库(独立 orderd 库,应用 docs/sql/schema.sql;连接参数走 env,默认 127.0.0.1:3306 root 空密码)
make setup-db
#   或 docker compose -f deploy/docker-compose.yml up -d  (一键起 MySQL+Redis+orderd)

# 3) 起服务:结构化配置读 config/env/env.yml,敏感项走环境变量(JWT_KEYS 必填,否则拒绝启动)
export ORDERD_JWT_KEYS='dev1=dev-secret-change-me-32bytes-xx'
# MySQL 口令写在 env.yml 的 database.password;如需覆盖(生产密钥注入)再 export ORDERD_MYSQL_PASSWORD
go run ./cmd/server --config config/env/env.yml
# gRPC :9090   HTTP :8080(/healthz + 支付回调 + REST 网关 /v1/*)

# 4) 下单联调(带 JWT 的 gRPC 客户端示例)
go run ./examples/createorder
```

---

## 配置

结构化、非敏感项走 **YAML**(`config/env/env.yml`,一机一份,不入库);运行环境由 yaml 的 `env` 字段决定(不依赖文件名/命令行)。
`cmd/server --config <path>` 指定文件,严格解析(未知键 fail-fast)。模板见 `config/env/env.example.yml`。

**进程级敏感项走环境变量注入**:

| 环境变量 | 必填 | 说明 |
|---|---|---|
| `ORDERD_JWT_KEYS` | **是** | `kid=secret[,kid2=secret2]`,支持轮换;**缺失则拒绝启动**(fail-closed) |
| `ORDERD_MYSQL_PASSWORD` | 否 | MySQL 口令覆盖:非空则盖过 yaml `database.password`(生产密钥注入用) |
| `ORDERD_WEBHOOK_SECRETS` | 否 | **仅 dev**:`ref=secret,...`(生产用 KMS) |

**支付渠道凭据在 `pay:` 段**(与 go-pay 的 alipay/wechat Config 对齐:私钥/证书支持 `*_pem` 内容或 `*_path` 挂载文件;证书模式 vs 公钥模式)。这些是敏感项:**真实 `env.yml` 必须 git 忽略 + 按环境挂载,提交仓库的只能是 `env.example.yml`(凭据留空);生产优先用 `*_path` 指向挂载的密钥文件**。深度凭据校验由 go-pay provider 在启动时 fail-fast,orderd 只校验路由必填项(`app_id`/`notify_url`/`account_id`,微信另加 `mch_id`)。

YAML 结构(节选,全量见 `config/env/env.example.yml`):

```yaml
env: dev                       # dev/test/prod
log: { level: debug, json: false }
grpc: { addr: ":9090" }
http: { addr: ":8080", notifybaseurl: "https://<域名>" }
database:                      # dbtype/host/port/dbname/user/password + 连接池;口令可被 env ORDERD_MYSQL_PASSWORD 覆盖
  { dbtype: "mysql", host: "127.0.0.1", port: "3306", dbname: "orderd", user: "root", password: "",
    maxopenconn: 50, maxidleconn: 25, maxlifeseconds: 1800 }
redis: { addr: "127.0.0.1:6379", password: "", dbs: [0,1,2], prefix: "" }   # 当前未消费,仅解析保留
order: { ttl: "30m", attemptttl: "15m" }
refund: { enabled: true }      # 开启需同时发放 refund:execute 权限
reconcile: { enabled: false }  # 需真实渠道账单源
pricing: { trustcaller: false }# 默认拒绝采信客户端金额(fail-closed);BFF 经严格服务身份才置 true
auth: { issuer: "bff", audience: "orderd" }   # JWT 密钥走 env ORDERD_JWT_KEYS
snowflake: { machineid: 0 }    # 多副本 StatefulSet ordinal 静态分配
webhook: { allowhosts: [], allowcidrs: [] }   # 出站事件 SSRF 白名单
pay:
  alipay:
    enabled: true
    app_id: "<APPID>"
    is_production: false         # false=沙箱
    notify_url: "https://<域名>/notify/alipay/<route_id>"
    account_id: "acc-alipay"     # 对应 app_pay_accounts.pay_account_id
    route_id: "alipay-sbx"       # 对应 app_pay_accounts.notify_route_id
    trade_type: "native"         # native(扫码)/page/wap/app/jsapi
    private_key: ""              # 或 private_key_path(敏感,勿提交)
    alipay_public_key: ""        # 普通公钥模式;或用证书模式 *_cert_* 三件套
  wechat:
    enabled: false
    app_id: ""
    mch_id: ""
    notify_url: "https://<域名>/notify/wechat/<route_id>"
    account_id: "acc-wechat"
    route_id: "wechat"
    trade_type: "native"
    mch_cert_serial_number: ""
    mch_apiv3_key: ""            # 敏感,勿提交
    mch_private_key_pem: ""      # 或 mch_private_key_path
    wechatpay_public_key_id: ""  # 公钥模式;或平台证书模式 wechatpay_certificate_*
    wechatpay_public_key_pem: ""
```

> **安全**:`ORDERD_JWT_KEYS` 必填、`pricing.trustcaller` 默认关,均为 fail-closed。私钥/密钥/证书**绝不入库、不入 git、不打日志**;`env.yml` 走 git 忽略 + 挂载注入,go-pay Config 打日志已脱敏(禁 `json.Marshal`)。

---

## 鉴权

所有 gRPC 调用必须在 metadata 携带 **`authorization: Bearer <JWT>`**。服务端拦截器完成:验签(HS256,kid 轮换)+ issuer/audience/expiry 校验 + **方法级权限**;通过后把身份注入上下文,**`app_id`/`user_id` 全部取自 token,不信任请求参数**。

JWT 载荷(claims):

| claim | 说明 |
|---|---|
| `app_id` | 接入方标识(多租户隔离键) |
| `user_id` | 用户 ID(>0) |
| `perms` | 权限数组,见下表 |
| `iss` / `aud` / `exp` | 与 `ORDERD_JWT_*` 一致;exp 必填 |
| JWT header `kid` | 选密钥(轮换) |

方法级权限(缺失 → `PermissionDenied`;无/错 token → `Unauthenticated`):

| 方法 | 所需权限 |
|---|---|
| `CreateOrder` / `PreparePayment` | `order:create` |
| `GetOrderStatus` | `order:read` |
| `CancelOrder` | `order:cancel` |
| `RefundOrder` | `refund:execute`(V1 不发放) |
| `GetRefund` | `refund:read` |
| `ListOrderEvents` | `event:read` |

签发示例见 [`examples/createorder/main.go`](./examples/createorder/main.go)。

---

## gRPC 接入

服务 `order.v1.OrderService`(`:9090`)。金额单位**分**;金额不变量 `original - discount = payable`(服务端强制)。

### CreateOrder — 下单(幂等)
请求 `CreateOrderRequest`:`product{product_id,title,business_type,metadata_type,metadata_version,metadata}`、`amount{original,discount,payable,currency}`、`pay_method`(WECHAT/ALIPAY)、`channel_id`、`client_ip`、`idempotency_key`、`client_order_no`。
响应:`order_no`、`order_token`(安全凭证,勿泄露)、`app_params`(前端调起参数)、`status`、`expire_at`、`reused`。
幂等:同 `idempotency_key` 同请求 → 复用(`reused=true`);同 key 异请求 → `AlreadyExists("IDEMPOTENCY_CONFLICT")`。

### PreparePayment — 换渠道 / 重新调起支付
请求:`order_no`、`pay_method`、`channel_id`、`client_ip`。对已 PENDING 订单(重新)调起支付;换渠道=开新尝试(旧的关单),同时只允许一个活跃尝试(§8/§9)。

### GetOrderStatus — 查询(前端反馈 + 兜底)
请求:`order_token`。响应:`status`、`status_text`、`close_reason`、`amount`、`trade_no`、`paid_at`、`refunded_amount`、`aggregate_version`(与事件对齐,判新旧)、`metadata`。

### CancelOrder — 用户取消待支付订单
从未提交网关 → `CANCELLED`;有已提交历史 → `CLOSING`(reconcile/查单确认后 `CLOSED`)。

### RefundOrder / GetRefund — 退款(V1 授权关闭)
`RefundOrder`:`order_no`、`refund_amount`(V1=全额)、`reason`、`out_refund_no`(幂等键)。需 `refund:execute` + `ORDERD_REFUND_ENABLED=true`。
`GetRefund`:`refund_no` 或 `out_refund_no` 二选一(均按 `app_id` 隔离)。

### ListOrderEvents — 漏投补偿
请求:`cursor`(空=从头)、`limit`(≤1000)。响应:`events[]`(含序列化 `EventEnvelope`)、`next_cursor`。接入方大规模漏事件时游标补拉。

### 错误码映射
| gRPC code | 场景 |
|---|---|
| `AlreadyExists` | 幂等键冲突(`IDEMPOTENCY_CONFLICT`) |
| `InvalidArgument` | 金额/字段校验失败(protovalidate) |
| `Unauthenticated` | 无/错/过期 token |
| `PermissionDenied` | 缺权限 / 订单不属于该用户 |
| `FailedPrecondition` | 订单状态不允许(非 PENDING / 退款未授权 / 无可用账号) |
| `Aborted` | 已存在活跃支付尝试 / 活跃退款 |
| `NotFound` | 订单/退款不存在(含跨租户越权,对外等同不存在) |

---

## REST 接入(grpc-gateway,供 PHP 等非 gRPC 方)

与 gRPC **同源同语义**:REST 经 grpc-gateway 回环转发到 gRPC,复用同一套鉴权(JWT 走 `Authorization: Bearer`)、protovalidate 校验与错误映射。挂 `:8080` 的 `/v1/*`(与回调 `/notify` 同端口不同前缀)。OpenAPI 文档:`pb/order.swagger.json`。

| RPC | REST |
|---|---|
| CreateOrder | `POST /v1/orders`(body) |
| GetOrderStatus | `GET /v1/orders/status?order_token=&user_id=` |
| CancelOrder | `POST /v1/orders/{order_no}/cancel`(body) |
| PreparePayment | `POST /v1/orders/{order_no}/prepare-payment`(body) |
| RefundOrder | `POST /v1/refunds`(body) |
| GetRefund | `GET /v1/refunds?refund_no=` 或 `?out_refund_no=` |
| ListOrderEvents | `GET /v1/events?cursor=&limit=` |

gRPC status → HTTP:`InvalidArgument`→400、`Unauthenticated`→401、`PermissionDenied`→403、`NotFound`→404、`AlreadyExists`/`Aborted`→409、`FailedPrecondition`→400(grpc-gateway 默认映射)。

---

## HTTP 接入

HTTP 服务(`:8080`)承载两类:**渠道打给 orderd 的支付回调** 和 **健康检查**;另有 orderd **主动推给接入方**的事件 webhook(接入方侧 HTTP,由 SDK 消费)。

### 1) 支付回调(渠道 → orderd)

| 路径 | 说明 |
|---|---|
| `POST /notify/alipay/{notify_route_id}` | 支付宝异步通知(已接):按 route_id 定位账号 → 验签 → 由 `out_trade_no` 派生订单入账(§7)→ ACK `success` |
| `POST /notify/wechat/{notify_route_id}` | 微信 V3(**未接**,返回 501) |
| `POST /refund-notify/wechat/{notify_route_id}` | 微信退款通知(**未接**,501) |

支付宝回调 ACK 约定:成功回 `success` 停止重投;验签失败/入账失败返回非 2xx → 支付宝重投(orderd 幂等,重复回调安全)。回调**只解析验签**,资金入账走 `ConfirmPaySuccess`(严格绑定 attempt/账号/渠道 + 交易号唯一 + 异常落库告警)。

### 2) 事件 webhook(orderd → 接入方)

orderd 通过 Outbox worker 把订单事件 HTTP 推给接入方配置的 `webhook_url`(`app_endpoints` 表)。**接入方用 `sdk/orderevents` 消费**:

- 传输:`application/x-protobuf`,body = 序列化的 `EventEnvelope`(见 `events.proto`)。
- 头:`X-Order-Event-Id`(幂等键)、`X-Order-Event-Type`、`X-Order-Key-Id`、`X-Order-Timestamp`、`X-Order-Signature = sha256=HMAC-SHA256(timestamp + "." + raw_body, secret)`。
- 事件类型:`order.paid` / `order.refunded` / `order.closed`。
- **幂等 + 防乱序**:`event_id` 去重 + `aggregate_version` 单调(拒绝比已处理更旧的事件)。

接入方消费(权益发放的**幂等真相源必须是接入方 MySQL 的 `inbox_events`**,见 `sdk/orderevents/inbox_schema.sql`):

```go
h := orderevents.NewHandler(orderevents.Config{
    KeyID: keyID, Secret: []byte(secret),        // 与 orderd app_endpoints 的 key/secret 对应
    OnPaid: func(ctx, m orderevents.EventMeta, e *orderv1.OrderPaidEvent) error {
        return tx(ctx, func(db) error {           // 接入方自己的 DB 事务
            if seen := insertInbox(db, m.EventID, e.OrderNo, m.AggregateVersion); seen { return nil } // 唯一键去重
            // 按 (order_no, aggregate_version) 单调校验,拒绝旧事件
            return grantBenefit(db, e.UserId, e.Metadata) // 同事务发权益,提交后才回 2xx
        })
    },
})
mux.Handle("/webhooks/order", h)
```

SDK 行为(资金安全):`OnPaid` 必填(缺失 panic);资金事件(paid/refunded)无处理器或 payload 无法识别 → 返回 5xx **fail-closed**(不静默吞);验签强制携带 `key_id`;`order.closed` 可选消费。

### 3) 健康检查

`GET /healthz` → `200 ok`。

---

## 支付宝接入与沙箱联调

**普通公钥模式**。凭证从支付宝开放平台(沙箱)获取:APPID、应用私钥、支付宝公钥、沙箱买家账号。

1. **播种账号**(schema 由 `make setup-db` 建好后):
   ```sql
   INSERT INTO app_pay_accounts(pay_account_id,app_id,channel,mch_id,pay_app_id,notify_route_id,config_ref,config_version,enabled)
   VALUES('acc-alipay','app1','alipay','','<APPID>','alipay-sbx','env',1,1);
   ```
   `app_id` 必须与调用 JWT 的 `app_id` 一致;`notify_route_id` 与 `pay.alipay.route_id` 一致。
2. **ngrok** 暴露本机 HTTP:`ngrok http 8080`,`pay.alipay.notify_url=https://<域名>/notify/alipay/alipay-sbx`。
3. **起服务**(`config/env/env.yml` 的 `pay.alipay.*` 填 app_id/notify_url/route_id;私钥/公钥填 `private_key`/`alipay_public_key`,或用 `private_key_path` 挂载,勿提交)。
4. **下单**:`go run ./examples/createorder`(`PAY_METHOD=alipay`)→ `app_params` 是二维码内容(native)→ 用沙箱买家扫码付款。
5. 支付宝异步回调 → ngrok → `/notify/alipay/alipay-sbx` → 订单转 `PAID` 并发 `order.paid` 事件;`GetOrderStatus` 或 `examples/createorder` 复查。

> 网关地址:`IS_PROD=false` 自动用沙箱 `https://openapi-sandbox.dl.alipaydev.com/gateway.do`。多账号:每个 `pay_account_id` 独立 Provider(规避单渠道覆盖,DESIGN §6)。

---

## 后台 worker

`bootstrap` 启动(多副本安全:租约 / 行锁 + 状态复核):

| worker | 职责 |
|---|---|
| Outbox 投递 | 认领待投递事件 → 签名 HTTP 推送 → 退避重试 → 死信告警(§12) |
| 支付查单 | 收敛 UNKNOWN/CLOSING 支付尝试(§9),查单=已付走 §7 |
| 超时关单 | 过期 PENDING 订单 → CLOSING/CLOSED;卡死 REQUESTING → UNKNOWN |
| 退款查单 | 收敛 PROCESSING 退款(`ORDERD_REFUND_ENABLED` 时) |
| 对账 | 每日渠道账单比对 → 差异落 `reconcile_diffs` + 告警(`ORDERD_RECONCILE_ENABLED` 时) |

---

## 开发与门禁

```bash
make check          # gofmt + buf lint + go vet + golangci-lint + go test -race(提交门禁)
make test-race      # 竞态测试
make setup-db       # 建库(应用 docs/sql/schema.sql;无迁移工具)
make gen            # 由 proto 生成 pb/(go + grpc + grpc-gateway + openapiv2)
make build          # 编译
# 集成测试(需真实 MySQL):
ORDERD_TEST_MYSQL_DSN='root@tcp(127.0.0.1:3306)/orderd_it?parseTime=true' make test-integration
```

核心资金链、状态机、幂等、并发矩阵均有单测(`internal/apptest`、`internal/module/**`),门禁要求 `go test -race` 全绿。

---

## 现状:已接 / 未接

**已接并验证**:下单/幂等/金额权威、支付准备与多尝试、支付成功核心事务(§7 CAS/唯一成功/第二笔真实付款落库告警)、取消(§8)、超时关单、退款闭环 + 退款查单、事件双写 + Outbox 投递(SSRF/HMAC)、支付查单、对账逻辑、JWT 鉴权 + 方法级权限、跨租户隔离、**支付宝(gtkit/go-pay,沙箱)**、**持久化 gtkit/ormx + gorm(store.Store 工作单元,真实 MySQL 集成回归)**、**gRPC + REST(grpc-gateway)+ OpenAPI 同源**。

**已接代码、待凭据联调**:
- **微信支付**(下单/回调/退款,APIv3)—— 网关适配 `wechatgw` + 配置 + `/notify/wechat/{route}` 回调 + 装配已完成并单测覆盖;
  真实端到端需微信商户凭据(商户号 + APIv3 密钥 + 证书序列号 + 商户私钥 + 平台证书或公钥),填入 `pay.wechat.*`(凭据在 git 忽略的 env.yml 或 *_path)后 `enabled: true` 即生效。
  微信**退款独立通知**(`/refund-notify/wechat`)为可选加速器,退款终态由 RefundQueryWorker 收敛,暂未接(501)。

**未接(需外部条件)**:
- **真实渠道账单源**(对账 worker 的 `bill.Source`)—— 现为占位;
- **KMS/Vault**(`secret.Resolver`)—— dev 走 env,生产需接密钥系统;
- DESIGN §17 完整故障注入集成测试矩阵。
