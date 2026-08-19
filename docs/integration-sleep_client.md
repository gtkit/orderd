# sleep_client 接入 orderd 改造方案

> 目标读者:sleep_client / sleep_admin / orderd 三侧开发。
> 结论先行:**不是把 order 模块搬进 orderd,而是拆职责**——orderd 接管「订单生命周期 + 资金」,
> sleep_client 瘦身为「商品/定价/权益/交付」的 **BFF + orderd 事件消费方**。

---

## 0. 原则

1. **orderd 只执行订单与资金,不发权益、不管商品/会员**(设计铁律)。
2. **sleep_client 的会员/权益/账单/退款审批业务不动**,只把「下单→支付→回调→关单→退款执行」委托给 orderd。
3. **金额权威在 orderd**:sleep_client 定价后把最终金额传入,orderd 强校验不变量(original-discount=payable),不代为定价。
4. **权益经 metadata 透传**:orderd 不解析 metadata,原样带进 `order.paid` 事件回传给 sleep_client 发放。
5. **可灰度、可回滚、先双跑对账**再切读、再下线旧代码。

---

## 1. 职责边界(最终态)

| 能力 | 归属 | 说明 |
|---|---|---|
| 商品目录 / 定价 / 优惠券 | **sleep_client** | 解析 productID→标题/金额/权益;算好折扣后把 payable 传 orderd |
| 会员权益配置(vip_type/days) | **sleep_client** | 编入 orderd 下单请求的 `metadata` |
| 下单(幂等/复用未付单) | **orderd** | `CreateOrder`(idempotency_key + request_hash) |
| 调起支付 / 换渠道 | **orderd** | `CreateOrder` 返回 app_params;`PreparePayment` 换渠道 |
| 支付回调验签入账 | **orderd** | `/notify/{alipay,wechat}/{route}`(支付宝、微信均已接;微信待凭据联调) |
| 支付成功核心事务(CAS 单赢家) | **orderd** | §7,含多支付尝试、异常落库 |
| 超时关单 / 关单竞态 | **orderd** | TimeoutWorker + §8 |
| **发权益 / 交付(发 VIP + 落账单)** | **sleep_client** | 消费 `order.paid` 事件,在自己业务库同事务里发放 |
| 交付态(Delivered/Completed) | **sleep_client** | orderd 只到 PAID,交付后由 sleep 自置 |
| 退款**审批流**(一审/二审) | **sleep_admin** | orderd 不做审批 |
| 退款**执行**(调网关 + 状态机 + 查单) | **orderd** | 审批通过后 sleep_admin 调 `RefundOrder` |
| 订单列表 / 时间线(APP) | **sleep_client** | 用本地订单镜像出(见 §4),不依赖 orderd 新 RPC |
| 状态轮询 / SSE 实时推送 | **sleep_client** | SSE 层保留;数据源改接 orderd 状态 + 事件 |

---

## 2. 契约映射

### 2.1 下单:sleep_client → `orderd.CreateOrder`

sleep_client 在 `CreateOrder` 里解析 productID,组装 orderd 请求:

| orderd 字段 | 来源(sleep_client) |
|---|---|
| `user_id` | 鉴权上下文 |
| `product.product_id` | `Order.ProductID` |
| `product.title` | `Product.Title`(下单快照) |
| `product.business_type` | 固定 `"membership"`(orderd 默认不设业务白名单=放行;收紧见 §6.3) |
| `product.metadata_type` / `metadata_version` | `"membership"` / `1` |
| `product.metadata` | **权益快照编码**(见 §2.2) |
| `amount.original` / `discount` / `payable` | `OriginalPrice` / `DiscountPrice`(含券) / `PayAmount` |
| `amount.currency` | `CNY` |
| `pay_method` | `PayMethod`(微信/支付宝) |
| `channel_id` | `ChannelID` |
| `client_order_no` | **sleep_client 自己的 `OrderNo`**(业务单号,一等字段,对账关联) |
| `idempotency_key` | sleep_client 生成的稳定幂等键(如 `user+product+未付单` 派生) |

响应 `CreateOrderResponse{order_no, order_token, app_params, status, expire_at, reused}`:
- `app_params` 直接回给 APP 调起支付;
- sleep_client 把 orderd 的 `order_no` / `order_token` 存进**本地订单镜像**(见 §4)。

### 2.2 权益 metadata 契约

定义一个**会员权益消息**(建议 proto,sleep_client 下单编码 + 事件消费解码共用一份):

```proto
// sleep_client 侧契约(不进 orderd 仓库;orderd 只当不透明字节透传)
message MembershipBenefit {
  int32 vip_type = 1;   // 1-月 2-季 3-年(= Order.BenefitVIPType)
  int64 vip_days = 2;   // 权益天数(= Order.BenefitVIPDays)
}
```

- 下单:`metadata = proto.Marshal(MembershipBenefit{vip_type, vip_days})`,`metadata_type="membership"`。
- orderd:原样存 `orders.metadata`,原样带进 `order.paid` 事件的 `metadata` 字段。
- 发权益:消费方 `proto.Unmarshal(event.Metadata, &b)` → 得到 vip_type/days → 发放。
- **禁止**往 metadata 塞密钥或多余 PII(orderd 契约约束,≤4096 字节)。

### 2.3 状态映射(orderd ↔ sleep_client)

| orderd status | sleep_client status | 触发 |
|---|---|---|
| PENDING | Pending | 下单 |
| PAID | Paid | 支付成功(orderd 事件) |
| —(orderd 不管) | **Delivered / Completed** | sleep 发权益成功后**自置** |
| CLOSING / CLOSED | Closed | 超时/被取代(orderd 事件) |
| CANCELLED | Cancelled | 用户取消(orderd) |
| REFUNDING / REFUNDED | Refunding / Refunded | 退款执行(orderd 事件) |
| REFUND_EXCEPTION | (告警,人工) | orderd 退款异常 |

要点:**Delivered/Completed 是 sleep 独有的交付态**,orderd 无对应——sleep 在消费 `order.paid` 并成功发权益后,在本地镜像自行推进到 Delivered。

### 2.4 事件契约(orderd → sleep_client webhook)

| 事件 | sleep_client 消费动作 |
|---|---|
| `order.paid` | 发 VIP + 落账单 + 本地镜像置 Paid→Delivered(幂等,见 §3.3) |
| `order.refunded` | 本地镜像置 Refunded,回收/调整权益(按业务) |
| `order.closed` | 本地镜像置 Closed |

事件传输、验签(HMAC + key 轮换)、幂等(inbox)全部复用 `sdk/orderevents` + `sdk/orderevents/inbox_schema.sql`。

---

## 3. sleep_client 侧改造(删 / 留 / 加)

### 3.1 删除(职责移交 orderd)

| 文件/能力 | 原因 |
|---|---|
| `application/payment.go`(`PaymentService`:ParseNotify/HandlePayNotify/复核/重开) | 支付回调改由 orderd 收 |
| `application/delivery.go` 的 `Close` / `CloseSuperseded` / `afterClose`(网关关单) | orderd TimeoutWorker + §8 接管关单 |
| `worker/close.go`(延迟队列关单) | 同上 |
| `OrderService` 内「调 payMgr.UnifiedOrder / 关单竞态」 | orderd 接管 |
| `PaymentGateway`(直连 paymgr)、`DelayQueue` 的关单用途 | 支付/关单不再本地做 |
| 支付回调路由 `/orders/notify/wxpay|alipay` | 迁到 orderd `/notify/...` |

### 3.2 保留(sleep 业务,不动)

- **商品目录 / 定价 / 优惠券**(`ProductRepository` 等)。
- **`delivery.go` 的发放核心**:`FinalizePaidOrder`(发 VIP + 落账单)—— 逻辑保留,**触发源从"本地支付回调"改为"消费 order.paid 事件"**。
- **会员账户 / `order_bill` / 退款审批(sleep_admin)** 全部保留。
- **`Order` 本地镜像表**:保留会员/交付/退款审批/权益到期等 orderd 不管的字段(benefit_*、access_expire_at、delivered_at、refund_status(审批 0-9)、coupon_*)。
- **SSE 层**(`sse_status.go` + Redis Pub/Sub):保留,数据源改接 orderd。

### 3.3 新增

| 组件 | 说明 |
|---|---|
| **orderd 客户端**(gRPC 或 REST) | 下单 `CreateOrder`、换渠道 `PreparePayment`、查状态 `GetOrderStatus`、按单号回查 `GetOrder`(镜像失步恢复 order_token,支持 `client_order_no`=sleep 业务单号);sleep_admin 侧 `RefundOrder`/`GetRefund` |
| **服务间 JWT 签发** | 用 orderd 约定的 HS256 + kid,claims 带 `app_id`/`user_id`/`perms`(order:create/read/cancel、refund:execute/read、event:read)、iss/aud/exp |
| **事件消费 handler** | 用 `orderevents.NewHandler`(OnPaid/OnRefunded/OnClosed);在**同一业务事务**里 `INSERT inbox_events` 去重 + 按 (order_no, aggregate_version) 单调 + 发权益/落账单 + 推进本地镜像,提交后才回 2xx |
| **`inbox_events` 表** | 按 `sdk/orderevents/inbox_schema.sql` 建在 **sleep 业务库** |
| **通知 outbox + 通知 worker** | 支付成功后的公众号/短信/邮件**不在事件消费事务里同步发**:消费 `order.paid` 时把「待发通知」与开会员**同事务**写进 sleep 本地 `notify_outbox`,提交后即回 2xx;由独立 worker 异步发送并按渠道退避重试(见本节末「通知异步化」) |
| **状态回填(可选兜底)** | 定时/按需 `GetOrderStatus` 补拉,纠正漏事件的本地镜像 + 触发 SSE;镜像丢失 order_token 时用 `GetOrder(client_order_no)` 回查恢复 |

发权益 handler 骨架(把原 `DeliveryService.AfterPaid` 挪进来):

```go
h := orderevents.NewHandler(orderevents.Config{
  KeyID: kid, Secret: secret,
  OnPaid: func(ctx context.Context, m orderevents.EventMeta, e *orderv1.OrderPaidEvent) error {
    return tx(ctx, func(dbCtx) error {                 // sleep 业务库事务
      if seen := insertInbox(dbCtx, m.EventID, e.OrderNo, m.AggregateVersion); seen {
        return nil                                      // 重复事件,幂等跳过
      }
      var b membershipv1.MembershipBenefit
      proto.Unmarshal(e.Metadata, &b)                   // 权益从 metadata 还原
      // 关联本地镜像(e.OrderNo=orderd order_no;或用 client_order_no 关联 sleep OrderNo)
      if err := finalizePaidOrder(dbCtx, e, &b); err != nil { // 发 VIP + 落账单 + 置 Delivered
        return err
      }
      // 通知不在此同步发:与开会员【同事务】落「待发通知」,实际发送交给独立 worker(见「通知异步化」)。
      return enqueueNotify(dbCtx, e, &b)                // 每渠道各写一条 pending 到 notify_outbox
    })
  },
})
mux.Handle("/webhooks/order", h)
```

#### 通知异步化(第二级 outbox)

支付成功后的用户触达(微信公众号模板消息、短信、邮件)与「开会员」是两种性质:开会员是**用户付费必须拿到的交付**,要求可靠 + 幂等 + 与交付标记同事务;通知是**可失败、可重试、可延迟的触达**,一次没发出去补发即可。二者必须解耦——否则慢/会失败的外部网关调用会拖住事件 2xx 应答(触发 orderd 重投),甚至连累会员发放回滚。

做法是在 sleep 业务库再加一级 outbox,与 orderd→sleep 的事件投递同构:

1. **入队与开会员同事务**:消费 `order.paid` 时,`enqueueNotify` 给每个渠道各写一条 `pending` 到 `notify_outbox`,和开会员/落账单在**同一事务**提交。保证「开了会员就一定有待发通知,不丢」;事务未提交则整体回滚,orderd 重投后重来(inbox 去重挡住重复开会员)。
2. **独立 worker 异步发送**:通知 worker 扫 `notify_outbox` 里到期的 `pending`/`failed`,按渠道分派到对应发送器,成功置 `sent`,失败退避重试(`attempts++` + 抬 `next_attempt_at`),超过阈值转 `dead` 并告警。发送失败**只重试,不回滚会员、不影响事件应答**。
3. **渠道隔离**:公众号/短信/邮件各自独立记录与重试,一个渠道网关故障不连累其它。
4. **发送幂等**:事件 at-least-once + worker 重试都可能重复触发,须按 `(order_no, channel)` 唯一约束去重;外部网关支持幂等号时带上 `notify_outbox.id`,避免用户收到重复短信。

`notify_outbox` 建在 **sleep 业务库**(与 `inbox_events` 同库,才能与开会员同事务):

```sql
CREATE TABLE notify_outbox (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_no        VARCHAR(64)  NOT NULL COMMENT 'orderd order_no,关联本地镜像',
  channel         VARCHAR(16)  NOT NULL COMMENT 'wechat_mp / sms / email',
  payload         JSON         NOT NULL COMMENT '渲染发送所需最小数据(模板号/收件人/变量),禁写密钥',
  status          TINYINT      NOT NULL DEFAULT 0 COMMENT '0 pending 1 sent 2 failed 3 dead',
  attempts        INT          NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(3)  NOT NULL,
  last_error      VARCHAR(512) NOT NULL DEFAULT '',
  created_at      DATETIME(3)  NOT NULL,
  sent_at         DATETIME(3)  NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_order_channel (order_no, channel),   -- 同单同渠道只发一次(幂等)
  KEY idx_due (status, next_attempt_at)              -- worker 认领到期待发
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付成功后的用户触达发件箱';
```

通知 worker 骨架(与 orderd 的 outbox worker 同构:认领到期 → 发送 → 成功置 sent / 失败退避 / 超阈转 dead):

```go
for _, n := range store.ClaimDueNotify(ctx, now, batch) { // WHERE status IN(pending,failed) AND next_attempt_at<=now
    err := senders[n.Channel].Send(ctx, n)                // 公众号/短信/邮件各自实现,带 n.ID 作幂等号
    switch {
    case err == nil:
        store.MarkSent(ctx, n.ID)                          // status=sent
    case n.Attempts+1 >= maxAttempts:
        store.MarkDead(ctx, n.ID, err)                     // status=dead + 告警(转人工补发)
    default:
        store.Reschedule(ctx, n.ID, backoff(n.Attempts), err) // attempts++,抬 next_attempt_at
    }
}
```

要点:通知 worker 与事件消费**完全隔离**——它的慢和失败永远碰不到 orderd 的事件应答与 sleep 的会员发放,即「慢/会失败的外部调用隔离在最外层,绝不倒灌回资金与交付」原则的落地。

---

## 4. 数据与双库策略

- **orderd 库**(资金权威):orders / payment_attempts / payment_transactions / order_events / event_outbox / refunds …
- **sleep 业务库**(会员权威):`orders` 本地镜像 + `inbox_events` + `order_bill` + 会员账户 + 退款审批。
- **单号约定**:
  - sleep 的 `Order.OrderNo` = 业务单号,作 orderd `client_order_no`(orderd 保证 `UNIQUE(app_id, client_order_no)`);
  - orderd 生成自己的 `order_no` + `order_token`,sleep 镜像存这两者用于回调关联与查状态;
  - 事件里的 `order_no` 是 orderd 的;消费时用它或 `client_order_no` 定位 sleep 镜像。
- **金额一致性**:sleep 定价 → payable 传 orderd → orderd 校验不变量;两侧金额必须一致,对账以 orderd `payment_transactions` 为准。

---

## 5. 关键流程时序

**下单**
```
APP → sleep.CreateOrder(productID)
  sleep: 解析商品/定价/权益 → 建本地镜像(Pending) → 签 JWT
       → orderd.CreateOrder(amount + metadata=权益, client_order_no=sleepOrderNo)
       ← {order_no, order_token, app_params}
  sleep: 镜像存 orderd order_no/token → 回 APP app_params
```

**支付成功 → 发权益**
```
渠道 → orderd /notify/alipay/{route} → 验签 → §7 入账 → 发 order.paid(带 metadata=权益)
     → sleep /webhooks/order: 验签 → inbox 去重 → 发 VIP + 落账单 + 镜像 Paid→Delivered + 落待发通知(同事务) → 2xx
     → sleep 通知 worker: 异步发公众号/短信/邮件,按渠道退避重试(失败只重试,不回滚会员)
```

**退款(审批后执行)**
```
用户/客服 → sleep_admin 退款审批(一审/二审)  ← 全在 sleep_admin
  通过 → sleep_admin.签JWT(refund:execute) → orderd.RefundOrder(order_no, amount, out_refund_no)
  orderd 调网关 + 退款状态机 + 退款查单 → 成功发 order.refunded
     → sleep 消费:镜像 Refunded + 回收/调整权益
```

**超时关单 / 用户取消**
```
orderd TimeoutWorker(过期 PENDING)/ CancelOrder → order.closed → sleep 镜像置 Closed
```

**状态查询 / SSE**
```
APP 轮询/SSE → sleep 读本地镜像(事件驱动更新);必要时 sleep 兜底 GetOrderStatus 校正
```

---

## 6. 缺口与前置(现状)

1. **微信支付**:✅ 代码已就绪(`internal/paygateway/wechatgw`:下单/查单/关单/退款/退款查单 + 回调解析 + ACK;
   `pay.wechat` 配置;`/notify/wechat/{route}` 已接真处理,不再 501;单测覆盖)。**仅差微信商户凭据做真实联调**
   (商户号 + APIv3 密钥 + 证书序列号 + 商户私钥 + 平台证书/公钥)。
2. **退款审批 vs 执行分离**:orderd 只提供「退款执行」;**一审/二审审批流留 sleep_admin**,
   审批通过后才调 `orderd.RefundOrder`。sleep 的 `OrderRefundStatus(0-9)` 与 orderd 的
   `RefundStatus` 不是一一对应——sleep 侧维护审批态,orderd 侧维护执行态,通过 `order.refunded` 事件对齐终态。
3. **订单列表 / 时间线**:推荐用 **sleep 本地镜像**出(会员字段本就在 sleep,改动最小),
   **不**依赖 orderd 新增 RPC;若坚持完全委托,再给 orderd 加「按用户列订单/时间线」RPC(额外工作量)。
4. **`business_type` 白名单**:⚠️ **当前不是前置**。orderd 默认策略(`policy.DefaultAmountPolicy`)**未设业务白名单**,
   `ValidateAmount` 对空白名单**放行全部**(`len(AllowedBusiness)>0` 才校验),故 `membership` 现在即可通过。
   **不要**把 `membership` 加进全局默认策略——那会拒掉其它接入方的 business_type、破坏多租户;
   要收紧应做**按 app 的策略**(见 §6.3 安全加固建议),而非全局硬编码。
5. **app_pay_accounts 播种 + JWT 对接**:orderd 侧需建账号记录并与 sleep 的签名协调(见 §6.1、§6.2)。

### 6.1 app_pay_accounts 播种(orderd 库,真需要)

`pay_account_id` 必须 == `config` 的 `pay.<渠道>.account_id`;`notify_route_id` 必须 == `pay.<渠道>.route_id`。占位值替换为真实商户信息:

```sql
-- 支付宝(沙箱示例:account_id/route_id 与 env.yml 的 pay.alipay 一致)
INSERT INTO app_pay_accounts(pay_account_id, app_id, channel, mch_id, pay_app_id, notify_route_id, config_ref, config_version, enabled)
VALUES ('acc-alipay', 'app1', 'alipay', '', '<支付宝APPID>', 'alipay-sbx', 'env', 1, 1);

-- 微信(启用后:mch_id=商户号,pay_app_id=开放平台appid,与 env.yml 的 pay.wechat 一致)
INSERT INTO app_pay_accounts(pay_account_id, app_id, channel, mch_id, pay_app_id, notify_route_id, config_ref, config_version, enabled)
VALUES ('acc-wechat', 'app1', 'wechat', '<微信商户号>', '<微信appid>', 'wechat', 'env', 1, 1);
```

要点:`app_id` 必须与调用方 JWT 里的 `app_id` 一致;渠道凭据(私钥/证书)**不在此表**,在 `pay.<渠道>` 段(git 忽略的 env.yml 或 *_path)。

### 6.2 服务间 JWT 对接契约

sleep(BFF)签发、orderd 验签,双方必须一致:

| 项 | orderd 侧(验签) | sleep 侧(签发) |
|---|---|---|
| 算法 | HS256 | HS256 |
| 密钥 | `env ORDERD_JWT_KEYS="<kid>=<secret>[,...]"` | 用同一 `<kid>` 选 header `kid`,同一 `<secret>` 签名 |
| `iss` / `aud` | `yaml auth.issuer` / `auth.audience`(如 bff / orderd) | claims `iss`/`aud` 与之逐字一致 |
| `exp` | 必校验(过期即拒) | 必带,建议短时效(如 1h) |
| claims | `app_id`(多租户键,取自 token)、`user_id`(>0) | 按业务填 |
| `perms` | 方法级权限,缺失 → PermissionDenied | 按调用的 RPC 授予:`order:create`(下单/换渠道)、`order:read`(查状态)、`order:cancel`、`refund:execute`(sleep_admin 审批后)、`refund:read`、`event:read`(拉事件) |

签发示例见 `examples/createorder/main.go`。密钥经密钥系统注入两侧,**不入库/git/日志**。

### 6.3 安全加固建议(可选,#4 白名单的正确形态)

当前 business_type/金额策略是全局占位(`policy.DefaultAmountPolicy`,对所有 app 相同)。若要按 app 收紧
(如限制 app1 只能下 `membership`、设更严的金额上下限),**应实现按 app 的策略来源**(配置表/配置中心),
而非在全局默认里硬编码——后者会波及其它接入方。这是 orderd 的独立增强项,按需再做。

---

## 7. 分阶段迁移(灰度、可回滚)

| 阶段 | 内容 | 回滚点 |
|---|---|---|
| **P0 前置** | 微信支付(代码已就绪)配真实商户凭据联调;orderd 侧播种 app_pay_accounts(§6.1)+ 对齐 JWT(§6.2);sleep 接 orderd client + 事件消费 handler + inbox 表(先不接流量) | 不切流量,随时停 |
| **P1 双跑** | 新下单走 orderd(灰度比例),sleep 旧支付路径保留;两边订单/金额**对账**(client_order_no 关联) | 关灰度开关回旧路径 |
| **P2 切读** | 状态/交付以 orderd 事件为权威;SSE/轮询数据源切到镜像(事件驱动) | 保留旧回调路由做兜底 |
| **P3 下线** | 删 sleep 本地支付/关单/回调代码(§3.1);旧回调路由下线 | 灰度回退窗口后执行 |

对账脚本(P1 必备):按 `client_order_no` 比对 sleep 镜像与 orderd 的「金额/状态/交易号」,不一致告警。

---

## 8. 风险清单

- **权益重复发放**:必须靠 inbox(event_id 唯一 + aggregate_version 单调),Redis 不能当权威幂等源。
- **金额不一致**:sleep 定价与 orderd 校验必须同源;券后金额传 payable,orderd 只信传入值(app1 需在 orderd 侧确认 `pricing.trustcaller` 边界)。
- **状态映射漏项**:Delivered/Completed 是 sleep 独有,别指望 orderd 回传;REFUND_EXCEPTION 要有人工告警通道。
- **退款双写**:审批态(sleep)与执行态(orderd)分离,终态靠事件对齐,避免两边各写一半。
- **微信缺口**:P0 未补微信前,微信单不能迁,否则丢单。
- **双库一致性**:orderd 资金权威、sleep 会员权威,以事件 + 对账收敛,不做跨库分布式事务。
- **通知重复/失败**:通知走 sleep 第二级 outbox(§3.3),入队与开会员同事务(不丢),发送按 `(order_no, channel)` 幂等去重、失败退避重试;绝不同步阻塞事件 2xx,也绝不因通知失败回滚会员。

---

## 9. 工作量粗估(不含 orderd 补微信)

| 侧 | 项 | 量级 |
|---|---|---|
| orderd | 微信支付 | 代码已完成;仅剩凭据配置 + 真实联调(小) |
| orderd | 账号播种(§6.1)/ JWT 对接(§6.2) | 小 |
| sleep | orderd client + JWT 签发 | 小 |
| sleep | 事件消费 handler + inbox + 发权益接线 | 中 |
| sleep | order 模块瘦身(删支付/关单,CreateOrder 改调 orderd) | 中 |
| sleep | 状态镜像 + SSE 数据源切换 | 中 |
| 双方 | 对账脚本 + 灰度开关 | 小-中 |

---

## 7. 接入注意事项(2026-08 服务端加固后)

- **单用户未付单上限**:orderd 对同 (app, user) 的 PENDING 订单数设上限(默认 5,`order.maxpendingperuser`),超限 `CreateOrder` 返回 `ResourceExhausted`——sleep_client 收到后应引导用户支付/取消既有订单,而不是重试;携带稳定幂等键的重试命中复用,不受上限影响。
- **金额策略 per-app**:上线前在 orderd 的 `app_amount_policies` 表为 sleep 的 app_id 配置实付上下限与 `allowed_business='membership'` 白名单(未配置时回退全局默认档:1 分~100 万,业务类型不限)。
- **client_order_no 冲突**:同 app 重复业务单号返回 `AlreadyExists / CLIENT_ORDER_NO_CONFLICT`(区别于幂等键冲突的 `IDEMPOTENCY_CONFLICT`)——前者应查旧单,后者是同键改参。
- **权限集**:sleep_client token 用 `order:create/read/cancel` + `event:read`;sleep_admin 退款用 `refund:execute/read`。运维权限(`ops:*`/`event:requeue`)属 orderd 值班,不进接入方 token。
