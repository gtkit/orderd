# Proposal: add-wechat-pay

## Why

orderd 现仅接支付宝(微信回调返回 501)。sleep_client 迁移到 orderd 的硬前置(见 docs/integration-sleep_client.md §6):
微信+支付宝双渠道都在用,微信不接则微信订单无法迁。本变更补齐微信支付,与支付宝同深度、同抽象。

## What Changes

- 新增 `internal/paygateway/wechatgw`:用 `gtkit/go-pay` 的 `wechat.Provider`(微信官方 APIv3 SDK 封装)
  实现 `paygateway.Gateway`(Prepare/Query/Close/Refund/RefundQuery)+ 回调解析,镜像 `alipaygw`。
  - 多账号:每个 `pay_account_id` 独立 Provider(规避单渠道覆盖,DESIGN §6),按 (channel=wechat, pay_account_id) 与 notify_route_id 路由。
  - 微信回调 ACK 用 `Provider.ACKNotify`(v3 约定,非支付宝的 "success" 文本)。
- **配置**:新增 wechat 段。非敏感项(enabled/appid/mchid/notifyurl/accountid/routeid/tradetype)入 `config/env/env.yml`;
  **敏感凭据(商户证书序列号、APIv3 密钥、商户私钥 PEM、平台证书/公钥)一律走 env 注入**,不入 yaml/库/日志。
- **回调**:`/notify/wechat/{route_id}` 接真实处理(验签+解密 → `out_trade_no` 派生结算 `ConfirmPaySuccess`(channel="wechat") → ACK)。
- **装配**:runtime 依 `wechat.enabled` 惰性构建 wechatgw.Router(需 ctx:下载平台证书),接入复合 `channelRouter` 的 wechat 分支。

## Impact

- **依赖**:复用已在用的 `github.com/gtkit/go-pay`(wechat 子包),无新增第三方。
- **契约**:gRPC/DB 契约不变;仅 HTTP 回调 `/notify/wechat/{route}` 从 501 变为真实处理。资金主链(§7/§9/§10)完全复用,不改。
- **验证边界**:适配层/装配/回调/配置可编译 + 单测(fake provider)覆盖;**真实端到端需微信商户凭据**
  (商户号 + APIv3 密钥 + 证书序列号 + 商户私钥 + 平台证书/公钥),当前无凭据,故 live 联调待凭据到位(与支付宝沙箱同理)。
- **范围**:P0 只接**支付回调**;微信**退款独立通知**(`/refund-notify/wechat`)为可选加速器,
  退款终态本由 RefundQueryWorker 轮询收敛,故本变更暂不接(仍 501),按需再补。
