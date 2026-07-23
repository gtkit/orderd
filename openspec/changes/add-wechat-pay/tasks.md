# Tasks: add-wechat-pay

> 与支付宝同深度、同抽象;资金主链复用不改。每步 `make check` 绿。

## 1. 网关适配

- [x] 1.1 `internal/paygateway/wechatgw/wechatgw.go`:Config(微信凭据)+ Gateway 包装 `*wechat.Provider`(经小接口保测试性);`NewGateway(ctx, cfg)`(需 ctx 下载平台证书);Prepare/Query/Close/Refund/RefundQuery 映射 + ParseNotify + Ack
- [x] 1.2 `internal/paygateway/wechatgw/router.go`:按 pay_account_id / notify_route_id 路由(镜像 alipaygw.Router)
- [x] 1.3 `wechatgw_test.go`:fake provider(嵌 paymgr.UnimplementedProvider + ACKNotify)覆盖 Prepare/Query/Refund/状态映射/ParseNotify

## 2. 配置

- [x] 2.1 `config` 加 wechat 段:yaml 非敏感(enabled/appid/mchid/notifyurl/accountid/routeid/tradetype);凭据(mch_cert_serial_number/mch_apiv3_key/mch_private_key_{pem,path}/wechatpay_certificate_{pem,path}|公钥模式)全在 pay.wechat 段(git 忽略的 env.yml 或 *_path 挂载)
- [x] 2.2 校验:wechat.enabled=true 需 appid/mchid/notifyurl + 路由必填齐(凭据缺失由 provider 启动时报错);config_test 覆盖
- [x] 2.3 env.yml / env.example.yml 加 wechat 段(仅非敏感)

## 3. 装配与回调

- [x] 3.1 runtime:依 wechat.enabled 惰性构建 wechatgw.Router(用 Build 的 ctx);接入 channelRouter 的 wechat 分支;传入回调 Handler
- [x] 3.2 callback:`/notify/wechat/{route}` 接真实处理(ParseNotify → ConfirmPaySuccess(channel="wechat") → Ack);验签失败非 2xx 触发重投
- [x] 3.3 回调层守卫测试:微信未启用→501、未知 route_id→400(不触达 payment.Service)。paid→ConfirmPaySuccess→ACK 的分支由 wechatgw 单测(ParseNotify/Ack)+ payment app 测试(ConfirmPaySuccess)分别覆盖,回调 paid 分支未做端到端单测(需真实 provider + payment.Service,同支付宝回调不做单测)

## 4. 收尾

- [x] 4.1 `make check`(race)全绿
- [x] 4.2 README 现状/支付宝段旁补微信接入说明 + 凭据清单;`openspec validate --strict` 过
- [x] 4.3 说明:真实端到端待微信商户凭据(无凭据不虚假声称已联通)
