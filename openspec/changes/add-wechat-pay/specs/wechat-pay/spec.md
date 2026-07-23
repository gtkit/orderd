# wechat-pay

## ADDED Requirements

### Requirement: 微信支付网关(多账号)
系统 SHALL 提供微信支付网关,经 `gtkit/go-pay` 的微信 APIv3 Provider 实现统一下单/查单/关单/退款/退款查单,
与支付宝同一 `paygateway.Gateway` 抽象。多账号场景下每个 `pay_account_id` MUST 使用各自独立的 Provider 与凭据,
按 (channel=wechat, pay_account_id) 路由,互不覆盖。金额单位为分,不做转换。

#### Scenario: 统一下单返回调起参数
- **WHEN** 应用层对某微信账号发起 Prepare(下单)
- **THEN** 调该账号 Provider 的统一下单,返回 prepay_id 与前端调起参数(app_params/code_url 等)

#### Scenario: 账号无法路由
- **WHEN** 请求的 (wechat, pay_account_id) 无对应已配置网关
- **THEN** 返回 ErrAccountNotRouted,不误用其它账号凭据

### Requirement: 微信支付回调验签入账
系统 SHALL 暴露 `/notify/wechat/{notify_route_id}` 接收微信异步通知,按 route_id 定位账号网关后对通知验签+解密;
验签通过且为支付成功时,MUST 以 `out_trade_no` 唯一派生订单并走支付成功核心事务(ConfirmPaySuccess,严格绑定+幂等+异常落库),
处理完毕按微信 v3 约定应答成功(ACKNotify)以停止重投;验签失败 MUST 不变更任何状态且以非 2xx 触发微信重投。

#### Scenario: 支付成功回调
- **WHEN** 微信通知验签+解密通过且交易状态为已支付
- **THEN** 按 out_trade_no 结算订单(幂等),返回微信 v3 成功应答

#### Scenario: 验签失败
- **WHEN** 通知无法验签/解密(伪造或密钥不符)
- **THEN** 不落库、不变更订单状态,返回非 2xx(微信将重投)

#### Scenario: 未知回调路由
- **WHEN** notify_route_id 无对应已配置微信账号
- **THEN** 返回 HTTP 400,不进入结算

### Requirement: 微信敏感凭据经挂载配置注入,不入库
微信商户敏感凭据配置在 `pay.wechat` 段(与 go-pay wechat.Config 对齐,`*_pem` 内容或 `*_path` 挂载文件),SHALL 只存在于【git 忽略 + 按环境挂载】的 env.yml 或 `*_path` 指向的挂载文件,MUST NOT 入库/打日志(go-pay Config 打印已脱敏),提交仓库的模板必须留空。`pay.wechat.enabled=true` 但路由必填项(app_id/mch_id/notify_url/account_id)缺失时 MUST 启动即 fail-fast;深度凭据校验由 go-pay provider 构造在启动时执行(缺私钥/证书即报错退出)。

#### Scenario: 启用微信但路由必填缺失
- **WHEN** 配置 pay.wechat.enabled=true 但缺 app_id/mch_id/notify_url/account_id 之一
- **THEN** 配置加载即报错退出(fail-fast)

#### Scenario: 启用微信但凭据缺失
- **WHEN** 路由必填齐全但缺商户私钥/证书等凭据
- **THEN** provider 构造在进程启动时报错退出,不以残缺凭据运行
