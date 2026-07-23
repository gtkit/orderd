# rest-gateway

## ADDED Requirements

### Requirement: gRPC 与 REST 同源
系统 SHALL 由同一份 `proto/order/v1` 契约生成并对外同时提供 gRPC 与 REST(grpc-gateway),并产出 OpenAPI v2 文档(`pb/order.swagger.json`)供 PHP 等非 gRPC 接入方对接。REST 请求 MUST 经回环转发到 gRPC 服务处理,复用同一套鉴权、参数校验与错误映射,不得旁路业务或另立一套语义。

#### Scenario: REST 只读请求转发并复用语义
- **WHEN** 接入方以 `GET /v1/orders/status?order_token=...&user_id=...` 携带合法 `Authorization: Bearer <JWT>` 调用
- **THEN** 网关将其转发到 gRPC `GetOrderStatus`,经同一鉴权与校验后返回 HTTP 200,响应体为对应 gRPC 消息的 JSON

#### Scenario: OpenAPI 文档随生成物交付
- **WHEN** 执行 `make gen`
- **THEN** 在 `pb/` 下生成 `order.swagger.json`,覆盖全部对外 RPC 的 REST 路径与消息结构

### Requirement: REST 鉴权与 gRPC 一致
REST 端点 SHALL 复用 gRPC 的 JWT 鉴权:身份只经 `Authorization` 头传递,MUST NOT 在 URL path/query 暴露鉴权信息;鉴权失败的映射与 gRPC 一致。

#### Scenario: 缺失或非法凭据
- **WHEN** REST 请求缺少或携带非法 `Authorization` 头
- **THEN** 复用 gRPC 鉴权拦截器判定为 Unauthenticated,网关返回 HTTP 401,不进入业务处理
