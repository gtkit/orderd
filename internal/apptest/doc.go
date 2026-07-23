// Package apptest 是应用层端到端测试脚手架:用 memstore + 各 fake 装配 order/payment/refund
// 三个应用服务,覆盖资金主链的并发/幂等/异常矩阵(评审 §17 中不依赖真实 DB/网关的用例)。
package apptest
