package models

import "errors"

// 领域错误(哨兵值)。应用层用 errors.Is 判定,并映射到 gRPC status code。
var (
	// ErrInvalidAmount 金额不满足基础不变量(非负/正/original-discount=payable/币种)。
	ErrInvalidAmount = errors.New("金额不合法")
	// ErrAmountPolicy 金额未通过 app 级白名单/上下限。
	ErrAmountPolicy = errors.New("金额策略校验失败")
	// ErrIdempotencyConflict 同一幂等键绑定了不同的归一化请求(同 key 异 hash)。
	ErrIdempotencyConflict = errors.New("幂等键冲突")
	// ErrClientOrderNoConflict 同 app 下 client_order_no 已被其它订单占用(与幂等键冲突语义不同:
	// 前者应查旧单/换业务单号,后者是同键改参)。
	ErrClientOrderNoConflict = errors.New("业务单号已存在")
	// ErrIllegalTransition 非法状态跃迁。
	ErrIllegalTransition = errors.New("非法状态跃迁")
	// ErrOrderNotFound 订单不存在。
	ErrOrderNotFound = errors.New("订单不存在")
	// ErrRefundNotFound 退款单不存在。
	ErrRefundNotFound = errors.New("退款单不存在")
	// ErrOrderNotOwned 订单不属于该用户(归属校验失败)。
	ErrOrderNotOwned = errors.New("订单归属不符")
	// ErrOrderNotPending 订单不处于 PENDING(取消/建尝试的前置)。
	ErrOrderNotPending = errors.New("订单非待支付")
	// ErrActiveAttemptExists 已存在活跃支付尝试(建新尝试冲突)。
	ErrActiveAttemptExists = errors.New("已存在活跃支付尝试")
	// ErrRefundNotAllowed 退款未授权(V1 双重关闭)或订单状态不允许退款。
	ErrRefundNotAllowed = errors.New("退款不被允许")
	// ErrActiveRefundExists 已存在活跃退款。
	ErrActiveRefundExists = errors.New("已存在活跃退款")
	// ErrPaymentAnomaly 资金异常(重复回调不一致/第二笔真实付款/金额账号不符),需人工。
	ErrPaymentAnomaly = errors.New("资金异常")
)
