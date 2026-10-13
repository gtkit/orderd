package models

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
)

// --- 枚举 proto↔DB 映射往返 ---

func TestOrderStatusProtoRoundTrip(t *testing.T) {
	all := []OrderStatus{
		OrderPending, OrderPaid, OrderClosing, OrderClosed,
		OrderCancelled, OrderRefunding, OrderRefunded, OrderRefundException,
	}
	for _, s := range all {
		p := s.Proto()
		if p == orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
			t.Fatalf("状态 %d 映射到 UNSPECIFIED", s)
		}
		got, ok := OrderStatusFromProto(p)
		if !ok || got != s {
			t.Fatalf("往返失败:%d → %v → %d(ok=%v)", s, p, got, ok)
		}
	}
	if _, ok := OrderStatusFromProto(orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED); ok {
		t.Fatal("UNSPECIFIED 应解析失败")
	}
	// REFUND_EXCEPTION 必须能映射(评审 #7:proto/DDL 曾缺此态)。
	if OrderRefundException.Proto() != orderv1.OrderStatus_ORDER_STATUS_REFUND_EXCEPTION {
		t.Fatal("REFUND_EXCEPTION 映射错误")
	}
}

func TestRefundStatusProtoRoundTrip(t *testing.T) {
	all := []RefundStatus{RefundCreated, RefundProcessing, RefundSucceeded, RefundFailed, RefundClosed, RefundAbnormal}
	for _, s := range all {
		got, ok := RefundStatusFromProto(s.Proto())
		if !ok || got != s {
			t.Fatalf("退款状态往返失败:%d", s)
		}
	}
}

func TestActiveGuards(t *testing.T) {
	// 支付尝试活跃集合含 CLOSING(评审 #5)。
	if !AttemptClosing.Active() {
		t.Fatal("CLOSING 应计入活跃")
	}
	for _, s := range []AttemptStatus{AttemptPaid, AttemptClosed, AttemptFailed} {
		if s.Active() {
			t.Fatalf("终态 %d 不应活跃", s)
		}
	}
	if !AttemptRequesting.SubmittedToGateway() || AttemptCreated.SubmittedToGateway() {
		t.Fatal("SubmittedToGateway 边界应为 REQUESTING")
	}
	// 退款活跃集合含 ABNORMAL(评审 #7)。
	if !RefundAbnormal.Active() {
		t.Fatal("ABNORMAL 应计入活跃")
	}
	for _, s := range []RefundStatus{RefundSucceeded, RefundFailed, RefundClosed} {
		if s.Active() {
			t.Fatalf("退款终态 %d 不应活跃", s)
		}
	}
}

// --- 状态机 ---

func TestOrderTransitions(t *testing.T) {
	cases := []struct {
		from, to OrderStatus
		want     bool
	}{
		{OrderPending, OrderPaid, true},
		{OrderPending, OrderCancelled, true},
		{OrderPending, OrderClosing, true},
		{OrderClosing, OrderClosed, true},
		{OrderClosing, OrderPaid, true},
		{OrderClosed, OrderPaid, true}, // 异常恢复
		{OrderPaid, OrderRefunding, true},
		{OrderRefunding, OrderRefunded, true},
		{OrderRefunding, OrderPaid, true},
		{OrderRefunding, OrderRefundException, true},
		{OrderCancelled, OrderPaid, true}, // 有效付款可恢复取消订单
		{OrderPending, OrderRefunding, false},
		{OrderPaid, OrderCancelled, false},
		{OrderRefunded, OrderPaid, false},
	}
	for _, c := range cases {
		if got := CanOrderTransit(c.from, c.to); got != c.want {
			t.Fatalf("CanOrderTransit(%d,%d)=%v,want %v", c.from, c.to, got, c.want)
		}
	}
}

// TestCanSettlePayment 结算准入白名单全状态枚举:仅 PENDING/CLOSING/CLOSED 可结算;
// 特别地 REFUNDING 虽有 CanOrderTransit(REFUNDING,PAID)=true(退款回退专用边),但不可结算。
func TestCanSettlePayment(t *testing.T) {
	cases := []struct {
		s    OrderStatus
		want bool
	}{
		{OrderPending, true},
		{OrderClosing, true},
		{OrderClosed, true}, // 异常恢复:查单证明真实付款
		{OrderPaid, false},
		{OrderCancelled, true},
		{OrderRefunding, false}, // 退款回退边不可被支付确认借道
		{OrderRefunded, false},
		{OrderRefundException, false},
	}
	for _, c := range cases {
		if got := CanSettlePayment(c.s); got != c.want {
			t.Fatalf("CanSettlePayment(%d)=%v,want %v", c.s, got, c.want)
		}
	}
	if !CanOrderTransit(OrderRefunding, OrderPaid) {
		t.Fatal("REFUNDING→PAID 状态机边(退款失败回退)必须保留")
	}
}

func TestAttemptAndRefundTransitions(t *testing.T) {
	if !CanAttemptTransit(AttemptClosed, AttemptPaid) {
		t.Fatal("CLOSED→PAID(异常恢复)应允许")
	}
	if CanAttemptTransit(AttemptPaid, AttemptClosed) {
		t.Fatal("PAID 为终态,不可转 CLOSED")
	}
	if !CanAttemptTransit(AttemptReady, AttemptClosed) {
		t.Fatal("READY→CLOSED(换渠道时渠道同步关单成功)应允许")
	}
	if !CanRefundTransit(RefundProcessing, RefundAbnormal) {
		t.Fatal("PROCESSING→ABNORMAL 应允许")
	}
	if CanRefundTransit(RefundSucceeded, RefundFailed) {
		t.Fatal("SUCCEEDED 为终态")
	}
	// CREATED 记录已提交后,提交网关与写回 PROCESSING 不在同一事务。
	// 映射必须包含明确拒绝及成功通知/查询先到的真实恢复路径。
	for _, target := range []RefundStatus{RefundSucceeded, RefundFailed, RefundAbnormal} {
		if !CanRefundTransit(RefundCreated, target) {
			t.Errorf("CREATED 受理写回窗口必须允许收敛到 %d", target)
		}
	}
}

// --- 金额权威 ---

type fixedPolicy struct{ b AmountBounds }

func (f fixedPolicy) Bounds(context.Context, string) (AmountBounds, bool, error) {
	return f.b, true, nil
}

// errPolicy 模拟策略查询失败(fail-closed 反证)。
type errPolicy struct{}

func (errPolicy) Bounds(context.Context, string) (AmountBounds, bool, error) {
	return AmountBounds{}, false, errors.New("db down")
}

func TestMoneyValidate(t *testing.T) {
	bad := []Money{
		{Original: 100, Discount: 10, Payable: 80, Currency: "CNY"}, // 不变量违反
		{Original: 100, Discount: 10, Payable: 0, Currency: "CNY"},  // 实付非正(90 才对,这里故意 0)
		{Original: -1, Discount: 0, Payable: -1, Currency: "CNY"},   // 负数
		{Original: 90, Discount: 0, Payable: 90, Currency: ""},      // 空币种
	}
	for i, m := range bad {
		if err := m.Validate(); err == nil {
			t.Fatalf("非法金额 #%d 应被拒:%+v", i, m)
		}
	}
	if err := (Money{Original: 100, Discount: 10, Payable: 90, Currency: "CNY"}).Validate(); err != nil {
		t.Fatalf("合法金额被拒:%v", err)
	}
}

func TestValidateAmountBounds(t *testing.T) {
	p := fixedPolicy{AmountBounds{MinPayable: 50, MaxPayable: 1000, AllowedBusiness: map[string]bool{"vip": true}}}
	ok := Money{Original: 90, Discount: 0, Payable: 90, Currency: "CNY"}
	if err := ValidateAmount(context.Background(), p, "app1", ok, "vip"); err != nil {
		t.Fatalf("合规金额被拒:%v", err)
	}
	if err := ValidateAmount(context.Background(), p, "app1", Money{Original: 10, Payable: 10, Currency: "CNY"}, "vip"); err == nil {
		t.Fatal("低于下限应被拒")
	}
	if err := ValidateAmount(context.Background(), p, "app1", ok, "unknown"); err == nil {
		t.Fatal("业务类型不在白名单应被拒")
	}
	if err := ValidateAmount(context.Background(), p, "app1", Money{Original: 90, Payable: 90, Currency: "USD"}, "vip"); err == nil {
		t.Fatal("非允许币种应被拒")
	}
	// 策略查询失败 → 拒单(fail-closed),不放行金额校验。
	if err := ValidateAmount(context.Background(), errPolicy{}, "app1", ok, "vip"); !errors.Is(err, ErrAmountPolicy) {
		t.Fatalf("查询失败应包装 ErrAmountPolicy 拒单,实际: %v", err)
	}
}

// --- 幂等哈希 ---

func baseHashInput() HashInput {
	return HashInput{
		AppID: "app1", UserID: 42, ProductID: 7, ProductTitle: "会员月卡",
		BusinessType: "vip", Original: 100, Discount: 10, Payable: 90, Currency: "CNY",
		PayMethod: PayMethodWechat, ChannelID: 3, ClientOrderNo: "biz-1",
		MetadataType: "m", MetadataVersion: 1, Metadata: []byte("x"),
	}
}

func TestRequestHashDeterministicAndSensitive(t *testing.T) {
	base := baseHashInput().RequestHash()
	if len(base) != 64 {
		t.Fatalf("哈希长度应为 64,实际 %d", len(base))
	}
	if base != baseHashInput().RequestHash() {
		t.Fatal("同输入哈希应确定")
	}
	// 每个语义字段变化都必须改变哈希(评审 #9:遗漏字段会被绕过)。
	mut := map[string]func(*HashInput){
		"AppID":           func(h *HashInput) { h.AppID = "app2" },
		"UserID":          func(h *HashInput) { h.UserID = 43 },
		"ProductID":       func(h *HashInput) { h.ProductID = 8 },
		"ProductTitle":    func(h *HashInput) { h.ProductTitle = "会员年卡" },
		"BusinessType":    func(h *HashInput) { h.BusinessType = "svip" },
		"Original":        func(h *HashInput) { h.Original = 200; h.Payable = 190 },
		"Discount":        func(h *HashInput) { h.Discount = 20; h.Payable = 80 },
		"Payable":         func(h *HashInput) { h.Payable = 91; h.Original = 101 },
		"Currency":        func(h *HashInput) { h.Currency = "USD" },
		"PayMethod":       func(h *HashInput) { h.PayMethod = PayMethodAlipay },
		"ChannelID":       func(h *HashInput) { h.ChannelID = 4 },
		"ClientOrderNo":   func(h *HashInput) { h.ClientOrderNo = "biz-2" },
		"MetadataType":    func(h *HashInput) { h.MetadataType = "n" },
		"MetadataVersion": func(h *HashInput) { h.MetadataVersion = 2 },
		"Metadata":        func(h *HashInput) { h.Metadata = []byte("y") },
	}
	for name, f := range mut {
		in := baseHashInput()
		f(&in)
		if in.RequestHash() == base {
			t.Fatalf("字段 %s 变化未改变哈希", name)
		}
	}
}

// TestRequestHashNoBoundaryAmbiguity 验证长度前缀消除字段边界歧义(naive '|' 拼接会碰撞)。
func TestRequestHashNoBoundaryAmbiguity(t *testing.T) {
	x := baseHashInput()
	x.ProductTitle, x.BusinessType = "a", "bc"
	y := baseHashInput()
	y.ProductTitle, y.BusinessType = "ab", "c"
	if x.RequestHash() == y.RequestHash() {
		t.Fatal("相邻字符串字段内容位移不应产生相同哈希(边界歧义未消除)")
	}
}

func TestNormalizeKey(t *testing.T) {
	if got := NormalizeKey("  "); got.Valid {
		t.Fatal("空白应归一化为 NULL")
	}
	if got := NormalizeKey(" k "); !got.Valid || got.String != "k" {
		t.Fatalf("应去空白并有效:%+v", got)
	}
}

// --- 事件信封 ---

func TestBuildPaidEnvelope(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	o := &Order{
		AppID: "app1", OrderNo: "OD1", UserID: 42, ProductID: 7, ProductTitle: "会员",
		Status: OrderPaid, PayAmount: 90, Currency: "CNY", TradeNo: "tx1",
		PayMethod: PayMethodWechat, PayChannel: "wechat", AggregateVersion: 2,
		PaidAt: sql.NullTime{Time: now, Valid: true}, MetadataType: "m", MetadataVersion: 1, Metadata: []byte("x"),
	}
	env := BuildPaidEnvelope(o, OrderPending, "evt-1", now)
	if env.EventType != EventTypePaid || env.AggregateVersion != 2 {
		t.Fatalf("信封头错误:%+v", env)
	}
	if env.PreviousStatus != OrderPending.Proto() || env.CurrentStatus != OrderPaid.Proto() {
		t.Fatal("前后状态错误")
	}
	paid, ok := env.Payload.(*orderv1.EventEnvelope_Paid)
	if !ok {
		t.Fatalf("payload 类型错误:%T", env.Payload)
	}
	if paid.Paid.Amount != 90 || paid.Paid.TradeNo != "tx1" || paid.Paid.OrderNo != "OD1" {
		t.Fatalf("paid 载荷错误:%+v", paid.Paid)
	}
	// order_token 已从事件删除:OrderPaidEvent 不再有该字段(编译期即保证,此处断言载荷不含凭证)。
}

// TestBuildPaidEnvelopeChannel 支付事件渠道取结算快照;无快照的历史单回退下单渠道。
func TestBuildPaidEnvelopeChannel(t *testing.T) {
	tests := []struct {
		name, settled, placed, wantChannel string
		wantMethod                         PayMethod
	}{
		{"换渠道后取结算渠道", "alipay", "wechat", "alipay", PayMethodAlipay},
		{"无结算快照回退下单渠道", "", "wechat", "wechat", PayMethodWechat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := &Order{OrderNo: "OD1", Status: OrderPaid, SettledChannel: tc.settled, PayChannel: tc.placed, PayMethod: PayMethodWechat}
			paid := BuildPaidEnvelope(o, OrderPending, "evt-1", time.Now()).GetPaid()
			if paid.PayChannel != tc.wantChannel || paid.PayMethod != int32(tc.wantMethod) {
				t.Fatalf("期望 %s/%d,实际 %s/%d", tc.wantChannel, tc.wantMethod, paid.PayChannel, paid.PayMethod)
			}
		})
	}
}
