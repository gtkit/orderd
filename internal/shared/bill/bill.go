// Package bill 定义流式渠道账单与受限正向资金确认端口。
package bill

import (
	"context"
	"time"

	"github.com/gtkit/orderd/internal/models"
)

// Location 是人民币商户账单日界使用的固定东八区，不依赖宿主机时区。
var Location = time.FixedZone("Asia/Shanghai", 8*60*60)

// Entry 是一条规范化渠道证据。退款 EventAt 可能是受理时间，不能当成功时间。
// 支付宝退款没有独立渠道退款号，TransactionID 留空，原 trade_no 放 OriginalTransactionID。
// ProviderAppID 只保留文件中真实存在的值，不用请求配置补造。
type Entry struct {
	Kind                  models.TxnKind
	TransactionID         string
	OriginalTransactionID string
	OutTradeNo            string
	OutRefundNo           string
	OrderNo               string
	Amount                int64
	Currency              string
	EventAt               time.Time
	MerchantID            string
	ProviderAppID         string
	Status                string
}

// Summary 在整个账单校验完成后返回；本金按方向分开，不以净额对平。
type Summary struct {
	DigestAlgorithm string // 渠道校验或本地审计摘要算法；不得把本地摘要冒充渠道签名。
	Digest          string
	Count           int64
	PayAmount       int64
	RefundAmount    int64
	NoStatement     bool
}

// Bill 仅为现有测试夹具保留；真实 Source 不构建整日 Entries。
type Bill struct {
	Entries     []Entry
	TotalAmount int64
}

// Source 同步流式输出。emit 的结果必须保持未发布，直到 Fetch 成功结束。
// 实现必须继承 ctx，任何读入/验签/解析/emit 错误都返回失败，不能伪造空账单。
type Source interface {
	Fetch(ctx context.Context, channel, payAccountID string, billDate time.Time, emit func(Entry) error) (Summary, error)
}

// RepairRequest 只来自已完整发布的账单证据，不是调用方自报金额。
type RepairRequest struct {
	AppID        string
	Channel      string
	PayAccountID string
	Entry        Entry
}

// Repairer 仅对已有对象执行成功查单后的确认，true 还须证明成功流水身份完整一致。
// 负面/不确定结果禁止更新业务资金状态、查询计数或退款守卫。
type Repairer interface {
	VerifyAndConfirmFromBill(context.Context, RepairRequest) (bool, error)
}

// Success 是完整验签渠道查单后保留的资金事实；Confirmed=false 时不允许资金写入。
// At 缺失保持零值，不能用账单受理时间或本机时间填充。
type Success struct {
	Confirmed             bool
	TransactionID         string
	OriginalTransactionID string
	OutTradeNo            string
	OutRefundNo           string
	Amount                int64
	Currency              string
	At                    time.Time
}

// Verifier 直接使用能够保留身份、币种和精确金额的已验签SDK响应。
// 通用paymgr查询结果丢弃这些字段，不能用于此严格补偿入口。
type Verifier interface {
	Verify(ctx context.Context, channel, payAccountID string, e Entry) (Success, error)
}
