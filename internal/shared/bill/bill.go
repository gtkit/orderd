// Package bill 定义渠道账单源端口(对账用)。真实实现按渠道拉取当日对账单并解析;
// phase 阶段用注入的 fake/文件源,便于本环境验证对账逻辑。
package bill

import (
	"context"
	"time"
)

// Entry 账单中的一笔支付记录(V1 对账聚焦支付;退款对账为后续)。
type Entry struct {
	TransactionID string // 渠道交易号
	OutTradeNo    string // 商户订单号
	OrderNo       string // 订单号(渠道账单可能带,可空)
	Amount        int64  // 金额(分)
}

// Bill 某账号某日的渠道账单。
type Bill struct {
	Entries     []Entry
	TotalAmount int64
}

// Source 拉取渠道账单。
type Source interface {
	Fetch(ctx context.Context, channel, payAccountID string, billDate time.Time) (Bill, error)
}
