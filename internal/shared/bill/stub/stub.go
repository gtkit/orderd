// Package stub 是账单源占位:未接真实渠道账单时返回错误,避免把"空账单"误判为短款。
package stub

import (
	"context"
	"errors"
	"time"

	"github.com/gtkit/orderd/internal/shared/bill"
)

// ErrNotImplemented 真实渠道账单源未接入。
var ErrNotImplemented = errors.New("bill/stub: 渠道账单源未接入")

// Source 恒返回错误(对账 worker 会将该账期批次标记为失败,不产生假差异)。
type Source struct{}

func (Source) Fetch(context.Context, string, string, time.Time, func(bill.Entry) error) (bill.Summary, error) {
	return bill.Summary{}, ErrNotImplemented
}
