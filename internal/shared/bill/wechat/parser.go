package wechat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
)

var detailHeaders = strings.Split("交易时间,公众账号ID,商户号,特约商户号,设备号,微信订单号,商户订单号,用户标识,交易类型,交易状态,付款银行,货币种类,应结订单金额,代金券金额,微信退款单号,商户退款单号,退款金额,充值券退款金额,退款类型,退款状态,商品名称,商户数据包,手续费,费率,订单金额,申请退款金额,费率备注", ",")
var totalHeaders = strings.Split("总交易单数,应结订单总金额,退款总金额,充值券退款总金额,手续费总金额,订单总金额,申请退款总金额", ",")
var summedFields = []string{"应结订单金额", "退款金额", "充值券退款金额", "手续费", "订单金额", "申请退款金额"}

// parse uses one bounded line and fixed-size totals. emit persists duplicate
// identities under the import's unique key; the parser retains no whole-day map.
// Official escaped merchant text is not RFC4180 quoted CSV.
func parse(ctx context.Context, r io.Reader, merchant string, day time.Time, emit func(bill.Entry) error, maxEntries int64) (bill.Summary, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), billio.MaxLineBytes+2)
	lineNo := 0
	next := func() (string, bool, error) {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if !scan.Scan() {
			return "", false, scan.Err()
		}
		lineNo++
		line := scan.Text()
		if len(line) > billio.MaxLineBytes || !utf8.ValidString(line) {
			return "", false, errors.New("wechat bill: invalid UTF8 or oversized line")
		}
		return line, true, nil
	}
	line, ok, err := next()
	if err != nil {
		return bill.Summary{}, err
	}
	if !ok {
		return bill.Summary{}, errors.New("wechat bill: missing header")
	}
	head, err := headers(strings.TrimPrefix(line, "\ufeff"), detailHeaders)
	if err != nil {
		return bill.Summary{}, err
	}
	var summary bill.Summary
	var totals [6]int64
	for {
		line, ok, err = next()
		if err != nil {
			return bill.Summary{}, fmt.Errorf("wechat bill: line %d: %w", lineNo+1, err)
		}
		if !ok {
			return bill.Summary{}, errors.New("wechat bill: missing totals")
		}
		if !strings.HasPrefix(line, "`") {
			tail, err := headers(line, totalHeaders)
			if err != nil {
				return bill.Summary{}, err
			}
			line, ok, err = next()
			if err != nil {
				return bill.Summary{}, err
			}
			if !ok {
				return bill.Summary{}, errors.New("wechat bill: missing totals values")
			}
			values, err := fields(line, len(tail))
			if err != nil {
				return bill.Summary{}, err
			}
			count := values[tail["总交易单数"]]
			for _, c := range count {
				if c < '0' || c > '9' {
					return bill.Summary{}, errors.New("wechat bill: invalid summary count")
				}
			}
			n, err := strconv.ParseInt(count, 10, 64)
			if err != nil || n != summary.Count {
				return bill.Summary{}, errors.New("wechat bill: summary count mismatch")
			}
			for i, name := range totalHeaders[1:] {
				amount, err := billio.ParseAmount(values[tail[name]])
				if err != nil || amount != totals[i] {
					return bill.Summary{}, errors.New("wechat bill: summary amount mismatch")
				}
			}
			for {
				line, ok, err = next()
				if err != nil {
					return bill.Summary{}, err
				}
				if !ok {
					return summary, nil
				}
				if line != "" {
					return bill.Summary{}, errors.New("wechat bill: data after summary")
				}
			}
		}
		if summary.Count >= maxEntries {
			return bill.Summary{}, billio.ErrLimit
		}
		values, err := fields(line, len(head))
		if err != nil {
			return bill.Summary{}, fmt.Errorf("wechat bill: line %d: %w", lineNo, err)
		}
		e, amounts, err := entry(values, head, merchant, day)
		if err != nil {
			return bill.Summary{}, fmt.Errorf("wechat bill: line %d: %w", lineNo, err)
		}
		for i, amount := range amounts {
			totals[i], err = billio.AddAmount(totals[i], amount)
			if err != nil {
				return bill.Summary{}, err
			}
		}
		summary.Count++
		summary.PayAmount = totals[4]
		summary.RefundAmount = totals[5]
		if err := emit(e); err != nil {
			return bill.Summary{}, fmt.Errorf("wechat bill: emit: %w", err)
		}
	}
}

func headers(line string, required []string) (map[string]int, error) {
	result := make(map[string]int)
	for i, name := range strings.Split(line, ",") {
		if name == "" {
			return nil, errors.New("wechat bill: empty header")
		}
		if _, ok := result[name]; ok {
			return nil, errors.New("wechat bill: duplicate header")
		}
		result[name] = i
	}
	for _, name := range required {
		if _, ok := result[name]; !ok {
			return nil, errors.New("wechat bill: unsupported or missing header")
		}
	}
	return result, nil
}

func fields(line string, count int) ([]string, error) {
	values := strings.Split(line, ",")
	if len(values) != count {
		return nil, errors.New("wechat bill: column count mismatch")
	}
	for i, value := range values {
		var ok bool
		values[i], ok = strings.CutPrefix(value, "`")
		if !ok {
			return nil, errors.New("wechat bill: missing value prefix")
		}
	}
	return values, nil
}

func identity(value string) bool {
	if value == "" || value == "0" || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if c <= 32 || c >= 127 || c == '`' || c == '\\' {
			return false
		}
	}
	return true
}

func entry(values []string, h map[string]int, merchant string, day time.Time) (bill.Entry, [6]int64, error) {
	value := func(name string) string { return values[h[name]] }
	var e bill.Entry
	var sums [6]int64
	invalid := func() (bill.Entry, [6]int64, error) {
		return bill.Entry{}, [6]int64{}, errors.New("wechat bill: invalid row identity, amount, status or date")
	}
	if value("商户号") != merchant || value("特约商户号") != "0" || !identity(value("公众账号ID")) || !identity(value("微信订单号")) || !identity(value("商户订单号")) || value("货币种类") != "CNY" {
		return invalid()
	}
	at, err := time.ParseInLocation(time.DateTime, value("交易时间"), bill.Location)
	if err != nil || at.Format(time.DateTime) != value("交易时间") || at.Before(day) || !at.Before(day.AddDate(0, 0, 1)) {
		return invalid()
	}
	for i, name := range summedFields {
		sums[i], err = billio.ParseAmount(value(name))
		if err != nil || (sums[i] < 0 && name != "手续费") {
			return invalid()
		}
	}
	coupon, err := billio.ParseAmount(value("代金券金额"))
	if err != nil || coupon < 0 {
		return invalid()
	}
	e = bill.Entry{TransactionID: value("微信订单号"), OutTradeNo: value("商户订单号"), Currency: "CNY", EventAt: at, MerchantID: merchant, ProviderAppID: value("公众账号ID"), Status: value("交易状态")}
	switch e.Status {
	case "SUCCESS":
		if sums[4] <= 0 || sums[0] > sums[4] || coupon > sums[4] || sums[1] != 0 || sums[2] != 0 || sums[5] != 0 || value("微信退款单号") != "0" || value("商户退款单号") != "0" || value("退款状态") != "" {
			return invalid()
		}
		e.Kind = models.TxnPay
		e.Amount = sums[4]
	case "REFUND", "REVOKED":
		if sums[5] <= 0 || sums[1] > sums[5] || sums[2] > sums[1] || sums[0] != 0 || sums[4] != 0 || coupon != 0 || !identity(value("微信退款单号")) || !identity(value("商户退款单号")) {
			return invalid()
		}
		switch value("退款状态") {
		case "SUCCESS", "PROCESSING", "FAIL", "CHANGE":
		default:
			return invalid()
		}
		e.Kind = models.TxnRefund
		e.Amount = sums[5]
		e.OriginalTransactionID = e.TransactionID
		e.TransactionID = value("微信退款单号")
		e.OutRefundNo = value("商户退款单号")
		e.Status += ":" + value("退款状态")
	default:
		return invalid()
	}
	return e, sums, nil
}
