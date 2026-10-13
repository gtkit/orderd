package alipay

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	periodPattern = regexp.MustCompile(`^#起始日期：\[([^]]+)\]\s+终止日期：\[([^]]+)\]$`)
	totalPattern  = regexp.MustCompile(`^#(交易|退款)合计：([0-9]+)笔，商家实收(退款)?共(-?[0-9]+(?:\.[0-9]{1,2})?)元，商家优惠(退款)?共(-?[0-9]+(?:\.[0-9]{1,2})?)元$`)
)

// parseArchive reads each ZIP member to EOF, verifying CRC even for the unused summary.
// Temporary files bound memory; no archive names become filesystem paths.
func parseArchive(ctx context.Context, r io.ReaderAt, size int64, seller string, day time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	if err := checkArchive(r, size); err != nil {
		return bill.Summary{}, err
	}
	zr, err := zip.NewReader(r, size)
	if err != nil || len(zr.File) == 0 || len(zr.File) > maxArchiveFiles {
		return bill.Summary{}, ErrInvalidBill
	}
	var total int64
	var found bool
	var result bill.Summary
	for _, zf := range zr.File {
		if err := ctx.Err(); err != nil {
			return bill.Summary{}, err
		}
		if !zf.Mode().IsRegular() || !strings.HasSuffix(strings.ToLower(zf.Name), ".csv") || zf.Flags&1 != 0 {
			return bill.Summary{}, ErrInvalidBill
		}
		if zf.UncompressedSize64 > uint64(billio.MaxExpandedBytes-total) {
			return bill.Summary{}, ErrInvalidBill
		}
		rc, err := zf.Open()
		if err != nil {
			return bill.Summary{}, ErrInvalidBill
		}
		f, n, spoolErr := billio.Spool(ctx, rc, billio.MaxExpandedBytes-total)
		closeErr := rc.Close()
		if spoolErr != nil || closeErr != nil {
			if f != nil {
				_ = f.Close()
				_ = os.Remove(f.Name())
			}
			return bill.Summary{}, errors.Join(ErrInvalidBill, ctx.Err())
		}
		total += n
		summary, detail, parseErr := parseFile(ctx, f, seller, day, emit)
		_ = f.Close()
		_ = os.Remove(f.Name())
		if parseErr != nil {
			return bill.Summary{}, parseErr
		}
		if detail {
			if found {
				return bill.Summary{}, ErrInvalidBill
			}
			found = true
			result = summary
		}
	}
	if !found {
		return bill.Summary{}, ErrInvalidBill
	}
	return result, nil
}

func parseFile(ctx context.Context, f *os.File, seller string, day time.Time, emit func(bill.Entry) error) (bill.Summary, bool, error) {
	// The exact Chinese title selects UTF-8 or GBK, rather than guessing from a filename.
	prefix := make([]byte, 128)
	n, err := f.Read(prefix)
	if err != nil && !errors.Is(err, io.EOF) {
		return bill.Summary{}, false, ErrInvalidBill
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return bill.Summary{}, false, ErrInvalidBill
	}
	var reader io.Reader = f
	if !bytes.HasPrefix(bytes.TrimPrefix(prefix[:n], []byte{0xef, 0xbb, 0xbf}), []byte("#支付宝业务")) {
		reader = transform.NewReader(f, simplifiedchinese.GBK.NewDecoder())
	}
	sc := bufio.NewScanner(reader)
	sc.Buffer(make([]byte, 4096), int(billio.MaxLineBytes)+1)
	var lines int
	var decoded int64
	var detail bool
	p := parser{seller: seller, day: day}
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return bill.Summary{}, false, err
		}
		line := sc.Text()
		decoded += int64(len(line)) + 1
		if decoded > billio.MaxExpandedBytes || len(line) > int(billio.MaxLineBytes) || !utf8.ValidString(line) || strings.ContainsRune(line, utf8.RuneError) {
			return bill.Summary{}, false, ErrInvalidBill
		}
		lines++
		if lines == 1 {
			line = strings.TrimPrefix(line, "\ufeff")
			switch line {
			case "#支付宝业务明细查询":
				detail = true
			case "#支付宝业务汇总查询":
				detail = false
			default:
				return bill.Summary{}, false, ErrInvalidBill
			}
			continue
		}
		if detail {
			if err := p.line(line, emit); err != nil {
				return bill.Summary{}, false, fmt.Errorf("alipay bill line %d: %w", lines, err)
			}
		}
	}
	if sc.Err() != nil || lines == 0 {
		return bill.Summary{}, false, ErrInvalidBill
	}
	if !detail {
		return bill.Summary{}, false, nil
	}
	if !p.exported || !p.payTotal || !p.refundTotal {
		return bill.Summary{}, false, ErrInvalidBill
	}
	return p.summary, true, nil
}

type parser struct {
	seller                                                        string
	day                                                           time.Time
	account, period, list, ended, payTotal, refundTotal, exported bool
	columns                                                       map[string]int
	summary                                                       bill.Summary
	payCount, refundCount, payReceipt, refundReceipt              int64
}

func (p *parser) line(raw string, emit func(bill.Entry) error) error {
	line := strings.TrimSpace(raw)
	if line == "" {
		return nil
	}
	if p.exported {
		return ErrInvalidBill
	}
	if !p.account {
		if line != "#账号：["+p.seller+"]" {
			return ErrInvalidBill
		}
		p.account = true
		return nil
	}
	if !p.period {
		parts := periodPattern.FindStringSubmatch(line)
		if len(parts) != 3 {
			return ErrInvalidBill
		}
		start, e1 := time.ParseInLocation("2006年01月02日 15:04:05", parts[1], channelLocation)
		end, e2 := time.ParseInLocation("2006年01月02日 15:04:05", parts[2], channelLocation)
		if e1 != nil || e2 != nil || !start.Equal(p.day) || !end.Equal(p.day.AddDate(0, 0, 1)) {
			return ErrInvalidBill
		}
		p.period = true
		return nil
	}
	if !p.list {
		if !separator(line, "业务明细列表") {
			return ErrInvalidBill
		}
		p.list = true
		return nil
	}
	if p.columns == nil {
		cols, err := csvLine(line)
		if err != nil {
			return err
		}
		p.columns = make(map[string]int, len(cols))
		for i, name := range cols {
			name = strings.TrimSpace(name)
			if name == "退款批次号" {
				name = "退款批次号/请求号"
			}
			if _, exists := p.columns[name]; exists || name == "" {
				return ErrInvalidBill
			}
			p.columns[name] = i
		}
		for _, name := range []string{"支付宝交易号", "商户订单号", "业务类型", "完成时间", "订单金额（元）", "商家实收（元）", "退款批次号/请求号"} {
			if _, ok := p.columns[name]; !ok {
				return ErrInvalidBill
			}
		}
		return nil
	}
	if !p.ended {
		if separator(line, "业务明细列表结束") {
			p.ended = true
			return nil
		}
		if strings.HasPrefix(line, "#") {
			return ErrInvalidBill
		}
		return p.entry(raw, emit)
	}
	if strings.HasPrefix(line, "#导出时间：[") && strings.HasSuffix(line, "]") {
		if !p.payTotal || !p.refundTotal {
			return ErrInvalidBill
		}
		value := strings.TrimSuffix(strings.TrimPrefix(line, "#导出时间：["), "]")
		if _, err := time.ParseInLocation(time.DateTime, value, channelLocation); err != nil {
			return ErrInvalidBill
		}
		p.exported = true
		return nil
	}
	parts := totalPattern.FindStringSubmatch(line)
	if len(parts) != 7 {
		return ErrInvalidBill
	}
	count, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return ErrInvalidBill
	}
	amount, err := billio.ParseAmount(parts[4])
	if err != nil {
		return ErrInvalidBill
	}
	// Discount totals are syntax checked, but never confused with order principal.
	if _, err := billio.ParseAmount(parts[6]); err != nil {
		return ErrInvalidBill
	}
	if parts[1] == "交易" {
		if p.payTotal || parts[3] != "" || parts[5] != "" || count != p.payCount || amount != p.payReceipt {
			return ErrInvalidBill
		}
		p.payTotal = true
	} else {
		if p.refundTotal || parts[3] != "退款" || parts[5] != "退款" || count != p.refundCount || amount != p.refundReceipt {
			return ErrInvalidBill
		}
		p.refundTotal = true
	}
	return nil
}

func separator(line, label string) bool {
	if !strings.HasPrefix(line, "#-") {
		return false
	}
	body := strings.Trim(strings.TrimPrefix(line, "#"), "-")
	return body == label
}

// This template accepts one CSV record per physical line; quoted multiline fields
// are unsupported and fail closed instead of growing an unbounded record buffer.
func csvLine(line string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(line))
	row, err := r.Read()
	if err != nil {
		return nil, ErrInvalidBill
	}
	if _, err = r.Read(); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidBill
	}
	return row, nil
}

func (p *parser) entry(line string, emit func(bill.Entry) error) error {
	row, err := csvLine(line)
	if err != nil || len(row) != len(p.columns) {
		return ErrInvalidBill
	}
	// Additional columns are normally informational. Explicit currency columns
	// must not silently broaden this domestic CNY template into cross-border data.
	for name, i := range p.columns {
		lower := strings.ToLower(name)
		if strings.Contains(name, "币种") || lower == "currency" || strings.HasSuffix(lower, "_currency") {
			if strings.TrimSpace(row[i]) != "CNY" {
				return ErrInvalidBill
			}
		}
	}
	get := func(name string) string { return strings.TrimSpace(row[p.columns[name]]) }
	trade, out, refund := get("支付宝交易号"), get("商户订单号"), get("退款批次号/请求号")
	if trade == "" || out == "" || len(trade) > 128 || len(out) > 64 || len(refund) > 64 {
		return ErrInvalidBill
	}
	amount, err := billio.ParseAmount(get("订单金额（元）"))
	if err != nil || amount == 0 || amount == math.MinInt64 {
		return ErrInvalidBill
	}
	receipt, err := billio.ParseAmount(get("商家实收（元）"))
	if err != nil {
		return ErrInvalidBill
	}
	at, err := time.ParseInLocation(time.DateTime, get("完成时间"), channelLocation)
	if err != nil || at.Before(p.day) || !at.Before(p.day.AddDate(0, 0, 1)) {
		return ErrInvalidBill
	}
	entry := bill.Entry{OutTradeNo: out, Currency: "CNY", MerchantID: p.seller, EventAt: at, Status: "SUCCESS"}
	switch get("业务类型") {
	case "交易":
		if amount < 0 || receipt < 0 || refund != "" {
			return ErrInvalidBill
		}
		entry.Kind = models.TxnPay
		entry.TransactionID = trade
		entry.Amount = amount
		p.summary.PayAmount, err = billio.AddAmount(p.summary.PayAmount, amount)
		if err != nil {
			return ErrInvalidBill
		}
		p.payReceipt, err = billio.AddAmount(p.payReceipt, receipt)
		p.payCount++
	case "退款":
		if amount > 0 || receipt > 0 || refund == "" {
			return ErrInvalidBill
		}
		entry.Kind = models.TxnRefund
		entry.OriginalTransactionID = trade
		entry.OutRefundNo = refund
		entry.Amount = -amount
		p.summary.RefundAmount, err = billio.AddAmount(p.summary.RefundAmount, -amount)
		if err != nil {
			return ErrInvalidBill
		}
		p.refundReceipt, err = billio.AddAmount(p.refundReceipt, receipt)
		p.refundCount++
	default:
		return ErrInvalidBill
	}
	if err != nil {
		return ErrInvalidBill
	}
	p.summary.Count++
	if p.summary.Count > billio.MaxEntries {
		return ErrInvalidBill
	}
	return emit(entry)
}
