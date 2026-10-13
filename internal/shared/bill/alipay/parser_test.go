package alipay

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
	"golang.org/x/text/encoding/simplifiedchinese"
)

var fixtureDay = time.Date(2025, 1, 2, 0, 0, 0, 0, channelLocation)

const fixtureSeller = "2088000000000001"
const fixtureHeader = "支付宝交易号,商户订单号,业务类型,完成时间,订单金额（元）,商家实收（元）,退款批次号/请求号,商品名称"
const fixturePay = "trade1,order1,交易,2025-01-02 12:00:00,10.00,9.00,,商品"
const fixtureRefund = "trade1,order1,退款,2025-01-02 13:00:00,-2.00,-1.80,refund1,商品"

func fixtureCSV(rows []string, payN, refundN int, payReceipt, refundReceipt string) string {
	return "#支付宝业务明细查询\n#账号：[" + fixtureSeller + "]\n#起始日期：[2025年01月02日 00:00:00]   终止日期：[2025年01月03日 00:00:00]\n#---业务明细列表---\n" + fixtureHeader + "\n" + strings.Join(rows, "\n") + "\n#---业务明细列表结束---\n" +
		fmt.Sprintf("#交易合计：%d笔，商家实收共%s元，商家优惠共0.00元\n#退款合计：%d笔，商家实收退款共%s元，商家优惠退款共0.00元\n#导出时间：[2025-01-03 09:00:00]\n", payN, payReceipt, refundN, refundReceipt)
}

func zipped(t *testing.T, files ...[]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for i, data := range files {
		f, err := w.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("%d.csv", i), Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func parseFixture(t *testing.T, data []byte) (bill.Summary, []bill.Entry, error) {
	t.Helper()
	var entries []bill.Entry
	sum, err := parseArchive(t.Context(), bytes.NewReader(data), int64(len(data)), fixtureSeller, fixtureDay, func(e bill.Entry) error { entries = append(entries, e); return nil })
	return sum, entries, err
}

func TestParseArchivePaymentRefundAndEncodings(t *testing.T) {
	t.Parallel()
	csvText := fixtureCSV([]string{fixturePay, fixtureRefund, strings.Replace(fixtureRefund, "refund1", "refund2", 1)}, 1, 2, "9.00", "-3.60")
	gbk, err := simplifiedchinese.GBK.NewEncoder().Bytes([]byte(csvText))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"utf8", []byte(csvText)}, {"bom", []byte("\ufeff" + csvText)}, {"gbk", gbk},
		{"refund_alias", []byte(strings.Replace(csvText, "退款批次号/请求号", "退款批次号", 1))},
		{"quoted_comma", []byte(strings.ReplaceAll(csvText, ",商品\n", ",\"商品,含逗号\"\n"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum, entries, err := parseFixture(t, zipped(t, tc.data, []byte("#支付宝业务汇总查询\n")))
			if err != nil {
				t.Fatal(err)
			}
			if sum.Count != 3 || sum.PayAmount != 1000 || sum.RefundAmount != 400 || len(entries) != 3 {
				t.Fatalf("sum=%+v entries=%d", sum, len(entries))
			}
			if entries[0].Kind != models.TxnPay || entries[0].TransactionID != "trade1" || entries[0].Currency != "CNY" {
				t.Fatalf("payment=%+v", entries[0])
			}
			if entries[1].Kind != models.TxnRefund || entries[1].TransactionID != "" || entries[1].OriginalTransactionID != "trade1" || entries[1].OutRefundNo != "refund1" || entries[2].OutRefundNo != "refund2" {
				t.Fatalf("refunds=%+v", entries[1:])
			}
			if entries[0].ProviderAppID != "" || entries[0].MerchantID != fixtureSeller || !entries[0].EventAt.Equal(fixtureDay.Add(12*time.Hour)) {
				t.Fatalf("scope/time=%+v", entries[0])
			}
		})
	}
}

func TestParseArchiveRejectsMalformedEvidence(t *testing.T) {
	t.Parallel()
	valid := fixtureCSV([]string{fixturePay, fixtureRefund}, 1, 1, "9.00", "-1.80")
	for _, tc := range []struct{ name, from, to string }{
		{"merchant", fixtureSeller, "another"},
		{"wrong_period", "2025年01月03日", "2025年01月04日"},
		{"wrong_event_day", "2025-01-02 12:00:00", "2025-01-01 12:00:00"},
		{"bad_time", "2025-01-02 12:00:00", "2025-01-02 24:00:00"},
		{"missing_header", "支付宝交易号", "原支付号"},
		{"duplicate_header", "商品名称", "商户订单号"},
		{"missing_id", "trade1,order1,交易", ",order1,交易"},
		{"missing_refund_id", "-1.80,refund1", "-1.80,"},
		{"unknown_type", ",退款,", ",撤销,"},
		{"positive_refund", ",-2.00,", ",2.00,"},
		{"negative_payment", ",10.00,", ",-10.00,"},
		{"zero", ",10.00,", ",0.00,"},
		{"fraction", ",10.00,", ",10.001,"},
		{"exponent", ",10.00,", ",1e1,"},
		{"overflow", ",10.00,", ",92233720368547758.08,"},
		{"min_negative", ",-2.00,", ",-92233720368547758.08,"},
		{"count", "#交易合计：1笔", "#交易合计：2笔"},
		{"receipt_total", "商家实收共9.00元", "商家实收共10.00元"},
		{"no_end", "#---业务明细列表结束---\n", ""},
		{"no_refund_total", "#退款合计：1笔，商家实收退款共-1.80元，商家优惠退款共0.00元\n", ""},
		{"no_export", "#导出时间：[2025-01-03 09:00:00]", ""},
		{"invalid_encoding", ",商品", ",\xff"},
		{"bad_csv", ",商品", ",\"unterminated"},
		{"multiline_record", ",商品", ",\"商品\n名称\""},
		{"extra_after_end", "#导出时间：[2025-01-03 09:00:00]", "#导出时间：[2025-01-03 09:00:00]\nmore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum, _, err := parseFixture(t, zipped(t, []byte(strings.ReplaceAll(valid, tc.from, tc.to))))
			if !errors.Is(err, ErrInvalidBill) || sum.Count != 0 {
				t.Fatalf("sum=%+v err=%v", sum, err)
			}
		})
	}
}

func TestArchiveIntegrityAndLimits(t *testing.T) {
	t.Parallel()
	valid := []byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00"))
	crc := zipped(t, valid)
	idx := bytes.Index(crc, []byte("商品"))
	if idx < 0 {
		t.Fatal("fixture not found")
	}
	crc[idx] ^= 1
	many := make([][]byte, maxArchiveFiles+1)
	for i := range many {
		many[i] = valid
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"corrupt_crc", crc}, {"not_zip", []byte("<html>expired</html>")},
		{"truncated_zip", zipped(t, valid)[:50]},
		{"ambiguous_detail", zipped(t, valid, valid)},
		{"no_detail", zipped(t, []byte("#支付宝业务汇总查询\n"))},
		{"unknown_member", zipped(t, valid, []byte("unknown"))},
		{"nested_zip", zipped(t, zipped(t, valid))},
		{"file_limit", zipped(t, many...)},
		{"line_limit", zipped(t, []byte(strings.Replace(string(valid), "商品", strings.Repeat("x", billio.MaxLineBytes+1), 1)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if sum, _, err := parseFixture(t, tc.data); !errors.Is(err, ErrInvalidBill) || sum.Count != 0 {
				t.Fatalf("sum=%+v err=%v", sum, err)
			}
		})
	}
}

func TestLargePrincipalAndAggregate(t *testing.T) {
	t.Parallel()
	for _, amount := range []string{"90071992547409.93", "92233720368547758.07"} {
		text := fixtureCSV([]string{strings.Replace(fixturePay, "10.00", amount, 1)}, 1, 0, "9.00", "0.00")
		sum, entries, err := parseFixture(t, zipped(t, []byte(text)))
		if err != nil {
			t.Fatal(err)
		}
		want, _ := billio.ParseAmount(amount)
		if entries[0].Amount != want || sum.PayAmount != want {
			t.Fatalf("precision lost: %+v", sum)
		}
	}
	a := strings.Replace(fixturePay, "10.00", "92233720368547758.07", 1)
	text := fixtureCSV([]string{a, fixturePay}, 2, 0, "18.00", "0.00")
	if _, _, err := parseFixture(t, zipped(t, []byte(text))); !errors.Is(err, ErrInvalidBill) {
		t.Fatalf("overflow err=%v", err)
	}
}

func TestEmptyBillAndInterruptedEmission(t *testing.T) {
	t.Parallel()
	data := zipped(t, []byte(fixtureCSV(nil, 0, 0, "0.00", "0.00")))
	if sum, entries, err := parseFixture(t, data); err != nil || sum.Count != 0 || sum.NoStatement || len(entries) != 0 {
		t.Fatalf("sum=%+v err=%v", sum, err)
	}
	data = zipped(t, []byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00")))
	stop := errors.New("storage failed")
	_, err := parseArchive(t.Context(), bytes.NewReader(data), int64(len(data)), fixtureSeller, fixtureDay, func(bill.Entry) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("emit error lost: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = parseArchive(ctx, bytes.NewReader(data), int64(len(data)), fixtureSeller, fixtureDay, func(bill.Entry) error { t.Fatal("emitted after cancellation"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestParserStreamsManyRows(t *testing.T) {
	t.Parallel()
	const count = 10_000
	var b strings.Builder
	for i := range count {
		fmt.Fprintf(&b, "t%d,o%d,交易,2025-01-02 12:00:00,0.01,0.01,,商品\n", i, i)
	}
	text := fixtureCSV([]string{strings.TrimSuffix(b.String(), "\n")}, count, 0, "100.00", "0.00")
	data := zipped(t, []byte(text))
	seen := int64(0)
	sum, err := parseArchive(t.Context(), bytes.NewReader(data), int64(len(data)), fixtureSeller, fixtureDay, func(bill.Entry) error { seen++; return nil })
	if err != nil || seen != count || sum.PayAmount != count {
		t.Fatalf("sum=%+v seen=%d err=%v", sum, seen, err)
	}
}

func TestCSVQuotedFields(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	if err := w.Write([]string{"a,b", "c\"d", ""}); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	row, err := csvLine(b.String())
	if err != nil || len(row) != 3 || row[0] != "a,b" || row[1] != "c\"d" {
		t.Fatalf("row=%q err=%v", row, err)
	}
}

func TestArchivePreflightRejectsForgedDirectory(t *testing.T) {
	t.Parallel()
	valid := zipped(t, []byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00")))
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"multiple_disks", func(b []byte) []byte { b[len(b)-18] = 1; return b }},
		{"other_directory_disk", func(b []byte) []byte { b[len(b)-16] = 1; return b }},
		{"count_mismatch", func(b []byte) []byte { b[len(b)-12] = 2; b[len(b)-14] = 2; return b }},
		{"oversized_directory", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[len(b)-10:], maxDirectoryBytes+1); return b }},
		{"zip64_offset", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[len(b)-6:], ^uint32(0)); return b }},
		{"zip64_locator", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[len(b)-42:], 0x07064b50); return b }},
		{"trailing_data", func(b []byte) []byte { return append(b, 'x') }},
		{"forged_record_length", func(b []byte) []byte {
			i := bytes.Index(b, []byte("PK\x01\x02"))
			binary.LittleEndian.PutUint16(b[i+28:], 65535)
			return b
		}},
		{"zip64_member", func(b []byte) []byte {
			i := bytes.Index(b, []byte("PK\x01\x02"))
			binary.LittleEndian.PutUint32(b[i+24:], ^uint32(0))
			return b
		}},
		{"member_other_disk", func(b []byte) []byte { i := bytes.Index(b, []byte("PK\x01\x02")); b[i+34] = 1; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.edit(bytes.Clone(valid))
			if err := checkArchive(bytes.NewReader(b), int64(len(b))); !errors.Is(err, ErrInvalidBill) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	// EOCD count alone is insufficient: archive/zip scans all directory records.
	files := make([][]byte, maxArchiveFiles+1)
	b := zipped(t, files...)
	b[len(b)-12], b[len(b)-14] = 1, 1
	if err := checkArchive(bytes.NewReader(b), int64(len(b))); !errors.Is(err, ErrInvalidBill) {
		t.Fatalf("forged count accepted: %v", err)
	}
}

func TestArchiveChineseNamesAndBoundedComment(t *testing.T) {
	t.Parallel()
	nameGBK, err := simplifiedchinese.GBK.NewEncoder().String("业务明细.csv")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"业务明细.csv", nameGBK} {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		if err := w.SetComment(strings.Repeat("c", 65535)); err != nil {
			t.Fatal(err)
		}
		f, err := w.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate, NonUTF8: !strings.Contains(name, "业务")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00"))); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if sum, _, err := parseFixture(t, b.Bytes()); err != nil || sum.Count != 1 {
			t.Fatalf("name=%q sum=%+v err=%v", name, sum, err)
		}
	}
}

func TestExplicitCurrencyCannotOverrideDomesticTemplate(t *testing.T) {
	t.Parallel()
	for _, column := range []string{"币种", "交易币种", "trans_currency"} {
		for _, currency := range []string{"CNY", "USD", ""} {
			text := fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00")
			text = strings.Replace(text, fixtureHeader, fixtureHeader+","+column, 1)
			text = strings.Replace(text, fixturePay, fixturePay+","+currency, 1)
			sum, _, err := parseFixture(t, zipped(t, []byte(text)))
			if currency == "CNY" {
				if err != nil || sum.Count != 1 {
					t.Fatalf("column=%q err=%v", column, err)
				}
			} else if !errors.Is(err, ErrInvalidBill) || sum.Count != 0 {
				t.Fatalf("column=%q currency=%q sum=%+v err=%v", column, currency, sum, err)
			}
		}
	}
}
