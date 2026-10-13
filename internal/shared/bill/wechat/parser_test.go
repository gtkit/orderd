package wechat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
)

var fixtureDay = time.Date(2026, 10, 11, 0, 0, 0, 0, bill.Location)

func paymentRow() []string {
	return []string{"2026-10-11 00:00:00", "wx-app", "1234567890", "0", "", "wx-pay", "out-pay", "openid", "NATIVE", "SUCCESS", "OTHERS", "CNY", "8.88", "1.12", "0", "0", "0.00", "0.00", "", "", "product\\\"\\ \\n", "", "0.06", "0.6%", "10.00", "0.00", ""}
}
func refundRow() []string {
	r := paymentRow()
	r[0] = "2026-10-11 23:59:59"
	r[9] = "REFUND"
	r[12] = "0.00"
	r[13] = "0.00"
	r[14] = "wx-refund"
	r[15] = "out-refund"
	r[16] = "6.50"
	r[17] = "0.20"
	r[18] = "ORIGINAL"
	r[19] = "PROCESSING"
	r[22] = "-0.04"
	r[24] = "0.00"
	r[25] = "7.00"
	return r
}
func valueLine(values []string) string { return "`" + strings.Join(values, ",`") }
func fixture(rows ...[]string) string {
	lines := []string{strings.Join(detailHeaders, ",")}
	for _, row := range rows {
		lines = append(lines, valueLine(row))
	}
	return strings.Join(lines, "\n") + "\n" + strings.Join(totalHeaders, ",") + "\n`2,`8.88,`6.50,`0.20,`0.02,`10.00,`7.00\n"
}
func testParse(t *testing.T, raw string) (bill.Summary, []bill.Entry, error) {
	t.Helper()
	var entries []bill.Entry
	summary, err := parse(t.Context(), strings.NewReader(raw), "1234567890", fixtureDay, func(e bill.Entry) error { entries = append(entries, e); return nil }, billio.MaxEntries)
	return summary, entries, err
}

func TestParseModernAllPreservesPrincipalAndAcceptance(t *testing.T) {
	t.Parallel()
	raw := fixture(paymentRow(), refundRow())
	for _, ending := range []string{"LF", "CRLF", "BOM", "no-final-newline"} {
		t.Run(ending, func(t *testing.T) {
			input := raw
			switch ending {
			case "CRLF":
				input = strings.ReplaceAll(raw, "\n", "\r\n")
			case "BOM":
				input = "\ufeff" + raw
			case "no-final-newline":
				input = strings.TrimSuffix(raw, "\n")
			}
			s, entries, err := testParse(t, input)
			if err != nil || s.Count != 2 || s.PayAmount != 1000 || s.RefundAmount != 700 || len(entries) != 2 {
				t.Fatalf("summary=%+v entries=%d err=%v", s, len(entries), err)
			}
			pay, refund := entries[0], entries[1]
			if pay.Kind != models.TxnPay || pay.Amount != 1000 || pay.TransactionID != "wx-pay" || pay.OriginalTransactionID != "" || pay.ProviderAppID != "wx-app" {
				t.Fatalf("payment=%+v", pay)
			}
			if refund.Kind != models.TxnRefund || refund.Amount != 700 || refund.TransactionID != "wx-refund" || refund.OriginalTransactionID != "wx-pay" || refund.OutRefundNo != "out-refund" || refund.Status != "REFUND:PROCESSING" || !refund.EventAt.Equal(fixtureDay.Add(24*time.Hour-time.Second)) {
				t.Fatalf("refund=%+v", refund)
			}
			_, offset := pay.EventAt.Zone()
			if offset != 8*60*60 {
				t.Fatalf("offset=%d", offset)
			}
		})
	}
}

func TestParseRejectsMalformedOrIncompleteEvidence(t *testing.T) {
	t.Parallel()
	valid := fixture(paymentRow(), refundRow())
	mutate := func(index int, value string) string {
		row := paymentRow()
		row[index] = value
		return fixture(row, refundRow())
	}
	cases := map[string]string{
		"empty": "", "missing-header": strings.Join(strings.Split(valid, "\n")[1:], "\n"),
		"old-header":            strings.Replace(valid, "订单金额,申请退款金额", "总金额,申请退款金额", 1),
		"duplicate-header":      strings.Replace(valid, "设备号,", "商户号,", 1),
		"missing-totals":        strings.Split(valid, "总交易单数")[0],
		"missing-total-values":  strings.Split(valid, "\n`2,")[0],
		"tail-after-total":      valid + "`unexpected\n",
		"duplicate-totals":      valid + strings.Join(totalHeaders, ",") + "\n",
		"count-mismatch":        strings.Replace(valid, "\n`2,", "\n`3,", 1),
		"count-not-integer":     strings.Replace(valid, "\n`2,", "\n`2.0,", 1),
		"pay-total-mismatch":    strings.Replace(valid, "`0.02,`10.00,`7.00", "`0.02,`9.00,`7.00", 1),
		"refund-total-mismatch": strings.Replace(valid, "`0.02,`10.00,`7.00", "`0.02,`10.00,`6.00", 1),
		"fee-total-mismatch":    strings.Replace(valid, "`0.02,`10.00,`7.00", "`0.01,`10.00,`7.00", 1),
		"negative-principal":    mutate(24, "-10.00"), "zero-principal": mutate(24, "0.00"),
		"exponent": mutate(24, "1e1"), "fractional-cent": mutate(24, "10.001"),
		"overflow":       mutate(24, "92233720368547758.08"),
		"wrong-merchant": mutate(2, "other"), "submerchant": mutate(3, "1234567891"),
		"missing-app": mutate(1, ""), "missing-txn": mutate(5, "0"), "space-id": mutate(6, "bad id"),
		"wrong-currency": mutate(11, "USD"), "unknown-status": mutate(9, "NOTPAY"),
		"wrong-date": mutate(0, "2026-10-12 00:00:00"), "wrong-layout": mutate(0, "2026-10-11T00:00:00+08:00"),
		"invalid-UTF8":          mutate(20, string([]byte{0xff})),
		"unescaped-comma":       mutate(20, "hello,world"),
		"missing-prefix":        strings.Replace(valid, "`wx-pay", "wx-pay", 1),
		"payment-refund-id":     mutate(14, "wx-refund"),
		"payment-refund-status": mutate(19, "SUCCESS"),
		"payment-refund-amount": mutate(25, "1.00"),
		"oversized-line":        mutate(20, strings.Repeat("x", billio.MaxLineBytes)),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, err := testParse(t, input)
			if err == nil || s != (bill.Summary{}) {
				t.Fatalf("accepted incomplete evidence: summary=%+v err=%v", s, err)
			}
		})
	}
}

func TestParseRefundStatusAndScope(t *testing.T) {
	t.Parallel()
	for _, trade := range []string{"REFUND", "REVOKED"} {
		for _, status := range []string{"SUCCESS", "PROCESSING", "FAIL", "CHANGE"} {
			t.Run(trade+"-"+status, func(t *testing.T) {
				r := refundRow()
				r[9] = trade
				r[19] = status
				r[1] = "another-app"
				_, entries, err := testParse(t, fixture(paymentRow(), r))
				if err != nil || entries[1].Status != trade+":"+status || entries[1].ProviderAppID != "another-app" {
					t.Fatalf("entries=%+v err=%v", entries, err)
				}
			})
		}
	}
	for name, change := range map[string]func([]string){
		"unknown-state": func(r []string) { r[19] = "UNKNOWN" }, "missing-refund-id": func(r []string) { r[14] = "0" },
		"missing-refund-key": func(r []string) { r[15] = "" }, "nonzero-order-principal": func(r []string) { r[24] = "1.00" },
		"negative-refund": func(r []string) { r[25] = "-7.00" }, "settlement-exceeds-principal": func(r []string) { r[16] = "7.01" },
	} {
		t.Run(name, func(t *testing.T) {
			r := refundRow()
			change(r)
			if _, _, err := testParse(t, fixture(paymentRow(), r)); err == nil {
				t.Fatal("invalid refund accepted")
			}
		})
	}
}

func TestParseHeadersByNameAndExtraCompatibleColumn(t *testing.T) {
	t.Parallel()
	lines := strings.Split(fixture(paymentRow(), refundRow()), "\n")
	for i := range 3 {
		fields := strings.Split(lines[i], ",")
		fields[0], fields[1] = fields[1], fields[0]
		if i == 0 {
			fields = append(fields, "未来说明")
		} else {
			fields = append(fields, "`unused")
		}
		lines[i] = strings.Join(fields, ",")
	}
	for i := 3; i <= 4; i++ {
		fields := strings.Split(lines[i], ",")
		fields[0], fields[6] = fields[6], fields[0]
		lines[i] = strings.Join(fields, ",")
	}
	if s, _, err := testParse(t, strings.Join(lines, "\n")); err != nil || s.PayAmount != 1000 {
		t.Fatalf("s=%+v err=%v", s, err)
	}
}

func TestParseCancellationEmitterFailureAndEntryLimit(t *testing.T) {
	t.Parallel()
	raw := fixture(paymentRow(), refundRow())
	rejected := errors.New("staging rejected duplicate identity")
	if _, err := parse(t.Context(), strings.NewReader(raw), "1234567890", fixtureDay, func(bill.Entry) error { return rejected }, billio.MaxEntries); !errors.Is(err, rejected) {
		t.Fatalf("callback error lost: %v", err)
	}
	var emitted int
	if _, err := parse(t.Context(), strings.NewReader(raw), "1234567890", fixtureDay, func(bill.Entry) error { emitted++; return nil }, 1); !errors.Is(err, billio.ErrLimit) || emitted != 1 {
		t.Fatalf("emitted=%d err=%v", emitted, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	if _, err := parse(ctx, strings.NewReader(raw), "1234567890", fixtureDay, func(bill.Entry) error { cancel(); return nil }, billio.MaxEntries); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error lost: %v", err)
	}
}

// Repeats a single row without retaining a file or all emitted entries in memory.
type repeatReader struct {
	row       string
	remaining int
	offset    int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.row[r.offset:])
	r.offset += n
	if r.offset == len(r.row) {
		r.offset = 0
		r.remaining--
	}
	return n, nil
}
func TestParseLargeGeneratedStream(t *testing.T) {
	t.Parallel()
	const n = 100_000
	r := paymentRow()
	r[12] = "0.01"
	r[13] = "0.00"
	r[22] = "0.00"
	r[24] = "0.01"
	stream := io.MultiReader(strings.NewReader(strings.Join(detailHeaders, ",")+"\n"), &repeatReader{row: valueLine(r) + "\n", remaining: n}, strings.NewReader(strings.Join(totalHeaders, ",")+fmt.Sprintf("\n`%d,`1000.00,`0.00,`0.00,`0.00,`1000.00,`0.00\n", n)))
	count := 0
	s, err := parse(t.Context(), stream, "1234567890", fixtureDay, func(bill.Entry) error { count++; return nil }, billio.MaxEntries)
	if err != nil || count != n || s.Count != n || s.PayAmount != n {
		t.Fatalf("s=%+v emitted=%d err=%v", s, count, err)
	}
}

func TestParseAggregateOverflow(t *testing.T) {
	t.Parallel()
	row := paymentRow()
	row[12] = "0.00"
	row[13] = "0.00"
	row[22] = "0.00"
	row[24] = "92233720368547758.07"
	if s, _, err := testParse(t, fixture(row, row)); err == nil || s != (bill.Summary{}) {
		t.Fatalf("aggregate overflow accepted: s=%+v err=%v", s, err)
	}
}
