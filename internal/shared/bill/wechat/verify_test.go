package wechat

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
)

func paymentEvidence() bill.Entry {
	return bill.Entry{Kind: models.TxnPay, TransactionID: "wx-pay", OutTradeNo: "out-pay", Amount: 1000, Currency: "CNY", MerchantID: "1234567890", ProviderAppID: "wx-app", Status: "SUCCESS", EventAt: fixtureDay}
}
func refundEvidence() bill.Entry {
	e := paymentEvidence()
	e.Kind = models.TxnRefund
	e.OriginalTransactionID = e.TransactionID
	e.TransactionID = "wx-refund"
	e.OutRefundNo = "out-refund"
	e.Amount = 700
	e.Status = "REFUND:PROCESSING"
	return e
}
func paymentResponse() map[string]any {
	return map[string]any{"appid": "wx-app", "mchid": "1234567890", "out_trade_no": "out-pay", "transaction_id": "wx-pay", "trade_state": "REFUND", "success_time": "2026-10-10T23:59:59+08:00", "amount": map[string]any{"total": int64(1000), "currency": "CNY"}}
}
func refundResponse() map[string]any {
	return map[string]any{"refund_id": "wx-refund", "out_refund_no": "out-refund", "transaction_id": "wx-pay", "out_trade_no": "out-pay", "status": "SUCCESS", "success_time": "2026-10-12T00:05:00+08:00", "amount": map[string]any{"total": int64(1000), "refund": int64(700), "currency": "CNY"}}
}
func encodedResponse(t *testing.T, data map[string]any) string {
	t.Helper()
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestVerifySuccessRequiresSignedCompleteIndependentEvidence(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	for _, kind := range []string{"pay", "refund"} {
		for _, missingTime := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-known-time", true: "-unknown-time"}[missingTime], func(t *testing.T) {
				calls := 0
				var bodies []*trackedBody
				s := f.source(t, func(r *http.Request) (*http.Response, error) {
					calls++
					f.verifyRequest(t, r)
					if r.Header.Get("Accept") != "application/json" {
						t.Fatal("query missing JSON accept")
					}
					var body map[string]any
					switch r.URL.Path {
					case "/v3/pay/transactions/out-trade-no/out-pay":
						if r.URL.Query().Get("mchid") != "1234567890" {
							t.Fatal("query merchant missing")
						}
						body = paymentResponse()
					case "/v3/refund/domestic/refunds/out-refund":
						body = refundResponse()
					default:
						t.Fatalf("unexpected query path %s", r.URL.Path)
					}
					if missingTime {
						delete(body, "success_time")
					}
					response := f.response(t, encodedResponse(t, body), 200)
					bodies = append(bodies, response.Body.(*trackedBody))
					return response, nil
				})
				evidence := paymentEvidence()
				wantCalls := 1
				if kind == "refund" {
					evidence = refundEvidence()
					wantCalls = 2
				}
				success, err := s.Verify(t.Context(), "wechat", "account", evidence)
				if err != nil || !success.Confirmed || success.Amount != evidence.Amount || success.Currency != "CNY" || success.TransactionID != evidence.TransactionID || success.OutTradeNo != evidence.OutTradeNo || calls != wantCalls {
					t.Fatalf("result=%+v calls=%d err=%v", success, calls, err)
				}
				if kind == "refund" && (success.OriginalTransactionID != "wx-pay" || success.OutRefundNo != "out-refund") {
					t.Fatalf("refund identity lost: %+v", success)
				}
				if missingTime {
					if !success.At.IsZero() {
						t.Fatalf("fabricated time: %v", success.At)
					}
				} else {
					at := "2026-10-10T23:59:59+08:00"
					if kind == "refund" {
						at = "2026-10-12T00:05:00+08:00"
					}
					expected, err := time.Parse(time.RFC3339, at)
					if err != nil {
						t.Fatal(err)
					}
					if !success.At.Equal(expected) {
						t.Fatalf("wrong channel time: %v", success.At)
					}
				}
				for _, body := range bodies {
					if !body.closed {
						t.Fatal("query leaked SDK original response body")
					}
				}
			})
		}
	}
}

func TestVerifyRejectsPaymentIdentityCurrencyAndAmountMismatch(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	cases := map[string]func(map[string]any){
		"appid": func(r map[string]any) { r["appid"] = "other-app" }, "mchid": func(r map[string]any) { r["mchid"] = "other-mch" },
		"out-trade-no": func(r map[string]any) { r["out_trade_no"] = "other-pay" }, "txn": func(r map[string]any) { r["transaction_id"] = "other-txn" },
		"missing-appid": func(r map[string]any) { delete(r, "appid") }, "missing-currency": func(r map[string]any) { delete(r["amount"].(map[string]any), "currency") },
		"currency":    func(r map[string]any) { r["amount"].(map[string]any)["currency"] = "USD" },
		"zero-amount": func(r map[string]any) { r["amount"].(map[string]any)["total"] = 0 }, "wrong-amount": func(r map[string]any) { r["amount"].(map[string]any)["total"] = 999 },
		"missing-amount": func(r map[string]any) { delete(r, "amount") }, "missing-state": func(r map[string]any) { delete(r, "trade_state") },
		"invalid-time": func(r map[string]any) { r["success_time"] = "not-time" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s := f.source(t, func(*http.Request) (*http.Response, error) {
				body := paymentResponse()
				change(body)
				return f.response(t, encodedResponse(t, body), 200), nil
			})
			success, err := s.Verify(t.Context(), "wechat", "account", paymentEvidence())
			if err == nil || success.Confirmed {
				t.Fatalf("mismatched payment confirmed: %+v err=%v", success, err)
			}
		})
	}
}

func TestVerifyRefundChecksOriginalOwnershipAndAllFourIDs(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	cases := map[string]struct {
		target string
		change func(map[string]any)
	}{
		"original-app":            {"payment", func(r map[string]any) { r["appid"] = "another-app" }},
		"original-merchant":       {"payment", func(r map[string]any) { r["mchid"] = "another-merchant" }},
		"original-txn":            {"payment", func(r map[string]any) { r["transaction_id"] = "other" }},
		"original-currency":       {"payment", func(r map[string]any) { r["amount"].(map[string]any)["currency"] = "USD" }},
		"original-total":          {"payment", func(r map[string]any) { r["amount"].(map[string]any)["total"] = 1001 }},
		"refund-id":               {"refund", func(r map[string]any) { r["refund_id"] = "other" }},
		"refund-key":              {"refund", func(r map[string]any) { r["out_refund_no"] = "other" }},
		"refund-original-txn":     {"refund", func(r map[string]any) { r["transaction_id"] = "other" }},
		"refund-original-key":     {"refund", func(r map[string]any) { r["out_trade_no"] = "other" }},
		"refund-currency":         {"refund", func(r map[string]any) { r["amount"].(map[string]any)["currency"] = "USD" }},
		"refund-total":            {"refund", func(r map[string]any) { r["amount"].(map[string]any)["total"] = 999 }},
		"refund-principal":        {"refund", func(r map[string]any) { r["amount"].(map[string]any)["refund"] = 699 }},
		"missing-refund-id":       {"refund", func(r map[string]any) { delete(r, "refund_id") }},
		"missing-refund-amount":   {"refund", func(r map[string]any) { delete(r, "amount") }},
		"missing-refund-currency": {"refund", func(r map[string]any) { delete(r["amount"].(map[string]any), "currency") }},
		"missing-refund-status":   {"refund", func(r map[string]any) { delete(r, "status") }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := f.source(t, func(r *http.Request) (*http.Response, error) {
				body := paymentResponse()
				target := "payment"
				if strings.HasPrefix(r.URL.Path, "/v3/refund/") {
					body = refundResponse()
					target = "refund"
				}
				if target == tc.target {
					tc.change(body)
				}
				return f.response(t, encodedResponse(t, body), 200), nil
			})
			success, err := s.Verify(t.Context(), "wechat", "account", refundEvidence())
			if err == nil || success.Confirmed {
				t.Fatalf("mismatched refund confirmed: %+v err=%v", success, err)
			}
		})
	}
}

func TestVerifyNegativeStatesNeverBecomeSuccess(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	for _, state := range []string{"NOTPAY", "CLOSED", "REVOKED", "USERPAYING", "PAYERROR", "UNKNOWN"} {
		t.Run("pay-"+state, func(t *testing.T) {
			s := f.source(t, func(*http.Request) (*http.Response, error) {
				body := paymentResponse()
				body["trade_state"] = state
				return f.response(t, encodedResponse(t, body), 200), nil
			})
			for _, e := range []bill.Entry{paymentEvidence(), refundEvidence()} {
				success, err := s.Verify(t.Context(), "wechat", "account", e)
				if err != nil || success.Confirmed {
					t.Fatalf("negative state=%s result=%+v err=%v", state, success, err)
				}
			}
		})
	}
	for _, state := range []string{"PROCESSING", "CLOSED", "ABNORMAL", "UNKNOWN"} {
		t.Run("refund-"+state, func(t *testing.T) {
			s := f.source(t, func(r *http.Request) (*http.Response, error) {
				body := paymentResponse()
				if strings.HasPrefix(r.URL.Path, "/v3/refund/") {
					body = refundResponse()
					body["status"] = state
				}
				return f.response(t, encodedResponse(t, body), 200), nil
			})
			success, err := s.Verify(t.Context(), "wechat", "account", refundEvidence())
			if err != nil || success.Confirmed {
				t.Fatalf("negative state=%s result=%+v err=%v", state, success, err)
			}
		})
	}
}

func TestVerifyMissingSignaturesAndNotExistRemainUnconfirmed(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	for _, target := range []string{"payment", "refund"} {
		for _, failure := range []string{"unsigned", "not-exist", "transport"} {
			t.Run(target+"-"+failure, func(t *testing.T) {
				s := f.source(t, func(r *http.Request) (*http.Response, error) {
					body := paymentResponse()
					operation := "payment"
					if strings.HasPrefix(r.URL.Path, "/v3/refund/") {
						body = refundResponse()
						operation = "refund"
					}
					response := f.response(t, encodedResponse(t, body), 200)
					if operation == target {
						switch failure {
						case "unsigned":
							response.Header.Del("Wechatpay-Signature")
						case "not-exist":
							response = f.response(t, `{"code":"RESOURCE_NOT_EXISTS","message":"secret"}`, 404)
						case "transport":
							return nil, errors.New("secret URL token")
						}
					}
					return response, nil
				})
				success, err := s.Verify(t.Context(), "wechat", "account", refundEvidence())
				if err == nil || success.Confirmed {
					t.Fatalf("unverified response confirmed=%+v err=%v", success, err)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("sensitive SDK error escaped")
				}
			})
		}
	}
}

func TestVerifyRejectsInvalidScopeWithoutNetwork(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	calls := 0
	s := f.source(t, func(*http.Request) (*http.Response, error) { calls++; return nil, context.Canceled })
	for name, mutate := range map[string]func(*bill.Entry){"appid": func(e *bill.Entry) { e.ProviderAppID = "other" }, "merchant": func(e *bill.Entry) { e.MerchantID = "other" }, "amount": func(e *bill.Entry) { e.Amount = 0 }, "currency": func(e *bill.Entry) { e.Currency = "USD" }, "kind": func(e *bill.Entry) { e.Kind = 0 }, "status": func(e *bill.Entry) { e.Status = "UNKNOWN" }, "identity": func(e *bill.Entry) { e.OutTradeNo = "" }} {
		t.Run(name, func(t *testing.T) {
			e := paymentEvidence()
			mutate(&e)
			success, err := s.Verify(t.Context(), "wechat", "account", e)
			if err == nil || success.Confirmed {
				t.Fatalf("invalid evidence accepted: %+v %v", success, err)
			}
		})
	}
	e := refundEvidence()
	e.Status = "REVOKED:SUCCESS"
	if result, err := s.Verify(t.Context(), "wechat", "account", e); err != nil || result.Confirmed {
		t.Fatalf("revoked evidence confirmed: %+v %v", result, err)
	}
	if calls != 0 {
		t.Fatalf("invalid evidence used network %d times", calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Verify(ctx, "wechat", "account", paymentEvidence()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
}
