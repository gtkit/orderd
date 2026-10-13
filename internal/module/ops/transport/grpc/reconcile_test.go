package grpc

import (
	"database/sql"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
)

func TestDiffProtoRetainsRefundAndVerificationEvidence(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	d := models.ReconcileDiff{ID: 7, BatchID: 0, Kind: models.TxnRefund, RunID: "source-run", OriginalTransactionID: "original", OutRefundNo: "merchant-refund", RefundNo: "local-refund", Currency: "CNY", LocalTxnID: 12, VerifyAttempts: 3, NextVerifyAt: sql.NullTime{Time: now, Valid: true}, VerifyLastError: sql.NullString{String: "pending confirmation", Valid: true}, HandleStatus: models.DiffHandlePending, CreatedAt: now}
	p := diffProto(&d)
	if p.TransactionKind != 2 || p.OriginalTransactionId != "original" || p.OutRefundNo != "merchant-refund" || p.RefundNo != "local-refund" || p.Currency != "CNY" || p.ImportId != "source-run" || p.LocalTransactionId != 12 || p.VerificationAttempts != 3 || p.NextVerificationAt == nil || !p.NextVerificationAt.AsTime().Equal(now) || p.VerificationLastError != "pending confirmation" || p.BatchId != 0 {
		t.Fatalf("lost evidence: %+v", p)
	}
	if p := diffProto(&models.ReconcileDiff{}); p.NextVerificationAt != nil {
		t.Fatal("invented verification time")
	}
}
