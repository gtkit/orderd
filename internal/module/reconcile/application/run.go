package application

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

var errLeaseLost = errors.New("reconcile lease no longer current")

func identity(kind models.TxnKind, txn, outTrade, outRefund string) string {
	if kind == models.TxnRefund {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("refund:%d:%s:%s", len(outTrade), outTrade, outRefund))))
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte("pay:"+txn)))
}
func addAmount(total *int64, amount int64) error {
	if amount < 0 || *total > math.MaxInt64-amount {
		return fmt.Errorf("reconcile amount overflow")
	}
	*total += amount
	return nil
}
func toEntry(e models.ReconcileEntry) bill.Entry {
	return bill.Entry{Kind: e.Kind, TransactionID: e.TransactionID, OriginalTransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, OutRefundNo: e.OutRefundNo, Amount: e.Amount, Currency: e.Currency, EventAt: e.EventAt.Time, MerchantID: e.MerchantID, ProviderAppID: e.ProviderAppID, Status: e.SourceStatus}
}
func stageEntry(e bill.Entry, b *models.ReconcileBatch) (models.ReconcileEntry, error) {
	if (e.Kind != models.TxnPay && e.Kind != models.TxnRefund) || e.Amount <= 0 || e.Currency != "CNY" || e.OutTradeNo == "" || (e.Kind == models.TxnPay && e.TransactionID == "") || (e.Kind == models.TxnRefund && (e.OutRefundNo == "" || e.OriginalTransactionID == "")) {
		return models.ReconcileEntry{}, fmt.Errorf("invalid normalized bill identity or amount")
	}
	return models.ReconcileEntry{BatchID: b.ID, RunID: b.SourceRunID, Kind: e.Kind, IdentityKey: identity(e.Kind, e.TransactionID, e.OutTradeNo, e.OutRefundNo), Channel: b.Channel, PayAccountID: b.PayAccountID, TransactionID: e.TransactionID, OriginalTransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, OutRefundNo: e.OutRefundNo, Amount: e.Amount, Currency: e.Currency, EventAt: sql.NullTime{Time: e.EventAt, Valid: !e.EventAt.IsZero()}, MerchantID: e.MerchantID, ProviderAppID: e.ProviderAppID, SourceStatus: e.Status}, nil
}
func (s *Service) saveRun(ctx context.Context, b *models.ReconcileBatch, token, expectedRun string, entries []models.ReconcileEntry, diffs []models.ReconcileDiff, publish bool, matched ...uint64) error {
	previousPublished := b.PublishedRunID
	return s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
		ok, err := r.Reconcile().SaveRun(ctx, b, token, expectedRun, s.d.Clock(), entries, diffs, publish)
		if err != nil {
			return err
		}
		if !ok {
			return errLeaseLost
		}
		if len(matched) > 0 && previousPublished != "" {
			return r.Reconcile().ResolveMatchedDiffs(ctx, b.ID, previousPublished, matched, s.d.Clock())
		}
		return nil
	})
}
func (s *Service) reconcileClaimed(ctx context.Context, b *models.ReconcileBatch) (Result, error) {
	token := b.LockedBy
	if b.ImportComplete && b.RunID == b.PublishedRunID && b.DirtySeq > b.PublishedDirtySeq {
		previous := b.RunID
		run, err := idgen.UUIDv4()
		if err != nil {
			return Result{}, err
		}
		b.RunID = run
		b.RunDirtySeq = b.DirtySeq
		b.CompareAfterID = 0
		b.MatchedCount = 0
		b.DiffCount = 0
		b.LocalAmount = 0
		b.LocalRefundAmount = 0
		if err := s.saveRun(ctx, b, token, previous, nil, nil, false); err != nil {
			if errors.Is(err, errLeaseLost) {
				return Result{BatchNo: b.BatchNo, Skipped: true}, nil
			}
			return Result{}, err
		}
	}
	if !b.ImportComplete {
		if err := s.importBill(ctx, b, token); err != nil {
			if errors.Is(err, errLeaseLost) {
				return Result{BatchNo: b.BatchNo, Skipped: true}, nil
			}
			return Result{}, errors.Join(err, s.failBatch(ctx, b, token, "bill import failed"))
		}
	}
	for range comparePages {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		entries, err := s.d.Store.Reader().Reconcile().ListEntries(ctx, b.ID, b.SourceRunID, b.CompareAfterID, pageSize)
		if err != nil {
			return Result{}, err
		}
		diffs := make([]models.ReconcileDiff, 0, len(entries))
		matchedIDs := make([]uint64, 0, len(entries))
		for _, e := range entries {
			d, local, err := s.compareEntry(ctx, b, e)
			if err != nil {
				return Result{}, err
			}
			if local != nil {
				target := &b.LocalAmount
				if e.Kind == models.TxnRefund {
					target = &b.LocalRefundAmount
				}
				if err := addAmount(target, local.Amount); err != nil {
					return Result{}, err
				}
			}
			if d == nil {
				b.MatchedCount++
				matchedIDs = append(matchedIDs, e.ID)
			} else {
				diffs = append(diffs, *d)
				b.DiffCount++
			}
			b.CompareAfterID = e.ID
		}
		finished := len(entries) < pageSize
		if finished {
			b.Status = models.BatchDone
			b.ReconciledAt = sqlutil.Time(s.d.Clock())
			b.LockedBy = ""
			b.LockedUntil = sql.NullTime{}
		} else {
			b.Status = models.BatchPulled
			b.LockedUntil = sqlutil.Time(s.d.Clock().Add(leaseTTL))
		}
		if err := s.saveRun(ctx, b, token, b.RunID, nil, diffs, finished, matchedIDs...); err != nil {
			if errors.Is(err, errLeaseLost) {
				return Result{BatchNo: b.BatchNo, Skipped: true}, nil
			}
			return Result{}, err
		}
		if finished {
			if b.DiffCount > 0 && s.d.Alerter != nil {
				s.d.Alerter.Anomaly(ctx, "reconcile_diff", map[string]any{"batch_no": b.BatchNo, "channel": b.Channel, "pay_account_id": b.PayAccountID, "bill_date": b.BillDate.Format("2006-01-02"), "diff_count": b.DiffCount})
			}
			return Result{BatchNo: b.BatchNo, Matched: int(b.MatchedCount), DiffCount: int(b.DiffCount)}, nil
		}
	}
	b.LockedBy = ""
	b.LockedUntil = sql.NullTime{}
	if err := s.saveRun(ctx, b, token, b.RunID, nil, nil, false); err != nil {
		if errors.Is(err, errLeaseLost) {
			return Result{BatchNo: b.BatchNo, Skipped: true}, nil
		}
		return Result{}, err
	}
	return Result{BatchNo: b.BatchNo, Matched: int(b.MatchedCount), DiffCount: int(b.DiffCount)}, nil
}
func (s *Service) importBill(ctx context.Context, b *models.ReconcileBatch, token string) error {
	run, err := idgen.UUIDv4()
	if err != nil {
		return err
	}
	previous := b.RunID
	b.RunID = run
	b.SourceRunID = run
	b.RunDirtySeq = b.DirtySeq
	b.ImportComplete = false
	b.CompareAfterID = 0
	b.SourceCount = 0
	b.MatchedCount = 0
	b.DiffCount = 0
	b.SourceAmount = 0
	b.SourceRefundAmount = 0
	b.LocalAmount = 0
	b.LocalRefundAmount = 0
	b.LastError = sql.NullString{}
	if err := s.saveRun(ctx, b, token, previous, nil, nil, false); err != nil {
		return err
	}
	fetchCtx, cancel := context.WithTimeout(ctx, fetchBudget)
	defer cancel()
	page := make([]models.ReconcileEntry, 0, pageSize)
	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		b.LockedUntil = sqlutil.Time(s.d.Clock().Add(leaseTTL))
		if err := s.saveRun(fetchCtx, b, token, b.RunID, page, nil, false); err != nil {
			return err
		}
		page = page[:0]
		return nil
	}
	summary, err := s.d.Bill.Fetch(fetchCtx, b.Channel, b.PayAccountID, b.BillDate, func(e bill.Entry) error {
		if err := fetchCtx.Err(); err != nil {
			return err
		}
		row, err := stageEntry(e, b)
		if err != nil {
			return err
		}
		if b.SourceCount >= 1_000_000 {
			return fmt.Errorf("bill exceeds one million rows")
		}
		b.SourceCount++
		amount := &b.SourceAmount
		if e.Kind == models.TxnRefund {
			amount = &b.SourceRefundAmount
		}
		if err := addAmount(amount, e.Amount); err != nil {
			return err
		}
		page = append(page, row)
		if len(page) == pageSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := fetchCtx.Err(); err != nil {
		return err
	}
	if summary.Count != int64(b.SourceCount) || summary.PayAmount != b.SourceAmount || summary.RefundAmount != b.SourceRefundAmount || summary.NoStatement {
		return fmt.Errorf("bill summary mismatch")
	}
	if err := flush(); err != nil {
		return err
	}
	b.SourceDigestAlgorithm = summary.DigestAlgorithm
	b.SourceDigest = summary.Digest
	b.ImportComplete = true
	b.Status = models.BatchPulled
	b.PulledAt = sqlutil.Time(s.d.Clock())
	b.LockedUntil = sqlutil.Time(s.d.Clock().Add(leaseTTL))
	return s.saveRun(ctx, b, token, b.RunID, nil, nil, false)
}
func (s *Service) failBatch(ctx context.Context, b *models.ReconcileBatch, token, reason string) error {
	b.Status = models.BatchFailed
	b.LastError = sqlutil.Str(reason)
	b.LockedBy = ""
	b.LockedUntil = sqlutil.Time(s.d.Clock().Add(retryDelay))
	err := s.saveRun(ctx, b, token, b.RunID, nil, nil, false)
	if errors.Is(err, errLeaseLost) {
		return nil
	}
	return err
}
