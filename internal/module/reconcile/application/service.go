// Package application(reconcile)实现渠道账单对账(DESIGN §14):拉账单 → 与本地财务流水比对 →
// 差异(长款/短款/金额不一致)落 reconcile_diffs + 更新批次 + 告警。人工处理入口在 diffs 表。
package application

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

// Deps 是对账服务依赖。
type Deps struct {
	Store       store.Store
	Bill        bill.Source
	Repair      bill.Repairer
	AutoConfirm bool
	Alerter     alert.Alerter
	Clock       func() time.Time
	NewBatchNo  func() string
}

// Service 对账应用服务。
type Service struct{ d Deps }

// New 构造对账服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	return &Service{d: d}
}

// Result 对账结果摘要。
type Result struct {
	BatchNo   string
	Matched   int
	DiffCount int
	Skipped   bool // 已完成、重试未到期或认领已由另一副本持有/接管
}

const (
	algorithmVersion   int16 = 2
	pageSize                 = 100
	comparePages             = 8
	calendarBatchSize        = 31
	executionBatchSize       = 8
	transactionBudget        = 5 * time.Second
	fetchBudget              = 30 * time.Second
	executionBudget          = 45 * time.Second
	leaseTTL                 = time.Minute
	retryDelay               = 5 * time.Minute
)

// Reconcile 对指定账期认领后执行;租约未到期、已完成或认领已被接管时跳过。
func (s *Service) Reconcile(ctx context.Context, appID, channel, payAccountID string, billDate time.Time) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, executionBudget)
	defer cancel()
	token, err := idgen.UUIDv4()
	if err != nil {
		return Result{}, err
	}
	var batch *models.ReconcileBatch
	var claimed bool
	err = s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
		var err error
		batch, err = s.ensureBatch(ctx, r, appID, channel, payAccountID, billDate)
		if err != nil {
			return err
		}
		now := s.d.Clock()
		claimed, err = r.Reconcile().ClaimBatch(ctx, batch.ID, token, now, now.Add(leaseTTL))
		return err
	})
	if err != nil {
		return Result{}, err
	}
	if !claimed {
		return Result{BatchNo: batch.BatchNo, Matched: int(batch.MatchedCount), DiffCount: int(batch.DiffCount), Skipped: true}, nil
	}
	batch.LockedBy = token
	batch.LockedUntil = sql.NullTime{Time: s.d.Clock().Add(leaseTTL), Valid: true}
	result, err := s.reconcileClaimed(ctx, batch)
	if err == nil && !result.Skipped {
		err = s.verifyLocal(ctx, channel, payAccountID)
	}
	return result, err
}

// RecoverAccount 逐步确保历史账期存在,随后至多处理八期;不加载整个账号历史。
// 每轮最多31个日期,事务5秒、单期45秒,故一个账号总预算最多365秒。
func (s *Service) RecoverAccount(ctx context.Context, account models.AppPayAccount, through time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, transactionBudget+executionBatchSize*executionBudget)
	defer cancel()
	if err := s.seedCalendar(ctx, account, through); err != nil {
		return 0, err
	}
	if err := s.seedLocal(ctx, account); err != nil {
		return 0, err
	}
	var resultErr error
	n := 0
	for range executionBatchSize {
		if err := ctx.Err(); err != nil {
			return n, errors.Join(resultErr, err)
		}
		token, err := idgen.UUIDv4()
		if err != nil {
			return n, errors.Join(resultErr, err)
		}
		batchCtx, batchCancel := context.WithTimeout(ctx, executionBudget)
		var batch *models.ReconcileBatch
		err = s.withinTx(batchCtx, func(ctx context.Context, r store.Repos) error {
			var err error
			now := s.d.Clock()
			batch, err = r.Reconcile().ClaimNextBatch(ctx, account.Channel, account.PayAccountID, token, now, now.Add(leaseTTL), algorithmVersion)
			return err
		})
		if err != nil || batch == nil {
			batchCancel()
			resultErr = errors.Join(resultErr, err)
			break
		}
		n++
		_, err = s.reconcileClaimed(batchCtx, batch)
		batchCancel()
		resultErr = errors.Join(resultErr, err)
	}
	resultErr = errors.Join(resultErr, s.verifyLocal(ctx, account.Channel, account.PayAccountID))
	resultErr = errors.Join(resultErr, s.repair(ctx, account.Channel, account.PayAccountID))
	return n, resultErr
}

func (s *Service) seedCalendar(ctx context.Context, account models.AppPayAccount, through time.Time) error {
	through = calendarDate(through, bill.Location)
	return s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
		a, err := r.Reconcile().GetScanAccount(ctx, account.PayAccountID, true)
		if err != nil {
			return err
		}
		if a.Channel != account.Channel || a.CreatedAt.IsZero() {
			return fmt.Errorf("对账账号渠道或创建日期无效: %s", account.PayAccountID)
		}
		start := calendarDate(a.CreatedAt, through.Location())
		if a.ReconcileScanVersion == algorithmVersion && a.ReconcileScannedThrough.Valid {
			date := a.ReconcileScannedThrough.Time
			start = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, bill.Location).AddDate(0, 0, 1)
		}
		if start.After(through) {
			return nil
		}
		end := start.AddDate(0, 0, calendarBatchSize-1)
		if end.Before(through) {
			// 历史页留一个名额给昨日,不能因此把连续游标直接跳到昨日。
			end = end.AddDate(0, 0, -1)
			if _, err := s.ensureBatch(ctx, r, a.AppID, a.Channel, a.PayAccountID, through); err != nil {
				return err
			}
		} else {
			end = through
		}
		for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := s.ensureBatch(ctx, r, a.AppID, a.Channel, a.PayAccountID, day); err != nil {
				return err
			}
		}
		return r.Reconcile().UpdateScanCursor(ctx, a.PayAccountID, end, algorithmVersion)
	})
}

func calendarDate(t time.Time, location *time.Location) time.Time {
	t = t.In(location)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, location)
}

func (s *Service) ensureBatch(ctx context.Context, r store.Repos, appID, channel, accountID string, day time.Time) (*models.ReconcileBatch, error) {
	day = calendarDate(day, bill.Location)
	b, err := r.Reconcile().GetBatchByAccountDate(ctx, channel, accountID, day, algorithmVersion)
	if err != nil || b != nil {
		return b, err
	}
	b = &models.ReconcileBatch{BatchNo: s.d.NewBatchNo(), AppID: appID, Channel: channel,
		PayAccountID: accountID, BillDate: calendarDate(day, bill.Location), Status: models.BatchPending, AlgorithmVersion: algorithmVersion}
	if err := r.Reconcile().CreateBatch(ctx, b); err != nil {
		if !errors.Is(err, store.ErrDuplicate) {
			return nil, err
		}
		// 不能把 batch_no 碰撞误当成该账期已存在,否则会错误推进连续游标。
		existing, getErr := r.Reconcile().GetBatchByAccountDate(ctx, channel, accountID, day, algorithmVersion)
		if getErr != nil {
			return nil, getErr
		}
		if existing == nil {
			return nil, err
		}
		return existing, nil
	}
	return b, nil
}

func (s *Service) withinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	ctx, cancel := context.WithTimeout(ctx, transactionBudget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.d.Store.WithinTx(ctx, fn)
}
