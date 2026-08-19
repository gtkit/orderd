//go:build integration

// 集成冒烟测试:对真实 MySQL 验证 gorm 仓储与资金主链事务(需 ORDERD_TEST_MYSQL_DSN)。
// schema 从权威草稿 docs/sql/schema.sql 建立(无迁移工具)。
// 运行: ORDERD_TEST_MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/orderd?parseTime=true&interpolateParams=true' go test -tags=integration ./internal/repository/gormstore/
//
// interpolateParams=true:老版本 MySQL(实测 8.0.22)在 prepared(二进制)协议下,字符串参数与
// ascii_bin 列比较会顺序依赖地报 Error 1267(Illegal mix of collations)——同一 SQL 同一数据
// 时过时炸;文本协议的字面量可被正常 coerce,稳定通过。这是服务器侧行为,较新 8.0.x 已不复现;
// 生产部署若用旧版本 server,DSN 需带同样参数或升级 server。
package gormstore_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"io"
	"net/http/httptest"

	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/platform/metrics"
	"github.com/gtkit/orderd/internal/platform/policy"
	"github.com/gtkit/orderd/internal/repository/gormstore"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/ormx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "github.com/go-sql-driver/mysql"
)

var bg = context.Background()

func openTestStore(t *testing.T) *gormstore.Store {
	t.Helper()
	dsn := os.Getenv("ORDERD_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 ORDERD_TEST_MYSQL_DSN,跳过集成测试")
	}
	applySchema(t, dsn)
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("打开测试库失败: %v", err)
	}
	cli, err := ormx.OpenWithDB(bg, sqlDB, ormx.WithTranslateError(true))
	if err != nil {
		t.Fatalf("ormx 包装失败: %v", err)
	}
	return gormstore.New(cli)
}

// applySchema 重建 schema:drop(逆序)后整体执行 docs/sql/schema.sql,再播种一个支付账号。
// 用 multiStatements 连接一次性执行整份脚本——由 MySQL 自身解析,避免注释/字符串内分号被误切分。
func applySchema(t *testing.T, dsn string) {
	t.Helper()
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("mysql", dsn+sep+"multiStatements=true")
	if err != nil {
		t.Fatalf("打开建库连接: %v", err)
	}
	defer func() { _ = db.Close() }()

	tables := []string{
		"app_amount_policies", "ops_audit_logs", "reconcile_diffs", "reconcile_batches", "order_events", "payment_transactions",
		"app_pay_accounts", "payment_attempts", "refunds", "app_endpoints", "event_outbox", "order_logs", "orders",
	}
	for _, tb := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tb); err != nil {
			t.Fatalf("drop %s: %v", tb, err)
		}
	}
	raw, err := os.ReadFile("../../../docs/sql/schema.sql")
	if err != nil {
		t.Fatalf("读 schema.sql: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("执行 schema.sql 失败: %v", err)
	}
	_, err = db.Exec(`INSERT INTO app_pay_accounts(pay_account_id, app_id, channel, mch_id, pay_app_id, notify_route_id, config_ref, config_version, enabled)
		VALUES('acc-wechat','app1','wechat','mch-1','payapp-1','route-1','ref-1',1,1)`)
	if err != nil {
		t.Fatalf("播种账号: %v", err)
	}
}

// --- 仓储直连:活跃尝试唯一守卫(生成列 active_order,含 CLOSING)---

func TestGormOrderAndAttemptRepo(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()
	r := st.Reader()

	o := &models.Order{
		AppID: "app1", OrderNo: "OD-int-1", OrderToken: "tok-int-1", UserID: 42, Status: models.OrderPending,
		ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 100, DiscountAmount: 10, PayAmount: 90,
		PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
		IdempotencyKey: models.NormalizeKey("idem-int-1"), RequestHash: models.NormalizeKey("hash-1"),
		ExpireAt: time.Now().Add(time.Hour),
	}
	if err := r.Orders().Insert(bg, o); err != nil {
		t.Fatalf("插入订单: %v", err)
	}
	if o.ID == 0 {
		t.Fatal("插入后应回填 ID")
	}
	got, err := r.Orders().GetByOrderNo(bg, "app1", "OD-int-1", false)
	if err != nil || got.PayAmount != 90 || got.Status != models.OrderPending {
		t.Fatalf("查订单: got=%+v err=%v", got, err)
	}
	byIdem, err := r.Orders().FindByIdempotencyKey(bg, "app1", "idem-int-1")
	if err != nil || byIdem == nil {
		t.Fatalf("按幂等键查: %v", err)
	}

	a1 := &models.PaymentAttempt{
		AppID: "app1", OrderNo: "OD-int-1", AttemptNo: 1, Channel: "wechat",
		PayAccountID: "acc-wechat", OutTradeNo: "PT-int-1", Status: models.AttemptClosing, ExpireAt: time.Now().Add(time.Hour),
	}
	if err := r.Attempts().Insert(bg, a1); err != nil {
		t.Fatalf("插入尝试1: %v", err)
	}
	a2 := &models.PaymentAttempt{
		AppID: "app1", OrderNo: "OD-int-1", AttemptNo: 2, Channel: "wechat",
		PayAccountID: "acc-wechat", OutTradeNo: "PT-int-2", Status: models.AttemptCreated, ExpireAt: time.Now().Add(time.Hour),
	}
	if err := r.Attempts().Insert(bg, a2); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("CLOSING 占活跃位,第二个活跃尝试应被唯一约束拒绝(gorm.ErrDuplicatedKey),实际: %v", err)
	}
}

// --- DB 专有机制:退款 active_guard 生成列唯一 + outbox FOR UPDATE SKIP LOCKED 认领 ---

func TestGormRefundGuardAndOutboxClaim(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()
	r := st.Reader()

	// 活跃退款唯一守卫(active_guard 生成列:status∈{0,1,5} 时=order_no)。
	rf1 := &models.Refund{
		AppID: "app1", RefundNo: "RF-1", OutRefundNo: "OR-1", RequestHash: "h1",
		OrderNo: "OD-rf-1", PayAccountID: "acc-wechat", RefundAmount: 90, Status: models.RefundProcessing,
	}
	if err := r.Refunds().Insert(bg, rf1); err != nil {
		t.Fatalf("插入退款1: %v", err)
	}
	rf2 := &models.Refund{
		AppID: "app1", RefundNo: "RF-2", OutRefundNo: "OR-2", RequestHash: "h2",
		OrderNo: "OD-rf-1", PayAccountID: "acc-wechat", RefundAmount: 90, Status: models.RefundCreated,
	}
	if err := r.Refunds().Insert(bg, rf2); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("同订单第二笔活跃退款应被 active_guard 拒绝,实际: %v", err)
	}

	// outbox 认领:插一条到期待投递行,在事务内 ClaimDue 应认领到并打租约。
	if err := r.Events().InsertOutbox(bg, &models.EventOutbox{
		EventID: "evt-ob-1", AppID: "app1", OrderNo: "OD-rf-1", EventType: models.EventTypePaid,
		AggregateVersion: 1, Payload: []byte("x"), Status: models.OutboxPending,
		NextAttemptAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("插入 outbox: %v", err)
	}
	var claimed []*models.EventOutbox
	err := st.WithinTx(bg, func(ctx context.Context, rp store.Repos) error {
		var e error
		claimed, e = rp.Outbox().ClaimDue(ctx, "w1", time.Now(), time.Now().Add(time.Minute), 10)
		return e
	})
	if err != nil {
		t.Fatalf("ClaimDue 事务: %v", err)
	}
	if len(claimed) != 1 || claimed[0].EventID != "evt-ob-1" || claimed[0].LockedBy != "w1" {
		t.Fatalf("应认领到 1 条并打租约 w1: %+v", claimed)
	}
}

// --- 服务层:CreateOrder → ConfirmPaySuccess 全事务链(真实 gorm 事务 + FOR UPDATE)---

type itGateway struct{}

func (itGateway) Prepare(context.Context, paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	return paygateway.PrepareResult{PrepayID: "pp", AppParams: "params"}, nil
}

func (itGateway) Query(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error) {
	return paygateway.QueryResult{}, nil
}
func (itGateway) Close(context.Context, paygateway.CloseInput) error { return nil }
func (itGateway) Refund(context.Context, paygateway.RefundInput) (paygateway.RefundResult, error) {
	return paygateway.RefundResult{Accepted: true}, nil
}

func (itGateway) RefundQuery(context.Context, paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded}, nil
}

type itRouter struct{}

func (itRouter) Resolve(string, string) (paygateway.Gateway, error) { return itGateway{}, nil }

func TestGormCreateAndConfirmPay(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()

	accounts := gormstore.NewAccountResolver(st.DB())
	var n int64
	newEID := func() (string, error) { n++; return "evt-int-" + strconv.FormatInt(n, 10), nil }
	pay := paymentapp.New(paymentapp.Deps{
		Store: st, Router: itRouter{}, Accounts: accounts, Alerter: alert.Nop{}, Clock: time.Now,
		NewEventID: newEID, OutTradeNo: func() string { n++; return "PT-svc-" + strconv.FormatInt(n, 10) },
	})
	ord := orderapp.New(orderapp.Deps{
		Store: st, Payment: pay, Pricing: policy.TrustedCallerPricing{}, Amounts: policy.DefaultAmountPolicy(),
		Accounts: accounts, Clock: time.Now, NewOrderNo: func() string { n++; return "OD-svc-" + strconv.FormatInt(n, 10) },
		NewToken: func() (string, error) { n++; return "tok-svc-" + strconv.FormatInt(n, 10), nil }, NewEventID: newEID,
	})

	out, err := ord.CreateOrder(bg, orderapp.CreateInput{
		AppID: "app1", UserID: 42, ProductID: 7, ProductTitle: "会员", BusinessType: "vip",
		Amount:    models.Money{Original: 100, Discount: 10, Payable: 90, Currency: "CNY"},
		PayMethod: models.PayMethodWechat, IdempotencyKey: "idem-svc-1",
	})
	if err != nil {
		t.Fatalf("下单: %v", err)
	}
	atts, err := st.Reader().Attempts().ListByOrder(bg, out.OrderNo)
	if err != nil || len(atts) != 1 || atts[0].Status != models.AttemptReady {
		t.Fatalf("应有 1 个 READY 尝试: %+v err=%v", atts, err)
	}
	res, err := pay.ConfirmPaySuccess(bg, paymentapp.ConfirmInput{
		AppID: "app1", OrderNo: out.OrderNo, OutTradeNo: atts[0].OutTradeNo, Channel: "wechat",
		PayAccountID: "acc-wechat", TransactionID: "txn-int-1", Amount: 90, Currency: "CNY", PaidAt: time.Now(),
	})
	if err != nil || res.Outcome != "paid" {
		t.Fatalf("确认支付: res=%+v err=%v", res, err)
	}
	o, err := ord.GetOrderStatus(bg, "app1", out.OrderToken, 42)
	if err != nil || o.Status != models.OrderPaid || o.SettledPayAccountID != "acc-wechat" {
		t.Fatalf("支付后订单: o=%+v err=%v", o, err)
	}
	evs, err := st.Reader().Events().ListByApp(bg, "app1", 0, 10)
	if err != nil || len(evs) != 1 || evs[0].EventType != models.EventTypePaid {
		t.Fatalf("事件历史: %+v err=%v", evs, err)
	}
}

// TestIntegrationOpsDisposition 真库冒烟(评审 10.15):游标分页、FOR UPDATE 处置 CAS、
// SKIP LOCKED 兜底认领、审计插入/查询、anomaly_kind 列读写。
func TestIntegrationOpsDisposition(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()

	now := time.Now().Truncate(time.Second)
	// 播种:1 个批次 + 3 条差异、2 条死信(1 条满足兜底条件)、2 条流水(1 条 anomaly 带成因)。
	err := st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		b := &models.ReconcileBatch{BatchNo: "RC-i1", AppID: "app1", Channel: "wechat", PayAccountID: "acc-w", BillDate: now.AddDate(0, 0, -1)}
		if e := r.Reconcile().CreateBatch(ctx, b); e != nil {
			return e
		}
		for i := range 3 {
			if e := r.Reconcile().InsertDiff(ctx, &models.ReconcileDiff{
				BatchID: b.ID, AppID: "app1", Channel: "wechat", PayAccountID: "acc-w",
				DiffType: models.DiffShort, TransactionID: "itx-" + strconv.Itoa(i), LocalAmount: 90,
			}); e != nil {
				return e
			}
		}
		for i, attempts := range []int32{8, 20} { // 第二条超次数上限
			if e := r.Events().InsertOutbox(ctx, &models.EventOutbox{
				EventID: "ievt-" + strconv.Itoa(i), AppID: "app1", OrderNo: "OD-i", EventType: models.EventTypePaid,
				Payload: []byte("p"), Status: models.OutboxDead, Attempts: attempts, NextAttemptAt: now,
			}); e != nil {
				return e
			}
		}
		if e := r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: "app1", OrderNo: "OD-i", PayAccountID: "acc-w", Channel: "wechat", Kind: models.TxnPay,
			TransactionID: "itx-normal", Amount: 90, Currency: "CNY",
		}); e != nil {
			return e
		}
		return r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: "app1", OrderNo: "OD-i", PayAccountID: "acc-w", Channel: "wechat", Kind: models.TxnPay,
			TransactionID: "itx-anomaly", Amount: 90, Currency: "CNY",
			Anomaly: true, AnomalyKind: models.AnomalyKindSecondRealPayment,
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}
	// MySQL 侧把死信 updated_at 回拨以满足冷却(gorm 更新会刷新 updated_at,直接 SQL 回拨)。
	if err := st.DB().Exec("UPDATE event_outbox SET updated_at = ? WHERE status = 2", now.Add(-2*time.Hour)).Error; err != nil {
		t.Fatalf("回拨 updated_at: %v", err)
	}

	// 差异:游标分页(limit 2 → next=第2条 id)+ 过滤。
	page1, err := st.Reader().Reconcile().ListDiffs(bg, store.DiffFilter{AppID: "app1"}, 0, 2)
	if err != nil || len(page1) != 2 {
		t.Fatalf("差异首页: n=%d err=%v", len(page1), err)
	}
	page2, err := st.Reader().Reconcile().ListDiffs(bg, store.DiffFilter{AppID: "app1"}, page1[1].ID, 2)
	if err != nil || len(page2) != 1 || page2[0].ID <= page1[1].ID {
		t.Fatalf("差异翻页: n=%d err=%v", len(page2), err)
	}
	// 处置:FOR UPDATE + 写处置字段 + 审计同事务。
	diffID := page1[0].ID
	err = st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		d, e := r.Reconcile().GetDiff(ctx, diffID, true)
		if e != nil {
			return e
		}
		if !models.CanDiffHandleTransit(d.HandleStatus, models.DiffHandleResolved) {
			t.Fatalf("初始应可处置: %v", d.HandleStatus)
		}
		if e := r.Reconcile().UpdateDiffHandling(ctx, diffID, models.DiffHandleResolved, "ops:1", now, "已核对"); e != nil {
			return e
		}
		return r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
			Actor: "ops:1", Action: models.OpsActionReconcileDiffHandle,
			TargetType: models.OpsTargetReconcileDiff, TargetID: strconv.FormatUint(diffID, 10), Detail: "0→2",
		})
	})
	if err != nil {
		t.Fatalf("处置失败: %v", err)
	}
	got, _ := st.Reader().Reconcile().GetDiff(bg, diffID, false)
	if got.HandleStatus != models.DiffHandleResolved || got.HandledBy != "ops:1" || !got.HandledAt.Valid || got.HandleRemark != "已核对" {
		t.Fatalf("处置字段不符: %+v", got)
	}
	auds, err := st.Reader().OpsAudit().ListByTarget(bg, models.OpsTargetReconcileDiff, strconv.FormatUint(diffID, 10), 0, 10)
	if err != nil || len(auds) != 1 {
		t.Fatalf("审计查询: n=%d err=%v", len(auds), err)
	}

	// 死信:清单 + GetByEventID + 兜底认领(SKIP LOCKED;只应命中 attempts=8 那条)。
	dead, err := st.Reader().Outbox().ListDead(bg, store.DeadFilter{AppID: "app1"}, 0, 10)
	if err != nil || len(dead) != 2 {
		t.Fatalf("死信清单: n=%d err=%v", len(dead), err)
	}
	if _, err := st.Reader().Outbox().GetByEventID(bg, "ievt-0", false); err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	err = st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		batch, e := r.Outbox().ClaimDeadForRequeue(ctx, store.DeadRequeueCond{
			UpdatedBefore: now.Add(-time.Hour), AttemptsBelow: 11, CreatedAfter: now.Add(-7 * 24 * time.Hour),
		}, 10)
		if e != nil {
			return e
		}
		if len(batch) != 1 || batch[0].EventID != "ievt-0" {
			t.Fatalf("兜底认领应只命中 ievt-0: %+v", batch)
		}
		return r.Outbox().Requeue(ctx, batch[0].ID, now)
	})
	if err != nil {
		t.Fatalf("兜底认领/重放失败: %v", err)
	}
	ob, _ := st.Reader().Outbox().GetByEventID(bg, "ievt-0", false)
	if ob.Status != models.OutboxPending || ob.Attempts != 8 {
		t.Fatalf("重放后应 Pending 且 attempts 不重置: %+v", ob)
	}

	// anomaly 流水:只返回 anomaly=1 且成因可读。
	anoms, err := st.Reader().Transactions().ListAnomalies(bg, store.AnomalyFilter{AppID: "app1"}, 0, 10)
	if err != nil || len(anoms) != 1 || anoms[0].AnomalyKind != models.AnomalyKindSecondRealPayment {
		t.Fatalf("anomaly 清单: %+v err=%v", anoms, err)
	}
}

// TestIntegrationBacklogMetrics 真库断言积压指标(backlog-metrics spec):
// 空库为零值;播种各态数据后指标与库内状态一致。
func TestIntegrationBacklogMetrics(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()

	scrape := func() string {
		reg := prometheus.NewRegistry()
		reg.MustRegister(metrics.NewBacklogCollector(st.DB()))
		srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		defer srv.Close()
		resp, err := srv.Client().Get(srv.URL)
		if err != nil {
			t.Fatalf("scrape: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return string(b)
	}
	mustHave := func(body, line string) {
		t.Helper()
		if !strings.Contains(body, line) {
			t.Fatalf("指标缺失或值不符,期望含 %q", line)
		}
	}

	// 空库:全部零值。
	body := scrape()
	mustHave(body, `orderd_outbox_backlog{status="pending"} 0`)
	mustHave(body, `orderd_outbox_backlog{status="dead"} 0`)
	mustHave(body, "orderd_outbox_oldest_pending_age_seconds 0")
	mustHave(body, `orderd_attempts_converging{status="unknown"} 0`)
	mustHave(body, "orderd_anomaly_txns_total 0")
	mustHave(body, "orderd_metrics_scrape_errors_total 0")

	// 播种:1 pending(应投时间 10 分钟前)+ 2 dead outbox、1 UNKNOWN 尝试、1 PROCESSING 退款、
	// 1 过期 PENDING 订单、1 anomaly 流水。
	now := time.Now().Truncate(time.Second)
	err := st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: "OD-m1", OrderToken: "tok-m1", UserID: 42, Status: models.OrderPending,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, ExpireAt: now.Add(-time.Minute),
		}); e != nil {
			return e
		}
		if e := r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: "OD-m1", AttemptNo: 1, Channel: "wechat", PayAccountID: "acc-wechat",
			OutTradeNo: "PT-m1", Status: models.AttemptUnknown, ExpireAt: now.Add(time.Hour),
		}); e != nil {
			return e
		}
		if e := r.Refunds().Insert(ctx, &models.Refund{
			AppID: "app1", RefundNo: "RF-m1", OutRefundNo: "or-m1", OrderNo: "OD-m1",
			PayAccountID: "acc-wechat", RefundAmount: 90, Status: models.RefundProcessing,
		}); e != nil {
			return e
		}
		if e := r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: "app1", OrderNo: "OD-m1", PayAccountID: "acc-wechat", Channel: "wechat",
			Kind: models.TxnPay, TransactionID: "tx-m1", Amount: 90, Currency: "CNY",
			Anomaly: true, AnomalyKind: models.AnomalyKindAmountMismatch,
		}); e != nil {
			return e
		}
		for i, stx := range []models.OutboxStatus{models.OutboxPending, models.OutboxDead, models.OutboxDead} {
			if e := r.Events().InsertOutbox(ctx, &models.EventOutbox{
				EventID: "evt-m" + strconv.Itoa(i), AppID: "app1", OrderNo: "OD-m1", EventType: models.EventTypePaid,
				Payload: []byte("p"), Status: stx, NextAttemptAt: now.Add(-10 * time.Minute),
			}); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}

	body = scrape()
	mustHave(body, `orderd_outbox_backlog{status="pending"} 1`)
	mustHave(body, `orderd_outbox_backlog{status="dead"} 2`)
	mustHave(body, `orderd_attempts_converging{status="unknown"} 1`)
	mustHave(body, `orderd_refunds_in_flight{status="processing"} 1`)
	mustHave(body, "orderd_orders_expired_pending 1")
	mustHave(body, "orderd_anomaly_txns_total 1")
	// 最老待投递年龄 ≈ 600s(容忍执行耗时偏差)。
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "orderd_outbox_oldest_pending_age_seconds ") {
			v, perr := strconv.ParseFloat(strings.Fields(l)[1], 64)
			if perr != nil || v < 590 || v > 660 {
				t.Fatalf("最老待投递年龄应约 600s: %q", l)
			}
		}
	}
}

// TestIntegrationAmountPolicy 真库(app-amount-policy spec):配置行生效、enabled=0 与未配置
// 回退(resolver 返回 miss)、白名单 split 语义(币种空=仅 CNY 由 AmountBounds 层保证)。
func TestIntegrationAmountPolicy(t *testing.T) {
	st := openTestStore(t)
	defer func() { _ = st.Close() }()
	if err := st.DB().Exec(`INSERT INTO app_amount_policies(app_id, min_payable, max_payable, allowed_business, allowed_currency, enabled)
		VALUES('app1', 100, 0, 'membership, course', '', 1), ('app2', 1, 0, '', '', 0)`).Error; err != nil {
		t.Fatalf("播种策略失败: %v", err)
	}
	r := gormstore.NewAmountPolicyResolver(st.DB())

	b, ok, err := r.Bounds(bg, "app1")
	if err != nil || !ok || b.MinPayable != 100 {
		t.Fatalf("已配置 app 应命中: %+v ok=%v err=%v", b, ok, err)
	}
	if !b.AllowedBusiness["membership"] || !b.AllowedBusiness["course"] || b.AllowedBusiness[""] {
		t.Fatalf("业务白名单 split 语义不符: %+v", b.AllowedBusiness)
	}
	if b.AllowedCurrency != nil {
		t.Fatalf("币种空应为 nil(=仅 CNY): %+v", b.AllowedCurrency)
	}
	// min_payable 生效(经 ValidateAmount)。
	if err := models.ValidateAmount(bg, r, "app1", models.Money{Original: 90, Payable: 90, Currency: "CNY"}, "membership"); err == nil {
		t.Fatal("低于 per-app 下限应被拒")
	}
	// enabled=0 与未配置 → miss(由装配层回退链兜底)。
	if _, ok, err := r.Bounds(bg, "app2"); err != nil || ok {
		t.Fatalf("enabled=0 应 miss: ok=%v err=%v", ok, err)
	}
	if _, ok, err := r.Bounds(bg, "app-none"); err != nil || ok {
		t.Fatalf("未配置应 miss: ok=%v err=%v", ok, err)
	}
}
