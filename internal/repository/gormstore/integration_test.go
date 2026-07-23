//go:build integration

// 集成冒烟测试:对真实 MySQL 验证 gorm 仓储与资金主链事务(需 ORDERD_TEST_MYSQL_DSN)。
// schema 从权威草稿 docs/sql/schema.sql 建立(无迁移工具)。
// 运行: ORDERD_TEST_MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/orderd?parseTime=true' go test -tags=integration ./internal/repository/gormstore/
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

	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/platform/policy"
	"github.com/gtkit/orderd/internal/repository/gormstore"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/ormx"

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
		"reconcile_diffs", "reconcile_batches", "order_events", "payment_transactions",
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
