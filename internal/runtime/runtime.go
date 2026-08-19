// Package runtime 负责装配:拿资源(DB)→ 构造仓储/解析器/网关 → 注入应用服务 → 组装传输层。
// 不含业务规则,只做创建 + 适配 + 注入(DESIGN §2)。
package runtime

import (
	"context"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/appmeta"
	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/module/event/worker"
	opsapp "github.com/gtkit/orderd/internal/module/ops/application"
	opsgrpc "github.com/gtkit/orderd/internal/module/ops/transport/grpc"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	ordergrpc "github.com/gtkit/orderd/internal/module/order/transport/grpc"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	httpcb "github.com/gtkit/orderd/internal/module/payment/transport/http"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/paygateway/alipaygw"
	"github.com/gtkit/orderd/internal/paygateway/stub"
	"github.com/gtkit/orderd/internal/paygateway/wechatgw"
	"github.com/gtkit/orderd/internal/pkg/auth"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/pkg/ordernum"
	"github.com/gtkit/orderd/internal/pkg/webhook"
	"github.com/gtkit/orderd/internal/platform/config"
	"github.com/gtkit/orderd/internal/platform/data"
	"github.com/gtkit/orderd/internal/platform/metrics"
	"github.com/gtkit/orderd/internal/platform/policy"
	"github.com/gtkit/orderd/internal/repository/gormstore"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/alert/hook"
	billstub "github.com/gtkit/orderd/internal/shared/bill/stub"
	"github.com/gtkit/orderd/internal/shared/secret"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"net/http"
)

// App 是装配完成的应用(供 bootstrap 编排生命周期)。
type App struct {
	Cfg         config.Config
	Store       *gormstore.Store
	GRPC        *ordergrpc.Server
	AdminGRPC   *opsgrpc.Server // 运维契约:仅注册到 gRPC server,不挂 REST 网关
	Interceptor grpc.UnaryServerInterceptor
	HTTP        *httpcb.Handler
	Workers     *worker.Workers
	Metrics     http.Handler  // GET /metrics(积压类 gauge,scrape 时聚合)
	alertHook   *hook.Alerter // 可为 nil(未配置告警通道)
}

// Close 释放资源:先 flush 告警队列(有限预算),再关连接池。
func (a *App) Close() error {
	if a.alertHook != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		a.alertHook.Close(ctx)
		cancel()
	}
	return a.Store.Close()
}

// Build 依配置装配全部组件。ctx 供 ormx 启动期连通性探测(fail-fast)。
func Build(ctx context.Context, cfg config.Config) (*App, error) {
	cli, err := data.NewMySQL(ctx, cfg)
	if err != nil {
		return nil, err
	}
	st := gormstore.New(cli)
	gen, err := ordernum.NewGenerator(cfg.MachineID)
	if err != nil {
		return nil, err
	}
	accounts := gormstore.NewAccountResolver(st.DB())
	// 告警:日志永远写(审计保底);配置了 alert 段再叠加 webhook 尽力送达。
	var (
		alerter   alert.Alerter = loggerAlerter{}
		alertHook *hook.Alerter
	)
	if cfg.Alert.Provider != "" {
		h, herr := hook.New(hook.Config{
			Provider: cfg.Alert.Provider, WebhookURL: cfg.Alert.WebhookURL, Secret: cfg.Alert.Secret,
			Service: appmeta.ServiceName, Env: cfg.Env,
		})
		if herr != nil {
			return nil, herr
		}
		alertHook = h
		alerter = multiAlerter{loggerAlerter{}, h}
	}
	clock := time.Now

	// 网关路由:按渠道分派到 alipaygw / wechatgw(各自启用时),未启用渠道兜底到 stub。
	var (
		aliRouter *alipaygw.Router
		wcRouter  *wechatgw.Router
		alipayR   paygateway.Router
		wechatR   paygateway.Router
	)
	if a := cfg.Pay.Alipay; a.Enabled {
		ar, aerr := alipaygw.NewRouter([]alipaygw.Config{{
			PayAccountID: a.AccountID, NotifyRouteID: a.RouteID, NotifyURL: a.NotifyURL, TradeType: a.TradeType,
			AppID: a.AppID, PrivateKey: a.PrivateKey, PrivateKeyPath: a.PrivateKeyPath, IsProduction: a.IsProduction,
			AppCertPublicKey: a.AppCertPublicKey, AppCertPublicKeyPath: a.AppCertPublicKeyPath,
			AlipayRootCert: a.AlipayRootCert, AlipayRootCertPath: a.AlipayRootCertPath,
			AlipayCertPublicKey: a.AlipayCertPublicKey, AlipayCertPublicKeyPath: a.AlipayCertPublicKeyPath,
			AlipayPublicKey: a.AlipayPublicKey,
		}})
		if aerr != nil {
			return nil, aerr
		}
		aliRouter, alipayR = ar, ar
	}
	if w := cfg.Pay.Wechat; w.Enabled {
		wr, werr := wechatgw.NewRouter(ctx, []wechatgw.Config{{
			PayAccountID: w.AccountID, NotifyRouteID: w.RouteID, NotifyURL: w.NotifyURL, TradeType: w.TradeType,
			AppID: w.AppID, MchID: w.MchID, MchCertSerialNumber: w.MchCertSerialNumber, MchAPIv3Key: w.MchAPIv3Key,
			MchPrivateKeyPath: w.MchPrivateKeyPath, MchPrivateKeyPEM: w.MchPrivateKeyPEM,
			WechatPayCertificatePath: w.WechatpayCertificatePath, WechatPayCertificatePEM: w.WechatpayCertificatePEM,
			WechatPayPublicKeyID: w.WechatpayPublicKeyID, WechatPayPublicKeyPath: w.WechatpayPublicKeyPath, WechatPayPublicKeyPEM: w.WechatpayPublicKeyPEM,
		}})
		if werr != nil {
			return nil, werr
		}
		wcRouter, wechatR = wr, wr
	}
	var router paygateway.Router = channelRouter{alipay: alipayR, wechat: wechatR, fallback: stub.Router{}}

	// 指标 Registry:积压 gauge(scrape 时聚合)+ 关键路径速率/时延(进程内埋点)。
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewBacklogCollector(st.DB()))
	flow := metrics.NewFlowMetrics(reg)

	pay := paymentapp.New(paymentapp.Deps{
		Store: st, Router: router, Accounts: accounts, Alerter: alerter, Clock: clock,
		NewEventID: idgen.UUIDv4, OutTradeNo: gen.OutTradeNo, AttemptTTL: cfg.AttemptTTL, Obs: flow,
	})
	// 定价:默认 fail-closed;仅显式开启(BFF 经严格服务身份)才采信调用方金额(评审 P1)。
	var pricing models.PricingAuthority = policy.DenyAllPricing{}
	if cfg.PricingTrustCaller {
		pricing = policy.TrustedCallerPricing{}
	}
	// 金额策略:per-app 配置(app_amount_policies)优先,未配置/停用回退全局默认档;
	// 查询错误 fail-closed 拒单(FallbackAmountPolicy 语义)。
	amounts := policy.FallbackAmountPolicy{
		Primary:  gormstore.NewAmountPolicyResolver(st.DB()),
		Fallback: policy.DefaultAmountPolicy(),
	}
	ord := orderapp.New(orderapp.Deps{
		Store: st, Payment: pay, Pricing: pricing,
		Amounts: amounts, Accounts: accounts, Clock: clock,
		NewOrderNo: gen.OrderNo, NewToken: func() (string, error) { return idgen.SecureToken(32) },
		NewEventID: idgen.UUIDv4, OrderTTL: cfg.OrderTTL, MaxPendingPerUser: cfg.MaxPendingPerUser,
		Alerter: alerter,
	})
	ref := refundapp.New(refundapp.Deps{
		Store: st, Router: router, Accounts: accounts, Alerter: alerter, Clock: clock,
		NewRefundNo: gen.RefundNo, NewEventID: idgen.UUIDv4, Enabled: cfg.RefundEnabled,
	})
	grpcSrv, err := ordergrpc.New(ordergrpc.Deps{Order: ord, Refund: ref, Store: st})
	if err != nil {
		return nil, err
	}
	adminSrv, err := opsgrpc.New(opsapp.New(opsapp.Deps{Store: st, Clock: clock}))
	if err != nil {
		return nil, err
	}
	verifier, err := auth.NewJWTVerifier(cfg.JWTKeys, cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		return nil, err
	}
	interceptor := ordergrpc.AuthInterceptor(verifier)

	// Outbox 投递 worker。
	guard, err := webhook.NewSSRFGuard(cfg.WebhookAllowHosts, cfg.WebhookAllowCIDRs)
	if err != nil {
		return nil, err
	}
	sender := webhook.NewSender(guard, 10*time.Second)
	endpoints := gormstore.NewEndpointResolver(st.DB())
	// 密钥来源:挂载目录(生产,轮换免重启)优先;未配置回退 env Static(dev)。不合并两来源。
	var secrets secret.Resolver = secret.NewStatic(cfg.WebhookSecrets)
	if cfg.SecretsDir != "" {
		secrets = secret.NewDir(cfg.SecretsDir)
		logger.Infow("密钥解析走挂载目录", "dir", cfg.SecretsDir)
	}
	delivery := worker.NewDelivery(st, endpoints, secrets, sender, alerter, flow,
		worker.Config{WorkerID: cfg.WorkerID}, clock)
	query := worker.NewQueryWorker(st, router, pay, alerter, worker.QueryConfig{WorkerID: cfg.WorkerID}, clock)
	timeout := worker.NewTimeoutWorker(st, ord, pay, worker.TimeoutConfig{WorkerID: cfg.WorkerID}, clock)

	set := worker.Set{Delivery: delivery, Query: query, Timeout: timeout}
	if cfg.RefundEnabled { // 退款关闭时无 PROCESSING 退款,查单 worker 无需运行
		set.RefundQuery = worker.NewRefundQueryWorker(st, ref, alerter, worker.RefundQueryConfig{WorkerID: cfg.WorkerID}, clock)
	}
	// 死信兜底重放 worker:默认关闭(上线观察死信量级与端点健康后再开)。
	// MaxAttempts 与投递 worker 的死信阈值同源(worker.Config 默认 8,此处不另配)。
	if cfg.DeadLetter.Enabled {
		set.DeadLetterRequeue = worker.NewDeadLetterRequeueWorker(st, worker.DeadLetterRequeueConfig{
			Enabled:  true,
			Interval: cfg.DeadLetter.Interval, Cooldown: cfg.DeadLetter.Cooldown,
			MaxAutoRequeues: int32(cfg.DeadLetter.MaxAutoRequeues), MaxLifetime: cfg.DeadLetter.MaxLifetime,
		}, clock)
	}
	// 对账 worker:需真实渠道账单源;V1 默认关闭(ReconcileEnabled=false → 不启动)。
	if cfg.ReconcileEnabled {
		reconcileSvc := reconcileapp.New(reconcileapp.Deps{
			Store: st, Bill: billstub.Source{}, Alerter: alerter, Clock: clock, NewBatchNo: gen.BatchNo,
		})
		set.Reconcile = worker.NewReconcileWorker(reconcileSvc, accounts, cfg.ReconcileInterval, clock)
	}

	return &App{
		Cfg: cfg, Store: st, GRPC: grpcSrv, AdminGRPC: adminSrv, Interceptor: interceptor,
		HTTP: httpcb.New(aliRouter, wcRouter, pay), Workers: worker.New(set),
		Metrics: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), alertHook: alertHook,
	}, nil
}

// multiAlerter 顺序广播到多个告警出口(装配层组合:日志保底 + webhook 尽力送达)。
type multiAlerter []alert.Alerter

func (m multiAlerter) Anomaly(ctx context.Context, kind string, fields map[string]any) {
	for _, a := range m {
		a.Anomaly(ctx, kind, fields)
	}
}

// channelRouter 按渠道分派:alipay/wechat → 各自真实路由,未启用渠道 → 兜底(stub)。
type channelRouter struct {
	alipay   paygateway.Router
	wechat   paygateway.Router
	fallback paygateway.Router
}

func (c channelRouter) Resolve(channel, payAccountID string) (paygateway.Gateway, error) {
	switch {
	case channel == "alipay" && c.alipay != nil:
		return c.alipay.Resolve(channel, payAccountID)
	case channel == "wechat" && c.wechat != nil:
		return c.wechat.Resolve(channel, payAccountID)
	default:
		return c.fallback.Resolve(channel, payAccountID)
	}
}

// loggerAlerter 用 gtkit/logger 输出高等级告警(资金异常/死信等)。
type loggerAlerter struct{}

func (loggerAlerter) Anomaly(_ context.Context, kind string, fields map[string]any) {
	kv := make([]any, 0, len(fields)*2+2)
	kv = append(kv, "anomaly_kind", kind)
	for k, v := range fields {
		kv = append(kv, k, v)
	}
	logger.Errorw("资金异常告警", kv...)
}
