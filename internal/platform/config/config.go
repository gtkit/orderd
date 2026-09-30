// Package config 从外置 YAML 文件加载并校验运行配置(进程启动时一次性读取,fail-fast)。
//
// 分层来源(对齐 pay360 + 全局密钥纪律):
//   - 结构化、非敏感项来自 YAML 文件(config/env/env.yml,一机一份,不入库);
//   - 敏感项(支付宝私钥/公钥、JWT 密钥、webhook 密钥、MySQL 口令)一律走【环境变量覆盖】,
//     绝不写入 YAML、不入库、不打日志。
//
// 运行环境由 YAML 的 env 字段决定(dev/test/prod),不依赖文件名或命令行参数。
package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 运行配置(进程内扁平视图)。字段集与既有装配代码保持一致,消费方无需改动。
type Config struct {
	Env      string    // dev/staging/prod
	Log      LogConfig // 日志(gtkit/logger)
	GRPCAddr string    // gRPC 监听地址
	HTTPAddr string    // HTTP(支付回调/健康检查/REST 网关)监听地址
	// 数据库(database 段)。ormx.Open 用分字段;口令默认取 yaml,env ORDERD_MYSQL_PASSWORD 非空则覆盖(生产密钥注入)。
	MySQLDBType     string // 目前仅支持 mysql
	MySQLHost       string
	MySQLPort       string
	MySQLUser       string
	MySQLPassword   string
	MySQLDBName     string
	MySQLMaxOpen    int
	MySQLMaxIdle    int
	MySQLMaxLifeSec int

	Redis         RedisConfig   // Redis(当前未被消费,仅解析保留供后续使用)
	MachineID     int64         // 雪花 machineID(StatefulSet ordinal)
	NotifyBaseURL string        // 回调基础 URL
	RefundEnabled bool          // 退款开关(V1=false)
	OrderTTL      time.Duration // 订单支付截止
	AttemptTTL    time.Duration // 单次支付尝试截止
	// MaxPendingPerUser 单用户 PENDING 订单数上限(近似防刷;未配置默认 5,显式 0=不限)。
	MaxPendingPerUser int
	// LatePaymentRefundApps 已关闭订单收到付款时自动退款(不恢复订单)的 app;其余 app 结算为 PAID。
	LatePaymentRefundApps []string

	// Outbox 投递 worker。
	WorkerID          string            // 副本实例标识(租约持有者)
	WebhookAllowHosts []string          // SSRF 白名单主机
	WebhookAllowCIDRs []string          // SSRF 白名单 CIDR
	WebhookSecrets    map[string][]byte // dev:密钥引用→明文(env 覆盖;生产用 SecretsDir)
	SecretsDir        string            // 密钥挂载目录(K8s Secret volume);配置后优先于 env Static

	// 对账 worker(需真实渠道账单源;V1 默认关闭)。
	ReconcileEnabled  bool
	ReconcileInterval time.Duration

	// 死信兜底重放 worker(outbox.deadletter 段;默认关闭,上线观察后再开)。
	DeadLetter DeadLetterConfig

	// 鉴权(JWT):kid→密钥(轮换)+ issuer/audience。密钥走 env 覆盖;必填,禁止无鉴权启动。
	JWTKeys     map[string][]byte
	JWTIssuer   string
	JWTAudience string

	// 定价可信边界:默认 fail-closed(拒绝采信客户端金额),须显式开启才信任调用方。
	PricingTrustCaller bool

	// 告警送达通道(alert 段)。未配置 provider 时仅日志告警;webhook_url 含 token 属敏感,
	// 只存在于 git 忽略+挂载注入的 env.yml(与 pay 段同纪律),禁入库/git/日志。
	Alert AlertConfig

	// 支付渠道凭据与路由。字段与 go-pay 的 alipay/wechat Config 对齐(path/内容双形态、证书/公钥模式),
	// 外加 orderd 路由/运营字段。敏感项(私钥/APIv3 密钥/证书内容)只应存在于【git 忽略 + 挂载注入】的
	// env.yml 或 *_path 指向的挂载文件;提交仓库的 example 一律留空。禁入库/git/日志。
	Pay PayConfig
}

// LogConfig 日志配置(log 段),映射到 gtkit/logger 选项。
// 注:localtime / sql 在 gtkit/logger v1.8.1 无对应选项,仅解析保留(sql 预留给 ORM SQL 日志)。
type LogConfig struct {
	Level      string
	Path       string
	MaxAge     int  // 日志保留天数
	MaxBackups int  // 保留文件数
	MaxSize    int  // 单文件 MB
	Compress   bool // 归档压缩
	Console    bool // 输出到控制台(consolestdout)
	File       bool // 输出到文件(filestdout)
	LocalTime  bool // 轮转用本地时间(当前 logger 版本无选项,仅保留)
	SQL        bool // ORM SQL 日志开关(未接线,仅保留)
}

// RedisConfig Redis 配置(redis 段)。当前 orderd 未消费,仅解析保留。
type RedisConfig struct {
	Addr     string
	Password string
	DBs      []int
	Prefix   string
}

// AlertConfig 告警送达通道(alert 段):dingtalk/feishu/generic 群机器人或自建网关。
type AlertConfig struct {
	Provider   string // dingtalk / feishu / generic;空=未启用(仅日志)
	WebhookURL string // 机器人/网关地址(敏感)
	Secret     string // 可选:钉钉加签/飞书签名密钥(敏感)
}

// DeadLetterConfig 死信兜底重放(outbox.deadletter 段)。
type DeadLetterConfig struct {
	Enabled         bool
	Interval        time.Duration // 轮询间隔(默认 1h)
	Cooldown        time.Duration // 转死信后的冷却(默认 1h)
	MaxAutoRequeues int           // 自动重放追加次数上限(默认 3)
	MaxLifetime     time.Duration // 存活上限(默认 168h)
}

// PayConfig 支付渠道配置(pay 段)。
type PayConfig struct {
	Wechat WechatConfig
	Alipay AlipayConfig
}

// WechatConfig 微信支付账号配置(单账号;多账号待扩展为列表)。
type WechatConfig struct {
	Enabled bool
	// orderd 路由/运营。
	AppID     string
	MchID     string
	NotifyURL string
	AccountID string // == app_pay_accounts.pay_account_id
	RouteID   string // == app_pay_accounts.notify_route_id
	TradeType string // native(默认)/jsapi/app/h5
	// go-pay wechat.Config 凭据(敏感)。
	MchCertSerialNumber      string
	MchAPIv3Key              string
	MchPrivateKeyPath        string
	MchPrivateKeyPEM         string
	WechatpayCertificatePath string // 证书模式,与公钥模式二选一
	WechatpayCertificatePEM  string
	WechatpayPublicKeyID     string // 公钥模式
	WechatpayPublicKeyPath   string
	WechatpayPublicKeyPEM    string
}

// AlipayConfig 支付宝账号配置(单账号;多账号待扩展为列表)。
type AlipayConfig struct {
	Enabled bool
	// orderd 路由/运营。
	AppID     string
	NotifyURL string
	AccountID string // == app_pay_accounts.pay_account_id
	RouteID   string // == app_pay_accounts.notify_route_id
	TradeType string // native(默认)/page/wap/app/jsapi
	// go-pay alipay.Config 凭据(敏感)。
	PrivateKey     string
	PrivateKeyPath string
	IsProduction   bool
	// 证书模式(三项全填或全空):
	AppCertPublicKey        string
	AppCertPublicKeyPath    string
	AlipayRootCert          string
	AlipayRootCertPath      string
	AlipayCertPublicKey     string
	AlipayCertPublicKeyPath string
	// 普通公钥模式(未用证书模式时必填):
	AlipayPublicKey string
}

// fileConfig 是 YAML 文件形态(仅非敏感项)。敏感项不在此出现,一律 env 覆盖。
type fileConfig struct {
	Env string `yaml:"env"`
	Log struct {
		SQL           int    `yaml:"sql"`
		Compress      int    `yaml:"compress"`
		ConsoleStdout int    `yaml:"consolestdout"`
		FileStdout    int    `yaml:"filestdout"`
		Level         string `yaml:"level"`
		LocalTime     int    `yaml:"localtime"`
		MaxAge        int    `yaml:"maxage"`
		MaxBackups    int    `yaml:"maxbackups"`
		MaxSize       int    `yaml:"maxsize"`
		Path          string `yaml:"path"`
	} `yaml:"log"`
	GRPC struct {
		Addr string `yaml:"addr"`
	} `yaml:"grpc"`
	HTTP struct {
		Addr          string `yaml:"addr"`
		NotifyBaseURL string `yaml:"notifybaseurl"`
	} `yaml:"http"`
	Database struct {
		DBType         string `yaml:"dbtype"`
		Host           string `yaml:"host"`
		Port           string `yaml:"port"`
		DBName         string `yaml:"dbname"`
		User           string `yaml:"user"`
		Password       string `yaml:"password"`
		MaxIdleConn    int    `yaml:"maxidleconn"`
		MaxOpenConn    int    `yaml:"maxopenconn"`
		MaxLifeSeconds int    `yaml:"maxlifeseconds"`
	} `yaml:"database"`
	Redis struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		DBs      []int  `yaml:"dbs"`
		Prefix   string `yaml:"prefix"`
	} `yaml:"redis"`
	Snowflake struct {
		MachineID int64 `yaml:"machineid"`
	} `yaml:"snowflake"`
	Worker struct {
		ID string `yaml:"id"`
	} `yaml:"worker"`
	Order struct {
		TTL                   string   `yaml:"ttl"`
		AttemptTTL            string   `yaml:"attemptttl"`
		MaxPendingPerUser     *int     `yaml:"maxpendingperuser"` // nil=未配置(默认 5),显式 0=不限
		LatePaymentRefundApps []string `yaml:"latepaymentrefundapps"`
	} `yaml:"order"`
	Refund struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"refund"`
	Reconcile struct {
		Enabled  bool   `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"reconcile"`
	Webhook struct {
		AllowHosts []string `yaml:"allowhosts"`
		AllowCIDRs []string `yaml:"allowcidrs"`
	} `yaml:"webhook"`
	Secrets struct {
		Dir string `yaml:"dir"`
	} `yaml:"secrets"`
	Pricing struct {
		TrustCaller bool `yaml:"trustcaller"`
	} `yaml:"pricing"`
	Alert struct {
		Provider   string `yaml:"provider"`
		WebhookURL string `yaml:"webhook_url"`
		Secret     string `yaml:"secret"`
	} `yaml:"alert"`
	Outbox struct {
		Deadletter struct {
			Enabled         bool   `yaml:"enabled"`
			Interval        string `yaml:"interval"`
			Cooldown        string `yaml:"cooldown"`
			MaxAutoRequeues int    `yaml:"max_auto_requeues"`
			MaxLifetime     string `yaml:"max_lifetime"`
		} `yaml:"deadletter"`
	} `yaml:"outbox"`
	Auth struct {
		Issuer   string `yaml:"issuer"`
		Audience string `yaml:"audience"`
	} `yaml:"auth"`
	Pay struct {
		Wechat struct {
			Enabled                  bool   `yaml:"enabled"`
			AppID                    string `yaml:"app_id"`
			MchID                    string `yaml:"mch_id"`
			NotifyURL                string `yaml:"notify_url"`
			AccountID                string `yaml:"account_id"`
			RouteID                  string `yaml:"route_id"`
			TradeType                string `yaml:"trade_type"`
			MchCertSerialNumber      string `yaml:"mch_cert_serial_number"`
			MchAPIv3Key              string `yaml:"mch_apiv3_key"`
			MchPrivateKeyPath        string `yaml:"mch_private_key_path"`
			MchPrivateKeyPEM         string `yaml:"mch_private_key_pem"`
			WechatpayCertificatePath string `yaml:"wechatpay_certificate_path"`
			WechatpayCertificatePEM  string `yaml:"wechatpay_certificate_pem"`
			WechatpayPublicKeyID     string `yaml:"wechatpay_public_key_id"`
			WechatpayPublicKeyPath   string `yaml:"wechatpay_public_key_path"`
			WechatpayPublicKeyPEM    string `yaml:"wechatpay_public_key_pem"`
		} `yaml:"wechat"`
		Alipay struct {
			Enabled                 bool   `yaml:"enabled"`
			AppID                   string `yaml:"app_id"`
			NotifyURL               string `yaml:"notify_url"`
			AccountID               string `yaml:"account_id"`
			RouteID                 string `yaml:"route_id"`
			TradeType               string `yaml:"trade_type"`
			PrivateKey              string `yaml:"private_key"`
			PrivateKeyPath          string `yaml:"private_key_path"`
			IsProduction            bool   `yaml:"is_production"`
			AppCertPublicKey        string `yaml:"app_cert_public_key"`
			AppCertPublicKeyPath    string `yaml:"app_cert_public_key_path"`
			AlipayRootCert          string `yaml:"alipay_root_cert"`
			AlipayRootCertPath      string `yaml:"alipay_root_cert_path"`
			AlipayCertPublicKey     string `yaml:"alipay_cert_public_key"`
			AlipayCertPublicKeyPath string `yaml:"alipay_cert_public_key_path"`
			AlipayPublicKey         string `yaml:"alipay_public_key"`
		} `yaml:"alipay"`
	} `yaml:"pay"`
}

// Load 读取并校验配置文件;找不到、缺关键项、含未知键或时长非法直接 error(fail-fast)。
// 严格解析(KnownFields):拼错/废弃的键在启动时报错定位,杜绝"配置写了但静默失效"。
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path) // #nosec G304 -- 路径由运维经 --config 提供(可信),非外部输入
	if err != nil {
		return Config{}, fmt.Errorf("config: 读取 %q: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var fc fileConfig
	if derr := dec.Decode(&fc); derr != nil {
		return Config{}, fmt.Errorf("config: 解析 %q: %w", path, derr)
	}

	c := Config{
		Env: orDefault(fc.Env, "dev"),
		Log: LogConfig{
			Level: fc.Log.Level, Path: fc.Log.Path,
			MaxAge: fc.Log.MaxAge, MaxBackups: fc.Log.MaxBackups, MaxSize: fc.Log.MaxSize,
			Compress: fc.Log.Compress != 0, Console: fc.Log.ConsoleStdout != 0, File: fc.Log.FileStdout != 0,
			LocalTime: fc.Log.LocalTime != 0, SQL: fc.Log.SQL != 0,
		},
		GRPCAddr:           orDefault(fc.GRPC.Addr, ":9090"),
		HTTPAddr:           orDefault(fc.HTTP.Addr, ":8080"),
		MachineID:          fc.Snowflake.MachineID,
		NotifyBaseURL:      fc.HTTP.NotifyBaseURL,
		RefundEnabled:      fc.Refund.Enabled,
		WebhookAllowHosts:  fc.Webhook.AllowHosts,
		WebhookAllowCIDRs:  fc.Webhook.AllowCIDRs,
		SecretsDir:         fc.Secrets.Dir,
		ReconcileEnabled:   fc.Reconcile.Enabled,
		JWTIssuer:          fc.Auth.Issuer,
		JWTAudience:        fc.Auth.Audience,
		PricingTrustCaller: fc.Pricing.TrustCaller,
		Alert:              AlertConfig{Provider: fc.Alert.Provider, WebhookURL: fc.Alert.WebhookURL, Secret: fc.Alert.Secret},
	}
	c.WorkerID = orDefault(fc.Worker.ID, fmt.Sprintf("worker-%d", c.MachineID))

	if c.OrderTTL, err = parseDur(fc.Order.TTL, 30*time.Minute, "order.ttl"); err != nil {
		return Config{}, err
	}
	if c.AttemptTTL, err = parseDur(fc.Order.AttemptTTL, 15*time.Minute, "order.attemptttl"); err != nil {
		return Config{}, err
	}
	c.MaxPendingPerUser = 5 // 默认上限
	if fc.Order.MaxPendingPerUser != nil {
		if *fc.Order.MaxPendingPerUser < 0 {
			return Config{}, fmt.Errorf("config: order.maxpendingperuser 不可为负")
		}
		c.MaxPendingPerUser = *fc.Order.MaxPendingPerUser
	}
	c.LatePaymentRefundApps = fc.Order.LatePaymentRefundApps
	if len(c.LatePaymentRefundApps) > 0 && !c.RefundEnabled {
		// 自动退款依赖退款查单 worker(仅 refund.enabled 时运行)收敛,关闭时退款会永久停在 PROCESSING。
		return Config{}, fmt.Errorf("config: order.latepaymentrefundapps 非空需 refund.enabled=true")
	}
	if c.ReconcileInterval, err = parseDur(fc.Reconcile.Interval, 24*time.Hour, "reconcile.interval"); err != nil {
		return Config{}, err
	}
	c.DeadLetter.Enabled = fc.Outbox.Deadletter.Enabled
	c.DeadLetter.MaxAutoRequeues = fc.Outbox.Deadletter.MaxAutoRequeues
	if c.DeadLetter.Interval, err = parseDur(fc.Outbox.Deadletter.Interval, time.Hour, "outbox.deadletter.interval"); err != nil {
		return Config{}, err
	}
	if c.DeadLetter.Cooldown, err = parseDur(fc.Outbox.Deadletter.Cooldown, time.Hour, "outbox.deadletter.cooldown"); err != nil {
		return Config{}, err
	}
	if c.DeadLetter.MaxLifetime, err = parseDur(fc.Outbox.Deadletter.MaxLifetime, 7*24*time.Hour, "outbox.deadletter.max_lifetime"); err != nil {
		return Config{}, err
	}

	// database 段 → ormx.Open 分字段。口令默认取 yaml;env ORDERD_MYSQL_PASSWORD 非空则覆盖(生产密钥注入)。
	c.MySQLDBType = orDefault(fc.Database.DBType, "mysql")
	c.MySQLHost = fc.Database.Host
	c.MySQLPort = orDefault(fc.Database.Port, "3306")
	c.MySQLUser = fc.Database.User
	c.MySQLPassword = fc.Database.Password
	if v := os.Getenv("ORDERD_MYSQL_PASSWORD"); v != "" {
		c.MySQLPassword = v
	}
	c.MySQLDBName = fc.Database.DBName
	c.MySQLMaxOpen = fc.Database.MaxOpenConn
	c.MySQLMaxIdle = fc.Database.MaxIdleConn
	c.MySQLMaxLifeSec = fc.Database.MaxLifeSeconds

	// redis 段(当前未消费,仅解析保留)。
	c.Redis = RedisConfig{Addr: fc.Redis.Addr, Password: fc.Redis.Password, DBs: fc.Redis.DBs, Prefix: fc.Redis.Prefix}

	// 其余进程级敏感项走 env 注入(不入 YAML)。
	c.JWTKeys = kvMap(os.Getenv("ORDERD_JWT_KEYS"))               // "kid=secret,kid2=secret2"
	c.WebhookSecrets = kvMap(os.Getenv("ORDERD_WEBHOOK_SECRETS")) // dev only

	// 支付渠道:凭据来自 pay 段(git 忽略 + 挂载的 env.yml,或 *_path 指向的挂载文件),不再走独立 env 变量。
	w := fc.Pay.Wechat
	c.Pay.Wechat = WechatConfig{
		Enabled: w.Enabled, AppID: w.AppID, MchID: w.MchID, NotifyURL: w.NotifyURL,
		AccountID: orDefault(w.AccountID, "acc-wechat"), RouteID: orDefault(w.RouteID, "wechat"),
		TradeType:           orDefault(w.TradeType, "native"),
		MchCertSerialNumber: w.MchCertSerialNumber, MchAPIv3Key: w.MchAPIv3Key,
		MchPrivateKeyPath: w.MchPrivateKeyPath, MchPrivateKeyPEM: w.MchPrivateKeyPEM,
		WechatpayCertificatePath: w.WechatpayCertificatePath, WechatpayCertificatePEM: w.WechatpayCertificatePEM,
		WechatpayPublicKeyID: w.WechatpayPublicKeyID, WechatpayPublicKeyPath: w.WechatpayPublicKeyPath, WechatpayPublicKeyPEM: w.WechatpayPublicKeyPEM,
	}
	a := fc.Pay.Alipay
	c.Pay.Alipay = AlipayConfig{
		Enabled: a.Enabled, AppID: a.AppID, NotifyURL: a.NotifyURL,
		AccountID: orDefault(a.AccountID, "acc-alipay"), RouteID: orDefault(a.RouteID, "alipay"),
		TradeType:  orDefault(a.TradeType, "native"),
		PrivateKey: a.PrivateKey, PrivateKeyPath: a.PrivateKeyPath, IsProduction: a.IsProduction,
		AppCertPublicKey: a.AppCertPublicKey, AppCertPublicKeyPath: a.AppCertPublicKeyPath,
		AlipayRootCert: a.AlipayRootCert, AlipayRootCertPath: a.AlipayRootCertPath,
		AlipayCertPublicKey: a.AlipayCertPublicKey, AlipayCertPublicKeyPath: a.AlipayCertPublicKeyPath,
		AlipayPublicKey: a.AlipayPublicKey,
	}

	// 校验(fail-fast)。深度凭据校验(证书三件套/公钥模式完整性)交给 go-pay provider 构造在启动时报错,
	// 此处只校验 orderd 路由必填项,不重复 go-pay 的校验逻辑。
	if fc.Database.Host == "" || fc.Database.DBName == "" {
		return Config{}, fmt.Errorf("config %q: database.host 与 database.dbname 必填", path)
	}
	if c.MySQLDBType != "mysql" {
		return Config{}, fmt.Errorf("config %q: database.dbtype 仅支持 mysql, got %q", path, c.MySQLDBType)
	}
	if c.Pay.Alipay.Enabled && (c.Pay.Alipay.AppID == "" || c.Pay.Alipay.NotifyURL == "" || c.Pay.Alipay.AccountID == "") {
		return Config{}, fmt.Errorf("config: pay.alipay.enabled=true 需 app_id/notify_url/account_id 齐全(凭据缺失由 provider 启动时报错)")
	}
	if c.Pay.Wechat.Enabled && (c.Pay.Wechat.AppID == "" || c.Pay.Wechat.MchID == "" || c.Pay.Wechat.NotifyURL == "" || c.Pay.Wechat.AccountID == "") {
		return Config{}, fmt.Errorf("config: pay.wechat.enabled=true 需 app_id/mch_id/notify_url/account_id 齐全(凭据缺失由 provider 启动时报错)")
	}
	if len(c.JWTKeys) == 0 {
		return Config{}, fmt.Errorf("config: 需 env ORDERD_JWT_KEYS(至少一把 kid=secret;禁止无鉴权启动)")
	}
	// alert 段:填了任一项就必须完整可用(fail-fast,防"配置写了但静默失效")。
	switch c.Alert.Provider {
	case "", "dingtalk", "feishu", "generic":
	default:
		return Config{}, fmt.Errorf("config: alert.provider %q 非法(可选 dingtalk/feishu/generic)", c.Alert.Provider)
	}
	if c.Alert.Provider != "" && c.Alert.WebhookURL == "" {
		return Config{}, fmt.Errorf("config: alert.provider 已设,alert.webhook_url 必填")
	}
	if c.Alert.Provider == "" && c.Alert.WebhookURL != "" {
		return Config{}, fmt.Errorf("config: alert.webhook_url 已设,alert.provider 必填(dingtalk/feishu/generic)")
	}
	return c, nil
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func parseDur(s string, def time.Duration, field string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("config: %s 时长非法 %q: %w", field, s, err)
	}
	return d, nil
}

// csv 解析逗号分隔列表(去空白,忽略空项)。
func csv(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// kvMap 解析 "k1=v1,k2=v2"(env 注入的密钥引用→明文)。
func kvMap(s string) map[string][]byte {
	m := map[string][]byte{}
	for _, p := range csv(s) {
		if k, v, ok := strings.Cut(p, "="); ok {
			m[strings.TrimSpace(k)] = []byte(v)
		}
	}
	return m
}
