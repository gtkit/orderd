package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "env.yml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const validYAML = `env: dev
log:
  level: warn
grpc:
  addr: ":9091"
http:
  addr: ":8081"
  notifybaseurl: "https://cb.example.com"
database:
  dbtype: "mysql"
  host: "127.0.0.1"
  port: "3306"
  user: "root"
  dbname: "orderd"
  password: "ymlpass"
redis:
  addr: "127.0.0.1:6379"
  dbs: [0, 1, 2]
order:
  ttl: "20m"
pay:
  alipay:
    enabled: false
`

// setSecrets 注入必需/敏感 env,并在测试结束还原(t.Setenv 自动还原)。
func setSecrets(t *testing.T) {
	t.Helper()
	t.Setenv("ORDERD_JWT_KEYS", "dev1=dev-secret-change-me-32bytes-xx")
}

func TestLoadValid(t *testing.T) {
	setSecrets(t)
	c, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("合法配置应加载成功: %v", err)
	}
	if c.Env != "dev" || c.Log.Level != "warn" || c.GRPCAddr != ":9091" || c.HTTPAddr != ":8081" {
		t.Fatalf("基础字段错误: %+v", c)
	}
	if c.NotifyBaseURL != "https://cb.example.com" {
		t.Fatalf("notifybaseurl 错误: %q", c.NotifyBaseURL)
	}
	if c.OrderTTL != 20*time.Minute {
		t.Fatalf("order.ttl 解析错误: %v", c.OrderTTL)
	}
	if c.AttemptTTL != 15*time.Minute {
		t.Fatalf("attemptttl 应取默认 15m: %v", c.AttemptTTL)
	}
	if c.ReconcileInterval != 24*time.Hour {
		t.Fatalf("reconcile.interval 应取默认 24h: %v", c.ReconcileInterval)
	}
	if c.WorkerID != "worker-0" {
		t.Fatalf("worker.id 应自动 worker-0: %q", c.WorkerID)
	}
	if c.MySQLDBType != "mysql" || c.MySQLHost != "127.0.0.1" || c.MySQLPort != "3306" ||
		c.MySQLUser != "root" || c.MySQLDBName != "orderd" || c.MySQLPassword != "ymlpass" {
		t.Fatalf("database 分字段解析错误: %+v", c)
	}
	if c.Redis.Addr != "127.0.0.1:6379" || len(c.Redis.DBs) != 3 {
		t.Fatalf("redis 段解析错误: %+v", c.Redis)
	}
	if string(c.JWTKeys["dev1"]) != "dev-secret-change-me-32bytes-xx" {
		t.Fatalf("JWT 密钥应来自 env: %v", c.JWTKeys)
	}
}

func TestLoadStrictUnknownKey(t *testing.T) {
	setSecrets(t)
	if _, err := Load(writeTemp(t, validYAML+"unknownkey: 1\n")); err == nil {
		t.Fatal("未知键应 fail-fast")
	}
}

func TestLoadLogConfig(t *testing.T) {
	setSecrets(t)
	y := strings.Replace(validYAML, "log:\n  level: warn\n",
		"log:\n  level: info\n  sql: 1\n  compress: 0\n  consolestdout: 1\n  filestdout: 1\n  localtime: 1\n  maxage: 30\n  maxbackups: 7\n  maxsize: 64\n  path: \"./logs/orderd\"\n", 1)
	c, err := Load(writeTemp(t, y))
	if err != nil {
		t.Fatalf("log 段应加载成功: %v", err)
	}
	lg := c.Log
	if lg.Level != "info" || lg.Path != "./logs/orderd" || lg.MaxAge != 30 || lg.MaxBackups != 7 || lg.MaxSize != 64 {
		t.Fatalf("log 数值/路径映射错误: %+v", lg)
	}
	// int 0/1 → bool。
	if lg.Compress != false || !lg.Console || !lg.File || !lg.LocalTime || !lg.SQL {
		t.Fatalf("log int→bool 映射错误: %+v", lg)
	}
}

func TestLoadMissingMySQL(t *testing.T) {
	setSecrets(t)
	y := "env: dev\ndatabase:\n  dbtype: \"mysql\"\n  host: \"\"\n  dbname: \"\"\n"
	if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatalf("缺 database.host/dbname 应报错: %v", err)
	}
}

func TestLoadRejectsNonMySQLDBType(t *testing.T) {
	setSecrets(t)
	y := "env: dev\ndatabase:\n  dbtype: \"postgres\"\n  host: \"h\"\n  dbname: \"d\"\n"
	if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "dbtype") {
		t.Fatalf("非 mysql 的 dbtype 应报错: %v", err)
	}
}

func TestLoadMissingJWTFailClosed(t *testing.T) {
	// 不注入 ORDERD_JWT_KEYS。
	t.Setenv("ORDERD_JWT_KEYS", "")
	if _, err := Load(writeTemp(t, validYAML)); err == nil || !strings.Contains(err.Error(), "JWT") {
		t.Fatalf("无 JWT 密钥应拒绝启动(fail-closed): %v", err)
	}
}

// TestRepoConfigsParse 确保仓库内 env.yml / env.example.yml 始终能被严格解析(KnownFields),
// 防 config 结构与 yaml 键漂移(pay 段等)。
func TestRepoConfigsParse(t *testing.T) {
	t.Setenv("ORDERD_JWT_KEYS", "dev1=x")
	for _, p := range []string{"../../../config/env/env.yml", "../../../config/env/env.example.yml"} {
		if _, err := Load(p); err != nil {
			t.Fatalf("仓库配置 %s 应能严格解析: %v", p, err)
		}
	}
}

func TestLoadWechatRoutingRequired(t *testing.T) {
	setSecrets(t)
	// pay.wechat.enabled=true 但缺 account_id 之外的路由必填(mch_id 缺失)应报错。
	bad := validYAML + "  wechat:\n    enabled: true\n    app_id: \"wx1\"\n    notify_url: \"https://x/notify/wechat/r\"\n"
	if _, err := Load(writeTemp(t, bad)); err == nil || !strings.Contains(err.Error(), "wechat") {
		t.Fatalf("wechat.enabled=true 缺 mch_id 应报错: %v", err)
	}
	// 路由必填齐全即通过(凭据缺失留给 provider 启动时报错);凭据字段来自 pay 段(此处填占位)。
	ok := validYAML + "  wechat:\n    enabled: true\n    app_id: \"wx1\"\n    mch_id: \"16001\"\n    notify_url: \"https://x/notify/wechat/r\"\n    mch_apiv3_key: \"k\"\n    mch_private_key_pem: \"PEM\"\n"
	c, err := Load(writeTemp(t, ok))
	if err != nil {
		t.Fatalf("路由齐全应加载成功: %v", err)
	}
	if !c.Pay.Wechat.Enabled || c.Pay.Wechat.MchID != "16001" || c.Pay.Wechat.MchPrivateKeyPEM != "PEM" || c.Pay.Wechat.AccountID != "acc-wechat" {
		t.Fatalf("微信 pay 配置应正确: %+v", c.Pay.Wechat)
	}
}

func TestLoadAlipayFieldsFromPayBlock(t *testing.T) {
	setSecrets(t)
	// validYAML 末尾已有 pay.alipay.enabled=false;先剥掉再拼独立 pay 块,避免重复 key。
	full := strings.Replace(validYAML, "pay:\n  alipay:\n    enabled: false\n", "", 1)
	// 缺 notify_url/account_id 应报错。
	bad := full + "pay:\n  alipay:\n    enabled: true\n    app_id: \"2021\"\n"
	if _, err := Load(writeTemp(t, bad)); err == nil || !strings.Contains(err.Error(), "alipay") {
		t.Fatalf("alipay.enabled=true 缺 notify_url/account_id 应报错: %v", err)
	}
	okY := full + "pay:\n  alipay:\n    enabled: true\n    app_id: \"2021\"\n    notify_url: \"https://x/notify/alipay/r\"\n    private_key: \"PK\"\n    alipay_public_key: \"PUB\"\n"
	c, err := Load(writeTemp(t, okY))
	if err != nil {
		t.Fatalf("路由齐全应加载成功: %v", err)
	}
	if !c.Pay.Alipay.Enabled || c.Pay.Alipay.PrivateKey != "PK" || c.Pay.Alipay.AlipayPublicKey != "PUB" ||
		c.Pay.Alipay.AccountID != "acc-alipay" || c.Pay.Alipay.RouteID != "alipay" {
		t.Fatalf("支付宝 pay 配置应正确: %+v", c.Pay.Alipay)
	}
}

func TestLoadMySQLPassword(t *testing.T) {
	setSecrets(t)
	// 默认取 yaml 的 database.password。
	c, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if c.MySQLPassword != "ymlpass" {
		t.Fatalf("口令应默认取 yaml: %q", c.MySQLPassword)
	}
	// env ORDERD_MYSQL_PASSWORD 非空则覆盖(生产密钥注入)。
	t.Setenv("ORDERD_MYSQL_PASSWORD", "s3cr3t")
	c, err = Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if c.MySQLPassword != "s3cr3t" {
		t.Fatalf("env 非空应覆盖 yaml 口令: %q", c.MySQLPassword)
	}
}

func TestLoadBadDuration(t *testing.T) {
	setSecrets(t)
	y := strings.Replace(validYAML, "ttl: \"20m\"", "ttl: \"notaduration\"", 1)
	if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "order.ttl") {
		t.Fatalf("非法时长应报错: %v", err)
	}
}

// TestLoadAlertSection alert 段四态:未配置维持仅日志;合法;非法 provider;缺 url;缺 provider。
func TestLoadAlertSection(t *testing.T) {
	setSecrets(t)
	// 未配置:Provider 为空(仅日志),正常启动。
	c, err := Load(writeTemp(t, validYAML))
	if err != nil || c.Alert.Provider != "" {
		t.Fatalf("未配置 alert 应为空且不报错: %+v err=%v", c.Alert, err)
	}
	// 合法。
	c, err = Load(writeTemp(t, validYAML+"alert:\n  provider: dingtalk\n  webhook_url: \"https://oapi.dingtalk.com/robot/send?access_token=x\"\n  secret: \"s\"\n"))
	if err != nil || c.Alert.Provider != "dingtalk" || c.Alert.Secret != "s" {
		t.Fatalf("合法 alert 段应加载: %+v err=%v", c.Alert, err)
	}
	// 非法 provider。
	if _, err = Load(writeTemp(t, validYAML+"alert:\n  provider: slack\n  webhook_url: \"https://x\"\n")); err == nil {
		t.Fatal("非法 provider 应 fail-fast")
	}
	// 填 provider 缺 url。
	if _, err = Load(writeTemp(t, validYAML+"alert:\n  provider: feishu\n")); err == nil {
		t.Fatal("缺 webhook_url 应 fail-fast")
	}
	// 填 url 缺 provider。
	if _, err = Load(writeTemp(t, validYAML+"alert:\n  webhook_url: \"https://x\"\n")); err == nil {
		t.Fatal("缺 provider 应 fail-fast")
	}
}

// TestLoadMaxPendingPerUser order.maxpendingperuser 三态:未配置→5、显式 0→不限、负值拒。
func TestLoadMaxPendingPerUser(t *testing.T) {
	setSecrets(t)
	c, err := Load(writeTemp(t, validYAML))
	if err != nil || c.MaxPendingPerUser != 5 {
		t.Fatalf("未配置应默认 5: %d err=%v", c.MaxPendingPerUser, err)
	}
	c, err = Load(writeTemp(t, strings.Replace(validYAML, "order:\n  ttl: \"20m\"", "order:\n  ttl: \"20m\"\n  maxpendingperuser: 0", 1)))
	if err != nil || c.MaxPendingPerUser != 0 {
		t.Fatalf("显式 0 应不限: %d err=%v", c.MaxPendingPerUser, err)
	}
	if _, err = Load(writeTemp(t, strings.Replace(validYAML, "order:\n  ttl: \"20m\"", "order:\n  ttl: \"20m\"\n  maxpendingperuser: -1", 1))); err == nil {
		t.Fatal("负值应拒")
	}
}

// TestLoadSecretsDir secrets.dir 解析(未配置为空,配置后可读)。
func TestLoadSecretsDir(t *testing.T) {
	setSecrets(t)
	c, err := Load(writeTemp(t, validYAML))
	if err != nil || c.SecretsDir != "" {
		t.Fatalf("未配置应为空: %q err=%v", c.SecretsDir, err)
	}
	c, err = Load(writeTemp(t, validYAML+"secrets:\n  dir: \"/var/run/secrets/orderd\"\n"))
	if err != nil || c.SecretsDir != "/var/run/secrets/orderd" {
		t.Fatalf("secrets.dir 解析失败: %q err=%v", c.SecretsDir, err)
	}
}
