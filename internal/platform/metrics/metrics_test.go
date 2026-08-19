package metrics

import (
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"

	_ "github.com/go-sql-driver/mysql"
)

// TestCollectorScrapeFailure 采集失败不 5xx、指标缺失、scrape_errors_total 递增(spec 反证)。
func TestCollectorScrapeFailure(t *testing.T) {
	// 指向不可达端口的懒连接:查询时必然失败。
	sqlDB, err := sql.Open("mysql", "root:@tcp(127.0.0.1:1)/none?timeout=200ms")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}),
		&gorm.Config{Logger: glogger.Discard, DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("gorm: %v", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewBacklogCollector(db))

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("采集失败不得使 /metrics 5xx,实际: %d", resp.StatusCode)
	}
	buf := make([]byte, 64<<10)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "orderd_metrics_scrape_errors_total") {
		t.Fatalf("应暴露 scrape_errors_total: %s", body)
	}
	if strings.Contains(body, "orderd_outbox_backlog") {
		t.Fatal("查询失败的指标本轮不应产出")
	}
	// 计数应为本轮失败的查询条数(>0)。
	if strings.Contains(body, "orderd_metrics_scrape_errors_total 0") {
		t.Fatal("scrape_errors_total 应递增")
	}
}
