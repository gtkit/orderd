// Package alert 定义告警端口。application 只依赖接口(资金异常、死信等高等级告警),
// runtime 注入 gtkit/logger 支撑的实现;测试用 Nop 或记录型实现。
package alert

import "context"

// Alerter 高等级告警出口。
type Alerter interface {
	// Anomaly 上报一次异常事件(kind 为异常类别,fields 为结构化上下文)。
	Anomaly(ctx context.Context, kind string, fields map[string]any)
}

// Nop 是空实现。
type Nop struct{}

func (Nop) Anomaly(context.Context, string, map[string]any) {}
