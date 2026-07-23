module github.com/gtkit/orderd

go 1.26

// 依赖由 `go mod tidy` 根据 import 补全(先 cd proto && buf generate 生成 gen/)。
// 运行时要求:MySQL 8.0.16+(CHECK 约束 / 生成列唯一索引 / SELECT ... FOR UPDATE SKIP LOCKED)。
// 预期直接依赖:
//   buf.build/go/protovalidate      —— 请求校验
//   google.golang.org/grpc          —— gRPC server
//   google.golang.org/protobuf      —— protobuf 运行时 / EventEnvelope
//   github.com/go-sql-driver/mysql  —— MySQL 驱动
//   github.com/redis/go-redis/v9    —— Redis 客户端
//   go.uber.org/zap                 —— 结构化日志字段
//   github.com/gtkit/logger         —— 项目统一日志库(禁 slog)

require (
	buf.build/go/protovalidate v1.2.0
	github.com/go-sql-driver/mysql v1.10.0
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0
	github.com/gtkit/go-pay v1.6.1
	github.com/gtkit/logger v1.8.1
	github.com/gtkit/ormx v1.1.0
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/gorm v1.31.2
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260709200747-435963d16310.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260720211330-0afa2a65878a // indirect
)

require (
	cel.dev/expr v0.25.2 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/bytedance/gopkg v0.1.4 // indirect
	github.com/bytedance/sonic v1.15.2 // indirect
	github.com/bytedance/sonic/loader v0.5.1 // indirect
	github.com/cloudwego/base64x v0.1.7 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/cel-go v0.29.2 // indirect
	github.com/gtkit/json/v2 v2.0.7 // indirect
	github.com/gtkit/logrotate v1.1.2 // indirect
	github.com/gtkit/orderd/pb v1.0.0
	github.com/gtkit/orderd/sdk v1.0.0
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	github.com/smartwalle/alipay/v3 v3.2.30 // indirect
	github.com/smartwalle/ncrypto v1.0.4 // indirect
	github.com/smartwalle/ngx v1.1.1 // indirect
	github.com/smartwalle/nsign v1.0.9 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/wechatpay-apiv3/wechatpay-go v0.2.21 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/arch v0.28.0 // indirect
	golang.org/x/exp v0.0.0-20260718201538-764159d718ef // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260720155508-bb71a54f79dc // indirect
	gorm.io/driver/mysql v1.6.0 // indirect
)

replace github.com/gtkit/orderd/pb => ./pb

replace github.com/gtkit/orderd/sdk => ./sdk
