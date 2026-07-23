# Tasks: restructure-internal-layout

> 纯结构:仅移动包 + 改 import,不改逻辑。每阶段 `go build ./... && golangci-lint run ./... && go test -race ./...` 全绿方推进。

## 1. 纯工具 → internal/pkg

- [x] 1.1 `auth` → `internal/pkg/auth`;全仓 import 替换;编译/lint/race 绿
- [x] 1.2 `sqlutil` → `internal/pkg/sqlutil`;同上
- [x] 1.3 `webhook` → `internal/pkg/webhook`;同上

## 2. 跨模块 port → internal/shared

- [x] 2.1 `account` → `internal/shared/account`;import 替换;绿
- [x] 2.2 `endpoint` → `internal/shared/endpoint`;绿
- [x] 2.3 `bill` → `internal/shared/bill`;绿
- [x] 2.4 `secret` → `internal/shared/secret`;绿
- [x] 2.5 `alert` → `internal/shared/alert`;绿

## 3. 文档与门禁

- [x] 3.1 README 目录树 + DESIGN 同步新布局
- [x] 3.2 若有 check-architecture 脚本/grep,更新路径;`internal/pkg 禁反向 import internal/module` 等红线复核
- [x] 3.3 `make check`(govulncheck+gosec)+ `make tool`(lint+gofumpt)+ race 全绿;`openspec validate --strict` 过;归档
