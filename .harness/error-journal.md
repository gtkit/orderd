# Error Journal — Agent 错误记忆

> 本文件记录 AI Agent 在本项目中犯过的错误，作为前馈引导防止再犯。
> 每次任务开始时 Agent 必须读取此文件。
> 新错误追加在文件末尾，不要修改已有条目。

---

<!-- 以下是示例条目，可根据实际情况删除或保留作为参考 -->

## 2026-04-09: [编造 API]

**错误描述**：使用了 `gtkit/httputil.SendJSON()` 函数，该函数不存在
**根因分析**：对 gtkit 库的 API 记忆不准确，没有先确认就直接使用
**正确做法**：不确定 gtkit 下某个包的 API 时，明确告知用户"我不确定此函数是否存在，请确认"，或请用户提供 go doc 输出
**受影响范围**：所有涉及 gtkit 库的任务

---

## 2026-04-09: [跳过并发检查]

**错误描述**：在 service 层用 map 做本地缓存，没有加锁，多个 goroutine 并发读写导致 panic
**根因分析**：只关注了业务逻辑正确性，忽略了并发场景
**正确做法**：共享 map 必须使用 sync.RWMutex 或 sync.Map；写完代码后执行 `go test -race` 验证
**受影响范围**：所有涉及共享状态的 service 和中间件

---

<!-- 新错误从这里开始追加 -->

## [ERR-20260717-001] command-failure

**Logged**: 2026-07-17T20:55:40+0800
**Status**: open
**Area**: tooling

### Summary
代码审查扫描命令使用了环境中不存在的 rg，已改用 find 与 grep。

### What Happened
待补充

### Root Cause
待补充

### Corrective Action
待补充

### Prevention Rule
待补充

### Related Files
- 待补充

### Validation Added
- 待补充

---

## [ERR-20260717-002] test-failure

**Logged**: 2026-07-17T21:05:18+0800
**Status**: open
**Area**: validation

### Summary
orderd 全量 race 测试在 event/worker 的 httptest.Server 监听本地端口时被沙箱拒绝；sleep_client repository/order 的 miniredis 监听本地端口同样被拒绝。核心 application、transport、worker 测试已通过。

### What Happened
待补充

### Root Cause
待补充

### Corrective Action
待补充

### Prevention Rule
待补充

### Related Files
- 待补充

### Validation Added
- 待补充

---

## [ERR-20260717-003] command-failure

**Logged**: 2026-07-17T21:06:05+0800
**Status**: open
**Area**: validation

### Summary
make fmt-check 调用不到 gofmt，但配方未因 command not found 失败，导致格式门禁假绿；后续改用 $(go env GOROOT)/bin/gofmt 复核。

### What Happened
待补充

### Root Cause
待补充

### Corrective Action
待补充

### Prevention Rule
待补充

### Related Files
- 待补充

### Validation Added
- 待补充

---

## [ERR-20260717-004] test-failure

**Logged**: 2026-07-17T21:06:38+0800
**Status**: open
**Area**: lint

### Summary
golangci-lint run ./... 失败：context loading failed: no go files to analyze；go list 与 go test 正常，需检查 lint 版本/配置兼容性。

### What Happened
待补充

### Root Cause
待补充

### Corrective Action
待补充

### Prevention Rule
待补充

### Related Files
- 待补充

### Validation Added
- 待补充

---
