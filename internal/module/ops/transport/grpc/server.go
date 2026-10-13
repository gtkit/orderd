// Package grpc 是 AdminService 的 gRPC 传输适配层:鉴权(拦截器已校验)→ protovalidate →
// 转应用层入参 → 领域错误映射。只在 gRPC 端口注册,不挂 grpc-gateway(运维契约隔离,评审 D2)。
package grpc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"buf.build/go/protovalidate"
	"github.com/gtkit/orderd/internal/models"
	opsapp "github.com/gtkit/orderd/internal/module/ops/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/pkg/auth"
	"github.com/gtkit/orderd/internal/store"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const dateLayout = "2006-01-02"

// Server 实现 orderv1.AdminServiceServer。
type Server struct {
	orderv1.UnimplementedAdminServiceServer
	svc       *opsapp.Service
	refund    *refundapp.Service // 按异常流水退款 / 异常退款人工落定(动作与审计在退款事务内完成)
	validator protovalidate.Validator
}

// New 构造 AdminService 服务器。
func New(svc *opsapp.Service, refund *refundapp.Service) (*Server, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	return &Server{svc: svc, refund: refund, validator: v}, nil
}

// validate 使用 protovalidate 验证请求消息。
func (s *Server) validate(msg proto.Message) error {
	if err := s.validator.Validate(msg); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

// actorFromContext 读取拦截器注入的运维身份(actor 只来自 token,审计不可伪造)。
func actorFromContext(ctx context.Context) (opsapp.Actor, error) {
	c, ok := auth.ClaimsFromContext(ctx)
	if !ok {
		return opsapp.Actor{}, status.Error(codes.Unauthenticated, "未鉴权")
	}
	return opsapp.Actor{AppID: c.AppID, UserID: c.UserID}, nil
}

// toStatus 领域错误 → gRPC status(未分类错误不外泄内部细节)。
func toStatus(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "记录不存在")
	case errors.Is(err, models.ErrIllegalTransition), errors.Is(err, models.ErrRefundNotAllowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, opsapp.ErrInvalidRequest), errors.Is(err, models.ErrInvalidAmount):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, models.ErrActiveRefundExists):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, models.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "IDEMPOTENCY_CONFLICT")
	default:
		return status.Error(codes.Internal, "内部错误")
	}
}

// parseCursor 解析游标(空=从头)。
func parseCursor(c string) (uint64, error) {
	if c == "" {
		return 0, nil
	}
	id, err := strconv.ParseUint(c, 10, 64)
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, "游标非法")
	}
	return id, nil
}

func cursorStr(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// parseDate 解析 YYYY-MM-DD(空=零值,不过滤)。
func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return time.Time{}, status.Error(codes.InvalidArgument, "日期须为 YYYY-MM-DD")
	}
	return t, nil
}

// ---------------------------------------------------------------------
// 对账差异
// ---------------------------------------------------------------------

// ListReconcileDiffs 差异清单。
func (s *Server) ListReconcileDiffs(ctx context.Context, req *orderv1.ListReconcileDiffsRequest) (*orderv1.ListReconcileDiffsResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	if _, err := actorFromContext(ctx); err != nil {
		return nil, err
	}
	after, err := parseCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	f := store.DiffFilter{AppID: req.AppId, Channel: req.Channel, PayAccountID: req.PayAccountId}
	if req.HandleStatus != orderv1.DiffHandleStatus_DIFF_HANDLE_STATUS_UNSPECIFIED {
		hs := models.DiffHandleStatus(int32(req.HandleStatus) - 1) // proto = DB + 1
		f.HandleStatus = &hs
	}
	if req.DiffType != orderv1.ReconcileDiffType_RECONCILE_DIFF_TYPE_UNSPECIFIED {
		dt := models.DiffType(req.DiffType) // proto = DB
		f.DiffType = &dt
	}
	if f.BillDateFrom, err = parseDate(req.BillDateFrom); err != nil {
		return nil, err
	}
	if f.BillDateTo, err = parseDate(req.BillDateTo); err != nil {
		return nil, err
	}
	rows, next, err := s.svc.ListReconcileDiffs(ctx, f, after, req.Limit)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.ListReconcileDiffsResponse{NextCursor: cursorStr(next)}
	for i := range rows {
		resp.Diffs = append(resp.Diffs, diffProto(&rows[i]))
	}
	return resp, nil
}

func diffProto(d *models.ReconcileDiff) *orderv1.ReconcileDiffRecord {
	rec := &orderv1.ReconcileDiffRecord{
		Id: d.ID, BatchId: d.BatchID, AppId: d.AppID, Channel: d.Channel, PayAccountId: d.PayAccountID,
		TransactionKind: int32(d.Kind), OriginalTransactionId: d.OriginalTransactionID,
		OutRefundNo: d.OutRefundNo, RefundNo: d.RefundNo, Currency: d.Currency,
		ImportId: d.RunID, LocalTransactionId: d.LocalTxnID, VerificationAttempts: d.VerifyAttempts,
		VerificationLastError: d.VerifyLastError.String,
		DiffType:              orderv1.ReconcileDiffType(d.DiffType), OrderNo: d.OrderNo,
		TransactionId: d.TransactionID, OutTradeNo: d.OutTradeNo,
		GatewayAmount: d.GatewayAmount, LocalAmount: d.LocalAmount, Detail: d.Detail,
		HandleStatus: orderv1.DiffHandleStatus(int32(d.HandleStatus) + 1),
		HandledBy:    d.HandledBy, HandleRemark: d.HandleRemark,
		CreatedAt: timestamppb.New(d.CreatedAt),
	}
	if d.NextVerifyAt.Valid {
		rec.NextVerificationAt = timestamppb.New(d.NextVerifyAt.Time)
	}
	if d.HandledAt.Valid {
		rec.HandledAt = timestamppb.New(d.HandledAt.Time)
	}
	return rec
}

// HandleReconcileDiff 差异处置。
func (s *Server) HandleReconcileDiff(ctx context.Context, req *orderv1.HandleReconcileDiffRequest) (*orderv1.HandleReconcileDiffResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	target := models.DiffHandleStatus(int32(req.Target) - 1) // proto = DB + 1
	got, err := s.svc.HandleReconcileDiff(ctx, actor, req.Id, target, req.Remark)
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.HandleReconcileDiffResponse{HandleStatus: orderv1.DiffHandleStatus(int32(got) + 1)}, nil
}

// ---------------------------------------------------------------------
// Outbox 死信
// ---------------------------------------------------------------------

// ListDeadLetters 死信清单。
func (s *Server) ListDeadLetters(ctx context.Context, req *orderv1.ListDeadLettersRequest) (*orderv1.ListDeadLettersResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	if _, err := actorFromContext(ctx); err != nil {
		return nil, err
	}
	after, err := parseCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	rows, next, err := s.svc.ListDeadLetters(ctx, store.DeadFilter{
		AppID: req.AppId, EventType: req.EventType, OrderNo: req.OrderNo,
	}, after, req.Limit)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.ListDeadLettersResponse{NextCursor: cursorStr(next)}
	for i := range rows {
		ob := &rows[i]
		resp.DeadLetters = append(resp.DeadLetters, &orderv1.DeadLetterRecord{
			EventId: ob.EventID, AppId: ob.AppID, OrderNo: ob.OrderNo, EventType: ob.EventType,
			AggregateVersion: ob.AggregateVersion, Attempts: ob.Attempts, LastError: ob.LastError.String,
			CreatedAt: timestamppb.New(ob.CreatedAt), UpdatedAt: timestamppb.New(ob.UpdatedAt),
		})
	}
	return resp, nil
}

// RequeueDeadLetter 人工重放。
func (s *Server) RequeueDeadLetter(ctx context.Context, req *orderv1.RequeueDeadLetterRequest) (*orderv1.RequeueDeadLetterResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	attempts, err := s.svc.RequeueDeadLetter(ctx, actor, req.EventId)
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.RequeueDeadLetterResponse{EventId: req.EventId, Attempts: attempts}, nil
}

// ---------------------------------------------------------------------
// 资金异常流水
// ---------------------------------------------------------------------

// ListAnomalyTxns 异常流水清单。
func (s *Server) ListAnomalyTxns(ctx context.Context, req *orderv1.ListAnomalyTxnsRequest) (*orderv1.ListAnomalyTxnsResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	if _, err := actorFromContext(ctx); err != nil {
		return nil, err
	}
	after, err := parseCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	f := store.AnomalyFilter{AppID: req.AppId, PayAccountID: req.PayAccountId}
	if req.PaidFrom != nil {
		f.PaidFrom = req.PaidFrom.AsTime()
	}
	if req.PaidTo != nil {
		f.PaidTo = req.PaidTo.AsTime()
	}
	rows, next, err := s.svc.ListAnomalyTxns(ctx, f, after, req.Limit)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.ListAnomalyTxnsResponse{NextCursor: cursorStr(next)}
	for i := range rows {
		t := &rows[i]
		rec := &orderv1.AnomalyTxnRecord{
			Id: t.ID, AppId: t.AppID, OrderNo: t.OrderNo, PayAccountId: t.PayAccountID,
			Channel: t.Channel, OutTradeNo: t.OutTradeNo, TransactionId: t.TransactionID,
			Amount: t.Amount, Currency: t.Currency, AnomalyKind: t.AnomalyKind,
			CreatedAt: timestamppb.New(t.CreatedAt),
		}
		if t.PaidAt.Valid {
			rec.PaidAt = timestamppb.New(t.PaidAt.Time)
		}
		resp.Txns = append(resp.Txns, rec)
	}
	return resp, nil
}

// RefundAnomalyTxn 按异常流水全额退款。
func (s *Server) RefundAnomalyTxn(ctx context.Context, req *orderv1.RefundAnomalyTxnRequest) (*orderv1.RefundAnomalyTxnResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	out, err := s.refund.RefundAnomalyTxn(ctx, refundapp.AnomalyRefundInput{
		TxnID: req.TxnId, OutRefundNo: req.OutRefundNo, Reason: req.Reason, Actor: actor.String(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.RefundAnomalyTxnResponse{RefundNo: out.RefundNo, Status: out.Status.Proto(), Reused: out.Reused}, nil
}

// ResolveRefundAbnormal 异常退款人工落定。
func (s *Server) ResolveRefundAbnormal(ctx context.Context, req *orderv1.ResolveRefundAbnormalRequest) (*orderv1.ResolveRefundAbnormalResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	target, _ := models.RefundStatusFromProto(req.Target) // protovalidate 已限定 SUCCEEDED/FAILED
	out, err := s.refund.ResolveRefundAbnormal(ctx, refundapp.ResolveInput{
		RefundNo: req.RefundNo, Target: target, ActualAmount: req.ActualAmount, Remark: req.Remark, Actor: actor.String(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.ResolveRefundAbnormalResponse{Status: out.Status.Proto()}
	if out.HasOrder {
		resp.OrderStatus = out.OrderStatus.Proto()
	}
	return resp, nil
}
