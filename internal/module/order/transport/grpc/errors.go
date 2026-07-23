package grpc

import (
	"errors"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/account"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus 把领域错误映射到 gRPC status(消息保持稳定,供接入方判定)。
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, models.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "IDEMPOTENCY_CONFLICT")
	case errors.Is(err, models.ErrOrderNotFound), errors.Is(err, models.ErrRefundNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, models.ErrOrderNotOwned):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, models.ErrOrderNotPending), errors.Is(err, models.ErrIllegalTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, models.ErrActiveAttemptExists), errors.Is(err, models.ErrActiveRefundExists):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, models.ErrRefundNotAllowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, models.ErrInvalidAmount), errors.Is(err, models.ErrAmountPolicy):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, account.ErrNoAccount):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		// 未分类错误不外泄内部细节。
		return status.Error(codes.Internal, "内部错误")
	}
}
