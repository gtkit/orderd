package grpc

import (
	"testing"

	"github.com/gtkit/orderd/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestToStatusConflictMapping 冲突类领域错误的 gRPC 映射:业务单号冲突与幂等键冲突
// 均为 AlreadyExists,但消息可区分(接入方处置不同:查旧单/换单号 vs 同键改参)。
func TestToStatusConflictMapping(t *testing.T) {
	cases := []struct {
		err     error
		code    codes.Code
		message string
	}{
		{models.ErrClientOrderNoConflict, codes.AlreadyExists, "CLIENT_ORDER_NO_CONFLICT"},
		{models.ErrIdempotencyConflict, codes.AlreadyExists, "IDEMPOTENCY_CONFLICT"},
	}
	for _, c := range cases {
		st, ok := status.FromError(toStatus(c.err))
		if !ok || st.Code() != c.code || st.Message() != c.message {
			t.Fatalf("toStatus(%v) 期望 %v/%s,实际 %v/%s", c.err, c.code, c.message, st.Code(), st.Message())
		}
	}
}
