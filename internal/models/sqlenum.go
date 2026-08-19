package models

import (
	"database/sql/driver"
	"fmt"
)

// 让领域枚举可直接用于 database/sql 的扫描与写入(避免仓储层到处做 int 转换)。

func scanInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case []byte:
		var x int64
		_, err := fmt.Sscan(string(n), &x)
		return x, err
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("models: 无法把 %T 扫描为枚举", v)
	}
}

func (s OrderStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *OrderStatus) Scan(v any) error            { n, err := scanInt(v); *s = OrderStatus(n); return err }

func (c CloseReason) Value() (driver.Value, error) { return int64(c), nil }
func (c *CloseReason) Scan(v any) error            { n, err := scanInt(v); *c = CloseReason(n); return err }

func (m PayMethod) Value() (driver.Value, error) { return int64(m), nil }
func (m *PayMethod) Scan(v any) error            { n, err := scanInt(v); *m = PayMethod(n); return err }

func (s AttemptStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *AttemptStatus) Scan(v any) error            { n, err := scanInt(v); *s = AttemptStatus(n); return err }

func (s RefundStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *RefundStatus) Scan(v any) error            { n, err := scanInt(v); *s = RefundStatus(n); return err }

func (k TxnKind) Value() (driver.Value, error) { return int64(k), nil }
func (k *TxnKind) Scan(v any) error            { n, err := scanInt(v); *k = TxnKind(n); return err }

func (s OutboxStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *OutboxStatus) Scan(v any) error            { n, err := scanInt(v); *s = OutboxStatus(n); return err }

func (s BatchStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *BatchStatus) Scan(v any) error            { n, err := scanInt(v); *s = BatchStatus(n); return err }

func (d DiffType) Value() (driver.Value, error) { return int64(d), nil }
func (d *DiffType) Scan(v any) error            { n, err := scanInt(v); *d = DiffType(n); return err }

func (s DiffHandleStatus) Value() (driver.Value, error) { return int64(s), nil }
func (s *DiffHandleStatus) Scan(v any) error {
	n, err := scanInt(v)
	*s = DiffHandleStatus(n)
	return err
}
