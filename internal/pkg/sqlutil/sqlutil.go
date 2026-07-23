// Package sqlutil 提供 database/sql 可空类型的小工具,供应用层共享(避免各包重复)。
package sqlutil

import (
	"database/sql"
	"time"
)

// Time 非零时间 → 有效 NullTime,零值 → 无效。
func Time(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t, Valid: true}
}

// Int64 始终有效。
func Int64(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }

// Str 非空 → 有效 NullString,空 → 无效(NULL)。
func Str(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// OrDefault s 为空时返回 def。
func OrDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
