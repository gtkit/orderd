// Package ordernum 生成订单号/商户单号/退款号/对账批次号(雪花 + 前缀)。
//
// 雪花结构:42 位毫秒时间戳(自定义 epoch)+ 10 位 machineID + 12 位序列。
// machineID 由部署侧静态分配(StatefulSet ordinal,见 DESIGN §18);单实例内并发安全。
package ordernum

import (
	"fmt"
	"sync"
	"time"
)

const (
	machineBits  = 10
	seqBits      = 12
	maxMachineID = (1 << machineBits) - 1
	maxSeq       = (1 << seqBits) - 1
	timeShift    = machineBits + seqBits
	machineShift = seqBits
)

// epoch 自定义纪元:2024-01-01 00:00:00 UTC(毫秒)。
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// Snowflake 是并发安全的雪花 ID 生成器。
type Snowflake struct {
	mu        sync.Mutex
	machineID int64
	lastMs    int64
	seq       int64
	now       func() time.Time // 可注入,便于测试
}

// NewSnowflake 创建生成器;machineID 超范围返回 error。
func NewSnowflake(machineID int64) (*Snowflake, error) {
	if machineID < 0 || machineID > maxMachineID {
		return nil, fmt.Errorf("ordernum: machineID %d 超出范围 [0,%d]", machineID, maxMachineID)
	}
	return &Snowflake{machineID: machineID, now: time.Now}, nil
}

// NextID 返回下一个单调递增的雪花 ID。
func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 时钟回拨保护:不倒退,沿用上次时间推进序列。
	ms := max(s.now().UnixMilli()-epoch, s.lastMs)
	if ms == s.lastMs {
		s.seq = (s.seq + 1) & maxSeq
		if s.seq == 0 { // 本毫秒序列耗尽,自旋到下一毫秒
			for ms <= s.lastMs {
				ms = s.now().UnixMilli() - epoch
			}
		}
	} else {
		s.seq = 0
	}
	s.lastMs = ms
	return (ms << timeShift) | (s.machineID << machineShift) | s.seq
}

// Generator 按前缀格式化各类业务单号。
type Generator struct{ sf *Snowflake }

// NewGenerator 用给定 machineID 创建单号生成器。
func NewGenerator(machineID int64) (*Generator, error) {
	sf, err := NewSnowflake(machineID)
	if err != nil {
		return nil, err
	}
	return &Generator{sf: sf}, nil
}

// FromSnowflake 复用已有雪花生成器。
func FromSnowflake(sf *Snowflake) *Generator { return &Generator{sf: sf} }

func (g *Generator) OrderNo() string    { return fmt.Sprintf("OD%d", g.sf.NextID()) } // 订单号
func (g *Generator) OutTradeNo() string { return fmt.Sprintf("PT%d", g.sf.NextID()) } // 商户订单号(尝试)
func (g *Generator) RefundNo() string   { return fmt.Sprintf("RF%d", g.sf.NextID()) } // 退款单号
func (g *Generator) BatchNo() string    { return fmt.Sprintf("RC%d", g.sf.NextID()) } // 对账批次号
