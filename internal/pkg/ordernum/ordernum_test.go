package ordernum

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewSnowflakeMachineIDRange(t *testing.T) {
	if _, err := NewSnowflake(-1); err == nil {
		t.Fatal("machineID=-1 应报错")
	}
	if _, err := NewSnowflake(maxMachineID + 1); err == nil {
		t.Fatal("machineID 超上限应报错")
	}
	if _, err := NewSnowflake(0); err != nil {
		t.Fatalf("machineID=0 应合法:%v", err)
	}
}

func TestNextIDMonotonicUnique(t *testing.T) {
	sf, _ := NewSnowflake(1)
	const n = 20000
	seen := make(map[int64]struct{}, n)
	var last int64
	for range n {
		id := sf.NextID()
		if id <= last {
			t.Fatalf("ID 非单调:%d <= %d", id, last)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("ID 重复:%d", id)
		}
		seen[id] = struct{}{}
		last = id
	}
}

// TestNextIDSequenceExhaustion 固定时钟下同一毫秒序列耗尽应自旋到下一毫秒,不重复不阻死。
func TestNextIDSequenceExhaustion(t *testing.T) {
	cur := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	steps := 0
	sf, _ := NewSnowflake(1)
	sf.now = func() time.Time {
		steps++
		if steps > 3 { // 前几次同一毫秒,之后推进,避免测试卡死
			cur = cur.Add(time.Millisecond)
		}
		return cur
	}
	seen := make(map[int64]struct{})
	for range maxSeq + 50 {
		id := sf.NextID()
		if _, dup := seen[id]; dup {
			t.Fatalf("序列耗尽后 ID 重复:%d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNextIDConcurrent(t *testing.T) {
	sf, _ := NewSnowflake(1)
	const g, per = 8, 2000
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int64]struct{}, g*per)
	for range g {
		wg.Go(func() {
			ids := make([]int64, per)
			for j := range ids {
				ids[j] = sf.NextID()
			}
			mu.Lock()
			for _, id := range ids {
				if _, dup := seen[id]; dup {
					t.Errorf("并发下 ID 重复:%d", id)
				}
				seen[id] = struct{}{}
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(seen) != g*per {
		t.Fatalf("并发唯一 ID 数=%d,期望=%d", len(seen), g*per)
	}
}

func TestGeneratorPrefixes(t *testing.T) {
	gen, _ := NewGenerator(1)
	checks := []struct {
		got, prefix string
	}{
		{gen.OrderNo(), "OD"},
		{gen.OutTradeNo(), "PT"},
		{gen.RefundNo(), "RF"},
		{gen.BatchNo(), "RC"},
	}
	for _, c := range checks {
		if !strings.HasPrefix(c.got, c.prefix) {
			t.Fatalf("%q 应以 %q 开头", c.got, c.prefix)
		}
	}
}
