package window

import (
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

func ts(n int) time.Time { return time.Unix(int64(n), 0).UTC() }

func ev(key string, sec, value, seq int) event.Event {
	return event.Event{Key: key, Timestamp: ts(sec), Value: int64(value), Seq: int64(seq)}
}

// TestAssignBoundaries 验证 [start, end) 边界归属与乱序无关。
func TestAssignBoundaries(t *testing.T) {
	a := NewTumblingAssigner(10 * time.Second)
	cases := []struct {
		sec       int
		wantStart int
	}{
		{0, 0}, {9, 0}, {10, 10}, {19, 10}, {-1, -10},
	}
	for _, c := range cases {
		w := a.Assign(ts(c.sec))
		if w.Start.Unix() != int64(c.wantStart) || w.End.Unix() != int64(c.wantStart+10) {
			t.Fatalf("ts=%ds 期望窗口 [%d,%d)，实际 [%d,%d)",
				c.sec, c.wantStart, c.wantStart+10, w.Start.Unix(), w.End.Unix())
		}
		t.Logf("[窗口判定] 事件时间=%ds → 窗口 [%d,%d)", c.sec, w.Start.Unix(), w.End.Unix())
	}
}

// TestStateDedup 验证同一 Seq 重复投递只计入一次，与投递次数无关。
func TestStateDedup(t *testing.T) {
	st := NewState()
	w := Window{Start: ts(0), End: ts(10)}
	events := []event.Event{
		ev("k", 1, 100, 1),
		ev("k", 5, 20, 2),
		ev("k", 5, 20, 2), // 完全重复
		ev("k", 9, 5, 1),  // Seq=1 再次出现（值可能不同，仍只计第一次）
	}
	for i, e := range events {
		ok := st.Add(e)
		t.Logf("[去重判定] 第%d次投递 seq=%d value=%d → accepted=%v", i+1, e.Seq, e.Value, ok)
	}
	r := st.Result("k", w)
	if r.Count != 2 || r.Sum != 120 {
		t.Fatalf("去重后期望 count=2 sum=120，实际 count=%d sum=%d (seqs=%v)",
			r.Count, r.Sum, r.Seqs)
	}
	wantSeqs := []int64{1, 2}
	for i, s := range wantSeqs {
		if r.Seqs[i] != s {
			t.Fatalf("Seqs 期望 %v，实际 %v", wantSeqs, r.Seqs)
		}
	}
	t.Logf("[窗口结果] key=k window=[0,10) sum=%d count=%d seqs=%v", r.Sum, r.Count, r.Seqs)
}
