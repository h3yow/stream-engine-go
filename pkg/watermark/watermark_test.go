package watermark

import (
	"testing"
	"time"
)

func secTime(n int) time.Time { return time.Unix(int64(n), 0).UTC() }

// TestOutOfOrderAdvance 验证乱序到达时水位线 = 最大事件时间 - 允许乱序，且单调不减。
func TestOutOfOrderAdvance(t *testing.T) {
	g := New(2 * time.Second)
	cases := []struct {
		observe int // 观测的事件时间（秒）
		wantWM  int // 期望水位线（秒）
	}{
		{3, 1},
		{7, 5}, // 后来的更大时间戳推进水位线
		{5, 5}, // 乱序旧事件不回退
		{6, 5},
		{10, 8},
	}
	for i, c := range cases {
		wm := g.Observe(secTime(c.observe))
		got := wm.Unix()
		if got != int64(c.wantWM) {
			t.Fatalf("step %d: observe=%ds 期望水位线=%ds, 实际=%ds",
				i, c.observe, c.wantWM, got)
		}
		t.Logf("[水位线判定] 观测事件时间=%ds → 当前水位线=%ds (maxObserved=%ds, outOfOrderness=2s)",
			c.observe, got, c.observe)
	}
	if cur := g.Current().Unix(); cur != 8 {
		t.Fatalf("Current()=%ds 期望 8s", cur)
	}
}

// TestEmptyGenerator 验证未观测事件时水位线为零值。
func TestEmptyGenerator(t *testing.T) {
	g := New(time.Second)
	if !g.Current().IsZero() {
		t.Fatalf("空生成器水位线应为零值，实际=%v", g.Current())
	}
}

// TestRestore 验证从检查点恢复后水位线不回退。
func TestRestore(t *testing.T) {
	g := New(2 * time.Second)
	g.Restore(secTime(5))
	if wm := g.Current().Unix(); wm != 5 {
		t.Fatalf("恢复后期望水位线=5s，实际=%ds", wm)
	}
	g.Observe(secTime(6)) // 恢复后观测 6s，maxObserved=8s，水位线仍为 5s，不回退
	if wm := g.Current().Unix(); wm != 5 {
		t.Fatalf("恢复后观测旧时间戳不应回退，实际=%ds", wm)
	}
	g.Observe(secTime(9))
	if wm := g.Current().Unix(); wm != 7 {
		t.Fatalf("恢复后新推进期望 7s，实际=%ds", wm)
	}
}
