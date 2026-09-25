package stream

import "testing"

// TestWatermarkMonotonic 水位线只增不减，与观察顺序无关。
func TestWatermarkMonotonic(t *testing.T) {
	tr := NewWatermarkTracker(10, 1)
	if w := tr.Observe(0, 100); w != 90 {
		t.Fatalf("watermark=%d want 90", w)
	}
	if w := tr.Observe(0, 50); w != 90 { // 乱序旧事件不得回退水位线
		t.Fatalf("watermark=%d want 90（不得回退）", w)
	}
	if w := tr.Observe(0, 120); w != 110 {
		t.Fatalf("watermark=%d want 110", w)
	}
	t.Logf("水位线推进: 100->90, 50->90(乱序不回退), 120->110")
}

// TestWindowAssign 滚动窗口分配，边界归属 [start, end)。
func TestWindowAssign(t *testing.T) {
	m := NewWindowManager(100, 0)
	cases := []struct {
		ts         int64
		wantWindow Window
	}{
		{0, Window{0, 100}},
		{99, Window{0, 100}},
		{100, Window{100, 200}}, // 右边界开区间
		{-1, Window{-100, 0}},   // 负事件时间也正确
	}
	for _, c := range cases {
		if got := m.Assign(c.ts); got != c.wantWindow {
			t.Fatalf("Assign(%d)=%v want %v", c.ts, got, c.wantWindow)
		}
	}
}

// TestLateEventClassification 迟到判定：范围内计入、范围外拒绝、原因可区分。
func TestLateEventClassification(t *testing.T) {
	m := NewWindowManager(100, 20) // 窗口 100ms，允许迟到 20ms
	wm := int64(150)               // 当前水位线 150 => 迟到下限 130

	onTime := Event{ID: "a", Key: "k", EventTime: 160, Value: 1} // > 水位线：正常
	if ok, _ := m.Add(onTime, wm); !ok {
		t.Fatal("正常事件应被接受")
	}
	lateIn := Event{ID: "b", Key: "k", EventTime: 140, Value: 1} // (130,150]：允许范围内迟到
	if ok, _ := m.Add(lateIn, wm); !ok {
		t.Fatal("允许范围内的迟到事件应被接受")
	}
	lateOut := Event{ID: "c", Key: "k", EventTime: 130, Value: 1} // <= 130：超出允许范围
	if ok, reason := m.Add(lateOut, wm); ok || reason != RejectLateBeyondAllowed {
		t.Fatalf("超范围迟到应被拒绝且原因为 %s, got ok=%v reason=%s", RejectLateBeyondAllowed, ok, reason)
	}
	t.Logf("判定依据: 水位线=%d 允许迟到=%d 下限=%d; ts=160 正常, ts=140 范围内迟到, ts=130 拒绝(%s)",
		wm, 20, wm-20, RejectLateBeyondAllowed)
}

// TestDuplicateNotDoubleCounted 相同事件 ID 重复投递只计入一次。
func TestDuplicateNotDoubleCounted(t *testing.T) {
	m := NewWindowManager(100, 0)
	ev := Event{ID: "x", Key: "k", EventTime: 10, Value: 5}
	if ok, _ := m.Add(ev, 0); !ok {
		t.Fatal("首次投递应被接受")
	}
	if ok, reason := m.Add(ev, 0); ok || reason != RejectDuplicate {
		t.Fatalf("重复投递应被拒绝且原因为 %s, got ok=%v reason=%s", RejectDuplicate, ok, reason)
	}
	res := m.Fire(Window{0, 100})
	if len(res) != 1 || res[0].Count != 1 || res[0].Sum != 5 {
		t.Fatalf("重复投递被重复计数: %+v", res)
	}
	t.Logf("幂等: 事件 x 投递两次, 窗口 count=1 sum=5, 与投递一次一致")
}

// TestFireOnlyAfterWatermarkPasses 窗口在水位线越过 end+allowedLateness 后才触发。
func TestFireOnlyAfterWatermarkPasses(t *testing.T) {
	m := NewWindowManager(100, 20)
	m.Add(Event{ID: "a", Key: "k", EventTime: 10, Value: 1}, 0)
	if got := m.Fireable(119); len(got) != 0 { // 100+20=120 才关闭
		t.Fatalf("水位线 119 不应触发窗口, got %v", got)
	}
	if got := m.Fireable(120); len(got) != 1 {
		t.Fatalf("水位线 120 应触发窗口, got %v", got)
	}
	t.Logf("窗口[0,100) 允许迟到=20: 水位线 119 不触发, 120 触发")
}
