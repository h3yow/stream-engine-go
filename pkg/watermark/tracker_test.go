package watermark

import (
	"testing"
	"time"
)

// testClock 是可手动推进的处理时间时钟。
type testClock struct{ now time.Time }

func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }
func (c *testClock) Now() time.Time          { return c.now }

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

// TestTrackerMinAcrossPartitions 全局水位线取各 active 分片水位线的最小值，
// 且满足单调不减（新出现的分片不会把已推进的水位线拉回）。
func TestTrackerMinAcrossPartitions(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)

	if wm, _ := tr.Observe("p1", ts(30)); wm != ts(30) {
		t.Fatalf("首个分片观测后水位线期望30s，实际 %v", wm)
	}
	if wm, _ := tr.Observe("p0", ts(50)); wm != ts(30) {
		t.Fatalf("全局水位线应取 min(p0=50, p1=30)=30s，实际 %v", wm)
	}
	if wm, _ := tr.Observe("p1", ts(60)); wm != ts(50) {
		t.Fatalf("p1 追上后全局水位线应为 min(50,60)=50s，实际 %v", wm)
	}
	t.Logf("[水位线判定] p0=50s p1=60s → 全局水位线=50s（取 active 分片最小值，单调不减）")
}

// TestTrackerIdleExcluded 分片空闲后被排除，全局水位线随其余分片推进；
// 空闲分片恢复供数后自动重新参与，且全局水位线不回退。
func TestTrackerIdleExcluded(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, 10*time.Second, clk.Now)

	tr.Observe("p1", ts(90))
	tr.Observe("p0", ts(100))
	if wm := tr.Current(); wm != ts(90) {
		t.Fatalf("初始全局水位线期望 min(100,90)=90s，实际 %v", wm)
	}

	// p1 超过空闲阈值未供数 → 被排除，水位线推进到 p0 的 100s。
	clk.advance(11 * time.Second)
	tr.Observe("p0", ts(120))
	if wm := tr.Current(); wm != ts(120) {
		t.Fatalf("p1 空闲后水位线应跟随 p0 到120s，实际 %v", wm)
	}
	infos := tr.Partitions()
	st := map[string]PartitionStatus{}
	for _, info := range infos {
		st[info.ID] = info.Status
	}
	if st["p1"] != StatusIdle || st["p0"] != StatusActive {
		t.Fatalf("分片状态期望 p0=active p1=idle，实际 %v", st)
	}
	t.Logf("[空闲判定] p1 超过阈值未供数 → idle 被排除；水位线随 p0 推进到120s")

	// p1 恢复供数：带回较旧的事件时间，重新参与 min，但全局水位线不回退。
	clk.advance(time.Second)
	if wm, ok := tr.Observe("p1", ts(95)); !ok || wm != ts(120) {
		t.Fatalf("p1 恢复后水位线不应回退：期望120s ok=true，实际 %v ok=%v", wm, ok)
	}
	if st := tr.Partitions()[1].Status; st != StatusActive {
		t.Fatalf("p1 恢复供数后应回到 active，实际 %s", st)
	}
	t.Logf("[空闲恢复] p1 重新供数 → active；其旧水位线95s 参与 min，但全局水位线保持120s 不回退")
}

// TestTrackerAllIdleWatermarkStalls 所有分片都空闲时水位线停在最近值。
func TestTrackerAllIdleWatermarkStalls(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, 5*time.Second, clk.Now)
	tr.Observe("p0", ts(42))
	clk.advance(time.Minute)
	if wm := tr.Current(); wm != ts(42) {
		t.Fatalf("全部空闲时水位线应保持42s，实际 %v", wm)
	}
	t.Logf("[全空闲] 无 active 分片 → 水位线保持42s 不再推进")
}

// TestTrackerFinish 终止分片永久排除且拒绝后续事件；与 idle 表现不同。
func TestTrackerFinish(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr.Observe("p0", ts(100))
	tr.Observe("p1", ts(80))

	tr.Finish("p1")
	if wm := tr.Current(); wm != ts(100) {
		t.Fatalf("p1 终止后水位线应由 p0 决定为100s，实际 %v", wm)
	}
	if _, ok := tr.Observe("p1", ts(200)); ok {
		t.Fatal("已终止分片的事件必须被拒绝（ok=false）")
	}
	var p1 PartitionInfo
	for _, info := range tr.Partitions() {
		if info.ID == "p1" {
			p1 = info
		}
	}
	if p1.Status != StatusFinished {
		t.Fatalf("p1 状态期望 finished，实际 %s", p1.Status)
	}
	t.Logf("[终止判定] p1 finished → 永久排除；其后续事件被拒绝，与 idle 可恢复不同")

	// 对从未观测过的分片提前 Finish 同样生效。
	tr.Finish("ghost")
	if _, ok := tr.Observe("ghost", ts(1)); ok {
		t.Fatal("预先终止的分片不得接受事件")
	}
}

// TestTrackerRestore 恢复后水位线不回退，分片进展从快照继续。
func TestTrackerRestore(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr.Restore(map[string]time.Time{"p0": ts(100), "p1": ts(90)}, ts(85))

	if wm := tr.Current(); wm != ts(90) {
		t.Fatalf("恢复后水位线应取 min(100,90)=90s，实际 %v", wm)
	}
	// 恢复快照水位线兜底：即使分片信息缺失也不回退。
	tr2 := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr2.Restore(nil, ts(85))
	if wm := tr2.Current(); wm != ts(85) {
		t.Fatalf("无分片信息时水位线应由快照兜底为85s，实际 %v", wm)
	}
	t.Logf("[恢复] 分片进展 {p0:100s,p1:90s} + 快照水位线85s → 恢复后水位线90s")
}
