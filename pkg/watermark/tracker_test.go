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

	if wm, _ := tr.Observe("p1", ts(30), 1); wm != ts(30) {
		t.Fatalf("首个分片观测后水位线期望30s，实际 %v", wm)
	}
	if wm, _ := tr.Observe("p0", ts(50), 2); wm != ts(30) {
		t.Fatalf("全局水位线应取 min(p0=50, p1=30)=30s，实际 %v", wm)
	}
	if wm, _ := tr.Observe("p1", ts(60), 3); wm != ts(50) {
		t.Fatalf("p1 追上后全局水位线应为 min(50,60)=50s，实际 %v", wm)
	}
	t.Logf("[水位线判定] p0=50s p1=60s → 全局水位线=50s（取 active 分片最小值，单调不减）")
}

// TestTrackerIdleExcluded 分片空闲后被排除，全局水位线随其余分片推进；
// 空闲分片恢复供数后自动重新参与，且全局水位线不回退。
func TestTrackerIdleExcluded(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, 10*time.Second, clk.Now)

	tr.Observe("p1", ts(90), 1)
	tr.Observe("p0", ts(100), 2)
	if wm := tr.Current(); wm != ts(90) {
		t.Fatalf("初始全局水位线期望 min(100,90)=90s，实际 %v", wm)
	}

	// p1 超过空闲阈值未供数 → 被排除，水位线推进到 p0 的 100s。
	clk.advance(11 * time.Second)
	tr.Observe("p0", ts(120), 3)
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
	if wm, ok := tr.Observe("p1", ts(95), 4); !ok || wm != ts(120) {
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
	tr.Observe("p0", ts(42), 1)
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
	tr.Observe("p0", ts(100), 1)
	tr.Observe("p1", ts(80), 2)

	tr.Finish("p1")
	if wm := tr.Current(); wm != ts(100) {
		t.Fatalf("p1 终止后水位线应由 p0 决定为100s，实际 %v", wm)
	}
	if _, ok := tr.Observe("p1", ts(200), 3); ok {
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
	if _, ok := tr.Observe("ghost", ts(1), 9); ok {
		t.Fatal("预先终止的分片不得接受事件")
	}
}

// TestTrackerFinishCut 终止位点语义：序号不越过终止位点的事件
// （检查点恢复后的重放）允许重新计入，越过位点的必须拒绝。
func TestTrackerFinishCut(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr.Observe("p0", ts(100), 1)
	tr.Observe("p1", ts(80), 5)
	tr.Observe("p1", ts(90), 7) // p1 终止前最大序号为7
	tr.Finish("p1")

	// 终止后新到事件（序号越过位点）→ 拒绝。
	if _, ok := tr.Observe("p1", ts(95), 8); ok {
		t.Fatal("序号越过终止位点的事件必须拒绝")
	}
	// 恢复重放的终止前事件（序号不越过位点）→ 允许重新计入，但分片仍 finished。
	if _, ok := tr.Observe("p1", ts(80), 5); !ok {
		t.Fatal("重放的终止前事件（seq<=finishSeq）必须允许重新计入")
	}
	if _, ok := tr.Observe("p1", ts(90), 7); !ok {
		t.Fatal("序号恰等于终止位点的重放事件必须允许重新计入")
	}
	for _, info := range tr.Partitions() {
		if info.ID == "p1" {
			if info.Status != StatusFinished {
				t.Fatalf("重放接受不改变终止状态，实际 %s", info.Status)
			}
			if !info.HasFinishSeq || info.FinishSeq != 7 {
				t.Fatalf("终止位点期望7，实际 %+v", info)
			}
		}
	}
	t.Logf("[终止位点] p1 finishSeq=7：seq=8 拒绝；重放 seq=5/7 允许重新计入且分片保持 finished")
}

// TestTrackerRestoreFinished 已终止分片（含从未供数的）恢复后必须保持终止：
// 不变回正常供数、不参与水位线、位点之后的事件继续拒绝。
func TestTrackerRestoreFinished(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr.Restore(map[string]PartitionSnapshot{
		"p0": {MaxObserved: ts(100)},                                                  // 正常分片
		"p1": {MaxObserved: ts(80), Finished: true, FinishSeq: 7, HasFinishSeq: true}, // 有数据的终止分片
		"p2": {Finished: true},                                                        // 从未供数就被停掉的分片
	}, ts(75))

	st := map[string]PartitionStatus{}
	for _, info := range tr.Partitions() {
		st[info.ID] = info.Status
	}
	if st["p0"] != StatusActive || st["p1"] != StatusFinished || st["p2"] != StatusFinished {
		t.Fatalf("恢复后分片状态期望 active/finished/finished，实际 %v", st)
	}
	// 已终止分片不参与水位线：全局水位线只由 p0 决定。
	if wm := tr.Current(); wm != ts(100) {
		t.Fatalf("恢复后水位线应由 p0 决定为100s，实际 %v", wm)
	}
	// p1：位点之后拒绝、位点之内允许重放计入。
	if _, ok := tr.Observe("p1", ts(85), 8); ok {
		t.Fatal("恢复后 p1 位点之后的事件必须拒绝")
	}
	if _, ok := tr.Observe("p1", ts(70), 6); !ok {
		t.Fatal("恢复后 p1 位点之内的重放事件必须允许重新计入")
	}
	// p2：从未供数就被停掉，恢复后任何事件都拒绝。
	if _, ok := tr.Observe("p2", ts(50), 1); ok {
		t.Fatal("从未供数的终止分片恢复后任何事件都必须拒绝")
	}
	t.Logf("[恢复终止] p1(finishSeq=7)/p2(空) 恢复后保持 finished：拒绝位点后事件，不复活、不参与水位线")
}

// TestTrackerRestore 恢复后水位线不回退，分片进展从快照继续。
func TestTrackerRestore(t *testing.T) {
	clk := &testClock{now: ts(1000)}
	tr := NewTrackerWithClock(0, time.Minute, clk.Now)
	tr.Restore(map[string]PartitionSnapshot{
		"p0": {MaxObserved: ts(100)},
		"p1": {MaxObserved: ts(90)},
	}, ts(85))

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
