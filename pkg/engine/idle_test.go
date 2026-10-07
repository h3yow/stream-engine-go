package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// ep 构造带数据源分片标识的事件。
func ep(key, partition string, sec, value, seq int) event.Event {
	return event.Event{Key: key, Timestamp: et(sec), Value: int64(value), Seq: int64(seq), Partition: partition}
}

// waitFor 轮询条件直到满足或超时（用于异步处理断言）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待条件超时: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func logSnapshot(t *testing.T, snap Snapshot, tag string) {
	t.Helper()
	t.Logf("[快照:%s] 水位线=%ds maxSeq=%d 待定时窗口=%d 已定稿窗口=%d",
		tag, snap.Watermark.Unix(), snap.MaxSeq, len(snap.PendingWindows), len(snap.CommittedWindows))
	for _, p := range snap.Partitions {
		maxObs, partWM := "-", "-"
		if !p.MaxObserved.IsZero() {
			maxObs = fmt.Sprintf("%ds", p.MaxObserved.Unix())
		}
		if !p.Watermark.IsZero() {
			partWM = fmt.Sprintf("%ds", p.Watermark.Unix())
		}
		if p.Status == watermark.StatusFinished {
			t.Logf("[快照:%s] 分片=%s 状态=%s 最大事件时间=%s 终止位点finishSeq=%d(有效=%v)",
				tag, p.ID, p.Status, maxObs, p.FinishSeq, p.HasFinishSeq)
		} else {
			t.Logf("[快照:%s] 分片=%s 状态=%s 分片水位线=%s 最大事件时间=%s",
				tag, p.ID, p.Status, partWM, maxObs)
		}
	}
	for _, w := range snap.PendingWindows {
		t.Logf("[快照:%s] 待定 key=%s window=[%d,%d)s", tag, w.Key, w.Start.Unix(), w.End.Unix())
	}
	for _, w := range snap.CommittedWindows {
		t.Logf("[快照:%s] 定稿 key=%s window=[%d,%d)s", tag, w.Key, w.Start.Unix(), w.End.Unix())
	}
	t.Logf("[快照:%s] 拒绝计数=%v", tag, snap.Drops)
}

// TestIdlePartitionWatermarkAdvances 某数据源分片长期不供数时：
//   - 超过 IdleTimeout 后该分片被判定 idle，从全局水位线计算中排除；
//   - 全局水位线随正常供数的分片继续推进，到期窗口照常定稿；
//   - 空闲分片恢复供数后：允许迟到范围内的事件正常计入、重复投递去重、
//     超出范围的按 late_beyond_allowed_lateness 明确拒绝。
func TestIdlePartitionWatermarkAdvances(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128,
		Parallelism: 2, IdleTimeout: 200 * time.Millisecond,
	}
	snk, eng := newTestEngine(t, cfg)

	// 两个数据源分片：slow 只发一条后沉默，fast 持续供数。
	// 首条等待处理完成，保证 slow 先注册（晚注册的分片无法拉回已推进的水位线）。
	eng.Enqueue(ep("s", "slow", 1, 10, 1)) // slow: W0
	waitFor(t, "slow 分片注册", func() bool { return eng.Stats().Received == 1 })
	eng.Enqueue(ep("f", "fast", 1, 1, 2))  // fast: W0
	eng.Enqueue(ep("f", "fast", 10, 1, 3)) // fast 推进，但全局水位线被 slow(1s) 拖在 1s
	waitFor(t, "fast 前两条处理完成", func() bool { return eng.Stats().Received == 3 })
	if wm := eng.Watermark(); wm != et(1) {
		t.Fatalf("slow 沉默期间全局水位线应被拖在1s，实际 %ds", wm.Unix())
	}
	t.Logf("[水位线判定] slow 分片停在1s → 全局水位线=1s，W0(定稿线15s) 等待")

	// slow 超过 IdleTimeout 未供数 → idle 被排除；fast 再供数时水位线跟随推进。
	time.Sleep(300 * time.Millisecond)
	eng.Enqueue(ep("f", "fast", 20, 1, 4)) // 水位线→20s：W0(定稿线15s) 定稿
	eng.Enqueue(ep("f", "fast", 30, 1, 5)) // 水位线→30s：W1(定稿线25s) 定稿
	waitFor(t, "W0/W1 在 slow 空闲期间定稿", func() bool { return snk.Written() >= 3 })

	snap := eng.Snapshot()
	logSnapshot(t, snap, "slow空闲")
	slowStatus := ""
	for _, p := range snap.Partitions {
		if p.ID == "slow" {
			slowStatus = string(p.Status)
		}
	}
	if slowStatus != string(watermark.StatusIdle) {
		t.Fatalf("slow 分片应被判定 idle，实际 %s", slowStatus)
	}
	if wm := eng.Watermark(); wm != et(30) {
		t.Fatalf("slow 空闲后水位线应跟随 fast 到30s，实际 %ds", wm.Unix())
	}
	t.Logf("[空闲判定] slow 超过200ms未供数 → idle 排除；水位线随 fast 推进到30s，W0/W1 照常定稿")

	// slow 恢复供数：
	//   seq=6 ts=22 → W2(定稿线35s) 未定稿 → 正常计入；
	//   seq=7 ts=8  → W0 已定稿 → 拒绝 late_beyond_allowed_lateness；
	//   seq=6 重投  → 重复去重跳过。
	eng.Enqueue(ep("s", "slow", 22, 100, 6))
	eng.Enqueue(ep("s", "slow", 8, 999, 7))
	eng.Enqueue(ep("s", "slow", 22, 100, 6))
	drain(eng)
	logDecisions(t, eng, []event.Event{
		ep("s", "slow", 22, 100, 6), ep("s", "slow", 8, 999, 7), ep("s", "slow", 22, 100, 6),
	})
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(snk, eng)
	logResults(t, out, st, drops)

	want := expectedSet(map[resultKey]expectedAgg{
		{key: "s", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "f", start: 0}:  {sum: 1, count: 1, seqs: []int64{2}},
		{key: "f", start: 10}: {sum: 1, count: 1, seqs: []int64{3}},
		{key: "f", start: 20}: {sum: 1, count: 1, seqs: []int64{4}},
		{key: "s", start: 20}: {sum: 100, count: 1, seqs: []int64{6}},
		{key: "f", start: 30}: {sum: 1, count: 1, seqs: []int64{5}},
	})
	assertAggsEqual(t, want, out)
	if st.LateDropped != 1 || st.DuplicateSkipped != 1 {
		t.Fatalf("期望迟到拒绝1、重复跳过1，实际 late=%d dup=%d", st.LateDropped, st.DuplicateSkipped)
	}
	for _, d := range drops {
		if d.Reason != event.ReasonLateBeyondAllowed || d.Event.Seq != 7 {
			t.Fatalf("拒绝明细期望 seq=7 late_beyond_allowed_lateness，实际 %+v", d)
		}
	}
}

// TestFinishedPartitionRejected 显式宣告终止的分片与暂时空闲表现不同：
//   - finished 不可逆：分片永久排除出水位线计算；
//   - 其后续事件一律以 partition_finished 拒绝（与迟到/背压原因可区分）；
//   - 终止前已计入的数据不受影响。
func TestFinishedPartitionRejected(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: 128,
		Parallelism: 1, IdleTimeout: time.Hour, // 空闲检测形同关闭，隔离变量
	}
	snk, eng := newTestEngine(t, cfg)

	eng.Enqueue(ep("a", "pa", 1, 1, 1))
	eng.Enqueue(ep("b", "pb", 2, 20, 2)) // pb 终止前计入 W0
	waitFor(t, "终止前两条事件处理完成", func() bool { return eng.Stats().Received == 2 })

	eng.FinishPartition("pb")
	eng.Enqueue(ep("b", "pb", 15, 99, 3)) // 已终止分片 → 拒绝 partition_finished
	eng.Enqueue(ep("a", "pa", 10, 1, 4))  // 水位线→10s：W0 定稿
	waitFor(t, "W0 定稿", func() bool { return snk.Written() >= 1 })

	snap := eng.Snapshot()
	logSnapshot(t, snap, "pb终止")
	pbStatus := ""
	for _, p := range snap.Partitions {
		if p.ID == "pb" {
			pbStatus = string(p.Status)
		}
	}
	if pbStatus != string(watermark.StatusFinished) {
		t.Fatalf("pb 应被判定 finished，实际 %s", pbStatus)
	}
	if got := snap.Drops[event.ReasonPartitionFinished]; got != 1 {
		t.Fatalf("partition_finished 拒绝计数期望1，实际 %d", got)
	}
	t.Logf("[终止判定] pb finished → 永久排除；seq=3 以 partition_finished 拒绝，与 idle 可恢复不同")

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(snk, eng)
	logResults(t, out, st, drops)
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 1, count: 1, seqs: []int64{1}},
		{key: "b", start: 0}:  {sum: 20, count: 1, seqs: []int64{2}},
		{key: "a", start: 10}: {sum: 1, count: 1, seqs: []int64{4}},
	})
	assertAggsEqual(t, want, out)
	if st.PartitionFinishedDropped != 1 || st.LateDropped != 0 {
		t.Fatalf("期望 partition_finished 拒绝1、迟到拒绝0，实际 pf=%d late=%d",
			st.PartitionFinishedDropped, st.LateDropped)
	}
	if len(drops) != 1 || drops[0].Reason != event.ReasonPartitionFinished || drops[0].Event.Seq != 3 {
		t.Fatalf("拒绝明细期望 seq=3 partition_finished，实际 %+v", drops)
	}
}
