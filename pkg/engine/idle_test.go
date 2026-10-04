package engine

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// ep 构造一条带数据源分片标识的事件。
func ep(key string, sec, value, seq, partition int) event.Event {
	return event.Event{Key: key, Timestamp: et(sec), Value: int64(value), Seq: int64(seq), Partition: partition}
}

func logSnapshotLite(t *testing.T, eng *Engine, tag string) {
	t.Helper()
	snap, err := eng.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot 失败: %v", err)
	}
	t.Logf("[%s] 水位线=%ds 待定稿窗口=%d 已提交窗口=%d", tag,
		snap.Watermark.Unix(), len(snap.PendingWindows), snap.CommittedWindows)
	for _, p := range snap.Partitions {
		t.Logf("           分片%d 状态=%s maxObserved=%ds", p.ID, p.Status, p.MaxObserved.Unix())
	}
	for _, w := range snap.PendingWindows {
		t.Logf("           待定稿 key=%s window=[%d,%d)s count=%d 定稿线=%ds",
			w.Key, w.Start.Unix(), w.End.Unix(), w.Count, w.Deadline.Unix())
	}
}

// TestIdlePartitionWatermarkAdvance 验证：某数据源分片长时间无数据被标记空闲后，
// 全局水位线跟随活跃分片继续推进，该定稿的窗口照常定稿；
// 空闲分片恢复供数后，允许迟到范围内的事件正常计入、超范围的明确拒绝。
func TestIdlePartitionWatermarkAdvance(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 2,
		SourcePartitions: 2,
	}
	snk, eng := newTestEngine(t, cfg)
	mustEnqueue := func(ev event.Event) {
		t.Helper()
		if !eng.Enqueue(ev) {
			t.Fatalf("Enqueue 失败: %+v", ev)
		}
	}

	// 两个分片都在供数：水位线 = min(分片0, 分片1)。
	mustEnqueue(ep("a", 1, 10, 1, 0))
	mustEnqueue(ep("b", 2, 5, 2, 1))
	mustEnqueue(ep("a", 11, 1, 3, 0))
	drain(eng)
	t.Logf("[水位线判定] 分片0@11s 分片1@2s → 水位线=%ds（被分片1拖住）", eng.Watermark().Unix())
	if wm := eng.Watermark().Unix(); wm != 2 {
		t.Fatalf("分片1沉默应拖住水位线在2s，实际=%ds", wm)
	}

	// 分片1 标记空闲：水位线应跟随分片0 推进，W[0,10)（含分片1的key b）照常定稿。
	eng.MarkPartitionIdle(1)
	mustEnqueue(ep("a", 21, 2, 4, 0))
	drain(eng)
	logSnapshotLite(t, eng, "分片1空闲后")
	if wm := eng.Watermark().Unix(); wm != 21 {
		t.Fatalf("分片1空闲后水位线应跟分片0到21s，实际=%ds", wm)
	}
	if w := snk.Written(); w != 2 {
		t.Fatalf("W[0,10) 两个key应已定稿输出2条，实际=%d", w)
	}
	t.Logf("[窗口判定] 水位线21s >= 定稿线15s → a/b 的 [0,10) 定稿（空闲分片不再拖累）")

	// 分片1 恢复供数：允许迟到范围内（定稿线25s未到）正常计入。
	mustEnqueue(ep("b", 16, 7, 5, 1))
	// 超范围迟到（W[0,10) 已定稿）：明确拒绝，原因可区分。
	mustEnqueue(ep("b", 4, 99, 6, 1))
	drain(eng) // 先让分片1恢复活跃，再推进分片0，水位线才会被分片1@16s拖住
	mustEnqueue(ep("a", 31, 3, 7, 0))
	drain(eng)
	if wm := eng.Watermark().Unix(); wm != 21 {
		t.Fatalf("分片1恢复到16s后水位线不回退，应保持21s，实际=%ds", wm)
	}
	// 分片1 追平后，水位线继续推进，W[10,20) 定稿。
	mustEnqueue(ep("b", 26, 1, 8, 1))
	drain(eng)
	logSnapshotLite(t, eng, "分片1追平后")
	if wm := eng.Watermark().Unix(); wm != 26 {
		t.Fatalf("分片1追平到26s后水位线应=26s，实际=%ds", wm)
	}
	if w := snk.Written(); w != 4 {
		t.Fatalf("W[10,20) 两个key应已定稿，累计输出4条，实际=%d", w)
	}

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(snk, eng)
	logResults(t, out, st, drops)
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "b", start: 0}:  {sum: 5, count: 1, seqs: []int64{2}},
		{key: "a", start: 10}: {sum: 1, count: 1, seqs: []int64{3}},
		{key: "b", start: 10}: {sum: 7, count: 1, seqs: []int64{5}}, // 空闲恢复后范围内迟到计入
		{key: "a", start: 20}: {sum: 2, count: 1, seqs: []int64{4}},
		{key: "b", start: 20}: {sum: 1, count: 1, seqs: []int64{8}},
		{key: "a", start: 30}: {sum: 3, count: 1, seqs: []int64{7}},
	})
	assertAggsEqual(t, want, out)
	if st.LateDropped != 1 || st.PartitionFinishedDropped != 0 {
		t.Fatalf("期望1条超范围迟到拒绝、0条终结分片拒绝，实际 late=%d finished=%d",
			st.LateDropped, st.PartitionFinishedDropped)
	}
	if len(drops) != 1 || drops[0].Event.Seq != 6 ||
		drops[0].Reason != event.ReasonLateBeyondAllowed {
		t.Fatalf("拒绝明细期望 seq=6 late_beyond_allowed，实际 %+v", drops)
	}
	if dc := eng.DropCounts(); dc[event.ReasonLateBeyondAllowed] != 1 {
		t.Fatalf("DropCounts 应按原因计数1条迟到拒绝，实际 %+v", dc)
	}
}

// TestFinishedPartitionRejected 验证“彻底不再供数”与“暂时空闲”的区别：
// 终结分片被永久排除在水位线之外，其后续事件一律以 partition_finished 拒绝，
// 且不会因新事件复活。
func TestFinishedPartitionRejected(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 2,
		SourcePartitions: 2,
	}
	snk, eng := newTestEngine(t, cfg)
	mustEnqueue := func(ev event.Event) {
		t.Helper()
		if !eng.Enqueue(ev) {
			t.Fatalf("Enqueue 失败: %+v", ev)
		}
	}

	mustEnqueue(ep("a", 1, 10, 1, 0))
	mustEnqueue(ep("b", 2, 5, 2, 1))
	drain(eng)

	// 分片1 彻底不再供数：水位线跟分片0 推进，W[0,10) 定稿。
	eng.MarkPartitionFinished(1)
	mustEnqueue(ep("a", 21, 2, 3, 0))
	drain(eng)
	logSnapshotLite(t, eng, "分片1终结后")
	if w := snk.Written(); w != 2 {
		t.Fatalf("分片1终结后 W[0,10) 应定稿输出2条，实际=%d", w)
	}

	// 终结分片的后续事件：无论落在哪个窗口，一律 partition_finished 拒绝。
	mustEnqueue(ep("b", 5, 99, 4, 1))  // 窗口已定稿
	mustEnqueue(ep("b", 55, 88, 5, 1)) // 窗口尚未定稿，也同样拒绝
	drain(eng)
	st := eng.Stats()
	if st.PartitionFinishedDropped != 2 {
		t.Fatalf("终结分片事件应拒绝2条，实际=%d", st.PartitionFinishedDropped)
	}
	if st.LateDropped != 0 {
		t.Fatalf("终结分片拒绝不应计入迟到拒绝，实际 late=%d", st.LateDropped)
	}
	dc := eng.DropCounts()
	if dc[event.ReasonPartitionFinished] != 2 {
		t.Fatalf("DropCounts 应有2条 partition_finished，实际 %+v", dc)
	}
	for _, d := range eng.Drops() {
		if d.Reason != event.ReasonPartitionFinished {
			t.Fatalf("拒绝原因应为 partition_finished，实际 %+v", d)
		}
	}
	t.Logf("[逐条判定] seq=4/5 来自已终结分片1 → 拒绝，原因=partition_finished（与迟到拒绝区分）")

	// 水位线不受终结分片事件影响（55s 不应推进水位线）。
	if wm := eng.Watermark().Unix(); wm != 21 {
		t.Fatalf("终结分片事件不应影响水位线，期望21s，实际=%ds", wm)
	}

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(snk, eng)
	logResults(t, out, st, drops)
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "b", start: 0}:  {sum: 5, count: 1, seqs: []int64{2}},
		{key: "a", start: 20}: {sum: 2, count: 1, seqs: []int64{3}},
	})
	assertAggsEqual(t, want, out)
	if st.Accepted != 3 {
		t.Fatalf("终结分片事件不应计入聚合，期望计入3条，实际=%d", st.Accepted)
	}
}

// TestPartitionIdleTimeout 验证配置空闲超时后，沉默分片被自动视为空闲，
// 无需显式标记，水位线即可跟随活跃分片推进。
func TestPartitionIdleTimeout(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 1,
		PartitionIdleTimeout: 50 * time.Millisecond, SourcePartitions: 2,
	}
	snk, eng := newTestEngine(t, cfg)
	mustEnqueue := func(ev event.Event) {
		t.Helper()
		if !eng.Enqueue(ev) {
			t.Fatalf("Enqueue 失败: %+v", ev)
		}
	}
	mustEnqueue(ep("a", 1, 10, 1, 0))
	mustEnqueue(ep("b", 2, 5, 2, 1))
	drain(eng) // drain 休眠100ms > 超时50ms：分片1 在下次观测时被自动标记空闲
	mustEnqueue(ep("a", 21, 2, 3, 0))
	drain(eng)
	if wm := eng.Watermark().Unix(); wm != 21 {
		t.Fatalf("分片1超时自动空闲后水位线应到21s，实际=%ds", wm)
	}
	if w := snk.Written(); w != 2 {
		t.Fatalf("W[0,10) 应已定稿输出2条，实际=%d", w)
	}
	t.Logf("[水位线判定] 分片1沉默超50ms自动idle → 分片0@21s → 水位线=21s，W[0,10)定稿")
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPartitionStateRestored 验证分片的 idle/finished 标记随检查点持久化：
// 崩溃恢复后终结分片依然终结（事件继续被拒）、水位线状态不回退。
func TestPartitionStateRestored(t *testing.T) {
	cfg := func() Config {
		return Config{
			WindowSize: winSize, MaxOutOfOrderness: 0,
			AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 2,
			SourcePartitions: 2,
		}
	}
	store := checkpoint.NewMemoryStore()
	persistentSink := sink.NewCollectingSink(0)

	// 第一次运行：分片1 终结后做检查点，再崩溃。
	eng1 := New(cfg(), persistentSink, store)
	if err := eng1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng1.Enqueue(ep("a", 1, 10, 1, 0))
	eng1.Enqueue(ep("b", 2, 5, 2, 1))
	drain(eng1)
	eng1.MarkPartitionFinished(1)
	eng1.Enqueue(ep("a", 21, 2, 3, 0))
	drain(eng1)
	cp, err := eng1.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[检查点] watermark=%ds 分片状态=%+v", cp.Watermark.Unix(), cp.Partitions)
	var p1Status string
	for _, p := range cp.Partitions {
		if p.ID == 1 {
			p1Status = p.Status
		}
	}
	if p1Status != "finished" {
		t.Fatalf("检查点应记录分片1为finished，实际=%q", p1Status)
	}
	eng1.Abort()

	// 恢复：分片1 仍是终结状态，其事件继续以 partition_finished 拒绝。
	eng2 := New(cfg(), persistentSink, store)
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, err := eng2.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range snap.Partitions {
		t.Logf("[恢复后快照] 分片%d 状态=%s maxObserved=%ds", p.ID, p.Status, p.MaxObserved.Unix())
		if p.ID == 1 && p.Status != "finished" {
			t.Fatalf("恢复后分片1应保持finished，实际=%s", p.Status)
		}
	}
	eng2.Enqueue(ep("b", 5, 99, 4, 1)) // 重放的终结分片事件 → 拒绝
	eng2.Enqueue(ep("a", 31, 3, 5, 0))
	drain(eng2)
	if st := eng2.Stats(); st.PartitionFinishedDropped != 1 {
		t.Fatalf("恢复后终结分片事件应拒绝1条，实际=%d", st.PartitionFinishedDropped)
	}
	if wm := eng2.Watermark().Unix(); wm != 31 {
		t.Fatalf("恢复后水位线应跟分片0到31s，实际=%ds", wm)
	}
	t.Logf("[恢复] 分片1终结状态随检查点恢复，其事件继续被拒；水位线=%ds", eng2.Watermark().Unix())
	if err := eng2.Close(); err != nil {
		t.Fatal(err)
	}
}
