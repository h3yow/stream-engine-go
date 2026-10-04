package engine

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// TestStatusSnapshot 验证运行期状态快照的各字段：
// 水位线、分片进展（含 idle/finished 区分）、待定稿窗口、已提交窗口数、
// 按原因分类的拒绝计数，且快照与后续判定结果一致。
func TestStatusSnapshot(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 2,
		SourcePartitions: 3,
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
	mustEnqueue(ep("c", 3, 7, 3, 2))
	drain(eng)                   // 先排空再标记，避免标记与在途事件处理交错
	eng.MarkPartitionIdle(2)     // 分片2 暂时空闲
	eng.MarkPartitionFinished(1) // 分片1 彻底不再供数
	mustEnqueue(ep("a", 21, 2, 4, 0))
	mustEnqueue(ep("b", 5, 99, 5, 1)) // 终结分片事件 → partition_finished
	mustEnqueue(ep("a", 3, 77, 6, 0)) // W[0,10) 已定稿 → late_beyond_allowed
	drain(eng)

	snap, err := eng.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[快照] 水位线=%ds 并行度=%d 待定稿=%d 已提交=%d",
		snap.Watermark.Unix(), snap.Parallelism, len(snap.PendingWindows), snap.CommittedWindows)
	for _, p := range snap.Partitions {
		t.Logf("[快照] 分片%d 状态=%s maxObserved=%ds 分片水位线=%ds",
			p.ID, p.Status, p.MaxObserved.Unix(), p.Watermark.Unix())
	}
	for _, w := range snap.PendingWindows {
		t.Logf("[快照] 待定稿 key=%s window=[%d,%d)s sum=%d count=%d 定稿线=%ds",
			w.Key, w.Start.Unix(), w.End.Unix(), w.Sum, w.Count, w.Deadline.Unix())
	}
	t.Logf("[快照] 拒绝分类=%v 统计=%+v", snap.Drops, snap.Stats)

	// 水位线：分片1/2 被排除后跟随分片0 推进到 21s。
	if snap.Watermark.Unix() != 21 {
		t.Fatalf("快照水位线期望21s，实际=%ds", snap.Watermark.Unix())
	}
	if snap.Parallelism != 2 {
		t.Fatalf("快照并行度期望2，实际=%d", snap.Parallelism)
	}
	// 分片状态：0=active、1=finished、2=idle，三种状态可区分。
	if len(snap.Partitions) != 3 {
		t.Fatalf("快照应含3个分片，实际=%d", len(snap.Partitions))
	}
	wantStatus := map[int]string{0: "active", 1: "finished", 2: "idle"}
	for _, p := range snap.Partitions {
		if p.Status != wantStatus[p.ID] {
			t.Fatalf("分片%d 状态期望=%s，实际=%s", p.ID, wantStatus[p.ID], p.Status)
		}
	}
	// 待定稿窗口：a 的 [20,30)（1条）；b 的 [0,10) 已随水位线定稿，c 的 [0,10) 也已定稿。
	if len(snap.PendingWindows) != 1 {
		t.Fatalf("快照待定稿窗口期望1个，实际=%d: %+v", len(snap.PendingWindows), snap.PendingWindows)
	}
	pw := snap.PendingWindows[0]
	if pw.Key != "a" || pw.Start.Unix() != 20 || pw.Count != 1 || pw.Sum != 2 ||
		pw.Deadline.Unix() != 35 {
		t.Fatalf("待定稿窗口字段异常: %+v", pw)
	}
	// 已提交窗口：a/b/c 的 [0,10) 共 3 个（b/c 虽来自 idle/finished 分片，定稿不受影响）。
	if snap.CommittedWindows != 3 {
		t.Fatalf("快照已提交窗口期望3，实际=%d", snap.CommittedWindows)
	}
	// 拒绝分类计数：两种原因各 1 条，可区分。
	if snap.Drops[event.ReasonPartitionFinished] != 1 ||
		snap.Drops[event.ReasonLateBeyondAllowed] != 1 {
		t.Fatalf("快照拒绝分类期望各1条，实际=%v", snap.Drops)
	}
	// Stats 与快照一致。
	if snap.Stats.PartitionFinishedDropped != 1 || snap.Stats.LateDropped != 1 {
		t.Fatalf("快照统计异常: %+v", snap.Stats)
	}
	// 快照与下游实际输出互证：已提交3窗口 = 下游已写3条。
	if snk.Written() != 3 {
		t.Fatalf("下游已写应与快照已提交一致（3），实际=%d", snk.Written())
	}

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// 关闭后快照不可用。
	if _, err := eng.Snapshot(context.Background()); err != ErrNotRunning {
		t.Fatalf("关闭后 Snapshot 应返回 ErrNotRunning，实际=%v", err)
	}
}

// TestSnapshotRestoreField 验证快照中的恢复信息字段反映检查点恢复情况。
func TestSnapshotRestoreField(t *testing.T) {
	store := newMemStoreWithCheckpoint(t)
	eng := New(Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: 64, Parallelism: 4,
	}, newTestSink(), store)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, err := eng.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[快照] 恢复信息=%+v", snap.Restore)
	if !snap.Restore.Restored || !snap.Restore.Rescaled ||
		snap.Restore.FromParallelism != 2 || snap.Restore.ToParallelism != 4 {
		t.Fatalf("快照恢复信息异常: %+v", snap.Restore)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

// newTestSink 构造无写延迟的收集型下游。
func newTestSink() *sink.CollectingSink { return sink.NewCollectingSink(0) }

// newMemStoreWithCheckpoint 构造预置一个并行度2检查点的内存存储。
func newMemStoreWithCheckpoint(t *testing.T) *checkpoint.MemoryStore {
	t.Helper()
	store := checkpoint.NewMemoryStore()
	cp := &checkpoint.Checkpoint{
		ID: 1, Version: checkpoint.CurrentVersion, Parallelism: 2,
		Watermark: et(5), MaxSeq: 3,
	}
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	return store
}
