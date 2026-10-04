package engine

import (
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// TestSnapshotFields 验证运行期状态快照的字段含义：
// 水位线、各分片进展与空闲判定、待定/已定稿窗口清单、按原因的拒绝计数。
func TestSnapshotFields(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128,
		Parallelism: 2, IdleTimeout: 200 * time.Millisecond,
	}
	snk, eng := newTestEngine(t, cfg)

	// p0 正常供数推进水位线；p1 一条后沉默（将转入 idle）。
	// 逐条等待处理完成，保证分片注册顺序确定（晚注册的分片无法拉回已推进的水位线）。
	eng.Enqueue(ep("a", "p0", 1, 10, 1))
	eng.Enqueue(ep("b", "p1", 2, 20, 2))
	waitFor(t, "前两条事件处理完成", func() bool { return eng.Stats().Received == 2 })
	eng.Enqueue(ep("a", "p0", 10, 1, 3))
	waitFor(t, "第三条事件处理完成", func() bool { return eng.Stats().Received == 3 })

	// 快照1：p1 尚未超时，水位线被拖在 2s；a 的 W0/W1 与 b 的 W0 共三个窗口待定。
	snap1 := eng.Snapshot()
	logSnapshot(t, snap1, "运行中-p1未空闲")
	if snap1.Watermark != et(2) {
		t.Fatalf("快照水位线期望2s，实际 %ds", snap1.Watermark.Unix())
	}
	if len(snap1.PendingWindows) != 3 || len(snap1.CommittedWindows) != 0 {
		t.Fatalf("期望3个待定窗口、0个已定稿，实际 pending=%d committed=%d",
			len(snap1.PendingWindows), len(snap1.CommittedWindows))
	}
	if snap1.MaxSeq != 3 {
		t.Fatalf("快照 maxSeq 期望3，实际 %d", snap1.MaxSeq)
	}
	for _, reason := range []event.DropReason{
		event.ReasonLateBeyondAllowed, event.ReasonBackpressureFull, event.ReasonPartitionFinished,
	} {
		if _, ok := snap1.Drops[reason]; !ok {
			t.Fatalf("Drops 必须固定包含原因 %s", reason)
		}
	}

	// p1 超时转入 idle：水位线随 p0 推进，W0(定稿线15s) 定稿。
	time.Sleep(300 * time.Millisecond)
	eng.Enqueue(ep("a", "p0", 20, 1, 4)) // 水位线→20s：W0 定稿
	waitFor(t, "W0 两 key 定稿", func() bool { return snk.Written() >= 2 })

	// 制造一条超范围迟到：W0 已定稿，p1 回来补 ts=3 的旧事件。
	eng.Enqueue(ep("b", "p1", 3, 99, 5))
	drain(eng)

	// 快照2：p1 已恢复 active；W0 已定稿；W1/W2 待定；迟到拒绝计数=1。
	snap2 := eng.Snapshot()
	logSnapshot(t, snap2, "p1空闲后恢复")
	if snap2.Watermark != et(20) {
		t.Fatalf("快照水位线期望20s，实际 %ds", snap2.Watermark.Unix())
	}
	if len(snap2.CommittedWindows) != 2 {
		t.Fatalf("期望2个已定稿窗口(W0×2key)，实际 %d", len(snap2.CommittedWindows))
	}
	// 待定：a 的 W1、W2（b 的迟到事件未开窗）。
	if len(snap2.PendingWindows) != 2 {
		t.Fatalf("期望2个待定窗口，实际 %d", len(snap2.PendingWindows))
	}
	if got := snap2.Drops[event.ReasonLateBeyondAllowed]; got != 1 {
		t.Fatalf("快照迟到拒绝计数期望1，实际 %d", got)
	}
	if snap2.Stats.LateDropped != snap2.Drops[event.ReasonLateBeyondAllowed] {
		t.Fatalf("Stats 与 Drops 计数应一致: %d vs %d",
			snap2.Stats.LateDropped, snap2.Drops[event.ReasonLateBeyondAllowed])
	}
	status := map[string]watermark.PartitionStatus{}
	for _, p := range snap2.Partitions {
		status[p.ID] = p.Status
	}
	if status["p0"] != watermark.StatusActive || status["p1"] != watermark.StatusActive {
		t.Fatalf("p1 恢复供数后两个分片都应为 active，实际 %v", status)
	}
	t.Logf("[快照校验] 水位线/分片状态/待定与定稿窗口/拒绝计数 全部符合字段语义")

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// 关闭后所有窗口冲刷完毕：无待定窗口。
	snap3 := eng.Snapshot()
	if len(snap3.PendingWindows) != 0 {
		t.Fatalf("Close 后不应有待定窗口，实际 %d", len(snap3.PendingWindows))
	}
	t.Logf("[快照校验] Close 后待定窗口=0，已定稿窗口=%d", len(snap3.CommittedWindows))
}
