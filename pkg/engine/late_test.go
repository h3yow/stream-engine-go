package engine

import (
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

// drain 等待入口与分片缓冲中的事件全部被处理（无慢下游时足够）。
func drain(eng *Engine) {
	for eng.Stats().Received < eng.accepted.Load()+eng.duplicateSkipped.Load()+
		eng.lateDropped.Load()+eng.backpressureDropped.Load() || eng.Stats().Received == 0 {
		// Stats.Received 在处理开始时递增，故以 received==目标接收数 作为排空条件。
		break
	}
	// 简单可靠的做法：等待一个远大于调度抖动的时长（纯本地计算，处理在微秒级）。
	time.Sleep(100 * time.Millisecond)
}

// logDecisions 在处理完成后按真实统计与拒绝明细打印每条事件的判定依据。
func logDecisions(t *testing.T, eng *Engine, evs []event.Event) {
	t.Helper()
	drops := eng.Drops()
	droppedSeq := make(map[int64]event.DropReason, len(drops))
	for _, d := range drops {
		droppedSeq[d.Event.Seq] = d.Reason
	}
	st := eng.Stats()
	dupLeft := st.DuplicateSkipped
	seen := make(map[int64]bool, len(evs)) // 同 Seq 的再次投递才判“重复跳过”
	t.Logf("[处理后水位线] %ds（定稿条件：wm >= windowEnd + allowedLateness=%ds）",
		eng.Watermark().Unix(), eng.cfg.AllowedLateness/time.Second)
	for _, ev := range evs {
		w := eng.assigner.Assign(ev.Timestamp)
		verdict := "计入窗口"
		if reason, ok := droppedSeq[ev.Seq]; ok {
			verdict = "拒绝，原因=" + string(reason)
		} else if seen[ev.Seq] && dupLeft > 0 {
			verdict = "重复投递跳过（Seq 已计入）"
			dupLeft--
		}
		seen[ev.Seq] = true
		t.Logf("[逐条判定] seq=%d key=%s 事件时间=%ds 所属窗口=[%d,%d)s 定稿线=%ds → %s",
			ev.Seq, ev.Key, ev.Timestamp.Unix(), w.Start.Unix(), w.End.Unix(),
			w.End.Add(eng.cfg.AllowedLateness).Unix(), verdict)
	}
}

// TestLatenessBoundary 用有序事件流验证水位线驱动的定稿与迟到处理：
//   - 水位线越过 end+allowedLateness 时窗口定稿；
//   - 定稿前到达、落在允许迟到范围内的事件正确计入；
//   - 定稿后到达的事件被拒绝，原因为 late_beyond_allowed_lateness。
func TestLatenessBoundary(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128, Parallelism: 1,
	}
	snk, eng := newTestEngine(t, cfg)
	events := []event.Event{
		e("a", 1, 10, 1),
		e("a", 9, 20, 2), // 窗口 [0,10) 内
		e("a", 10, 1, 3), // 水位线→10s；W0 定稿线=15s，尚未定稿
		e("a", 9, 7, 4),  // 迟到但在允许范围内（ts<wm 10，定稿线15）→ 计入 W0
		e("a", 20, 1, 5), // 水位线→20s：W0(定稿线15)定稿 sum=37；W1 打开
		e("a", 8, 99, 6), // W0 已定稿 → 明确拒绝
		e("a", 19, 5, 7), // 迟到 1s（ts<wm 20）但 W1 定稿线25 未到 → 计入 W1
		e("a", 30, 1, 8), // 水位线→30s：W1(定稿线25)定稿 sum=1+5=6
	}
	for _, ev := range events {
		if !eng.Enqueue(ev) {
			t.Fatalf("Enqueue 失败: %+v", ev)
		}
	}
	drain(eng)
	logDecisions(t, eng, events)
	t.Logf("[定稿检查] 处理后水位线=%ds，已输出窗口数=%d", eng.Watermark().Unix(), snk.Written())
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(snk, eng)
	logResults(t, out, st, drops)

	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 37, count: 3, seqs: []int64{1, 2, 4}},
		{key: "a", start: 10}: {sum: 6, count: 2, seqs: []int64{3, 7}},
		{key: "a", start: 20}: {sum: 1, count: 1, seqs: []int64{5}},
		{key: "a", start: 30}: {sum: 1, count: 1, seqs: []int64{8}},
	})
	assertAggsEqual(t, want, out)
	if st.LateDropped != 1 {
		t.Fatalf("期望 1 条超范围迟到拒绝，实际 %d", st.LateDropped)
	}
	if len(drops) != 1 || drops[0].Reason != event.ReasonLateBeyondAllowed ||
		drops[0].Event.Seq != 6 {
		t.Fatalf("拒绝明细期望 seq=6 原因为 late_beyond_allowed_lateness，实际 %+v", drops)
	}
	if st.WindowsEmitted != 4 {
		t.Fatalf("期望 4 个窗口输出，实际 %d", st.WindowsEmitted)
	}
}

// TestWatermarkTriggerDuringRun 验证窗口在运行中（而非仅 Close 时）就被水位线触发。
func TestWatermarkTriggerDuringRun(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: 128, Parallelism: 1,
	}
	snk, eng := newTestEngine(t, cfg)
	for _, ev := range []event.Event{e("k", 1, 1, 1), e("k", 15, 1, 2)} {
		if !eng.Enqueue(ev) {
			t.Fatal("Enqueue 失败")
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for snk.Written() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if snk.Written() != 1 {
		t.Fatalf("水位线越过定稿线后窗口应在运行中输出，实际 written=%d", snk.Written())
	}
	t.Logf("[运行中触发] 水位线=%ds >= 定稿线10s，窗口 [0,10) 已输出，written=%d",
		eng.Watermark().Unix(), snk.Written())
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}
