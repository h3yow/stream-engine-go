package engine

import (
	"context"
	"sort"
	"testing"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// recoveryConfig 下 MaxOutOfOrderness=0、AllowedLateness=0：
// 水位线随事件时间推进，窗口在“看到后续窗口的首个事件”时定稿。
func recoveryConfig(buf int) Config {
	return Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: buf, Parallelism: 1,
	}
}

func logCheckpoint(t *testing.T, cp *checkpoint.Checkpoint) {
	t.Helper()
	refs := cp.CommittedWindows
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Key != refs[j].Key {
			return refs[i].Key < refs[j].Key
		}
		return refs[i].Start.Before(refs[j].Start)
	})
	t.Logf("[检查点] id=%d watermark=%ds maxSeq=%d 已提交窗口=%d 个",
		cp.ID, cp.Watermark.Unix(), cp.MaxSeq, len(refs))
	for _, r := range refs {
		t.Logf("           已提交 key=%s window=[%d,%d)s",
			r.Key, r.Start.Unix(), r.End.Unix())
	}
}

// TestCrashReplayRecovery 崩溃后从检查点重放：
// 第一次运行注入部分事件并在检查点后 Abort；第二次运行（同 store、同幂等下游）
// 从头重放全部事件。最终下游结果必须与“一次完整有序运行”完全一致：
// 已提交窗口不重复、未提交窗口不丢失、重复投递只计一次。
func TestCrashReplayRecovery(t *testing.T) {
	all := []event.Event{
		e("k", 1, 10, 1), e("k", 11, 20, 2), // 看到 ts=11：W0 定稿
		e("k", 21, 30, 3), // W1 定稿
		e("k", 31, 40, 4), // W2 定稿
		e("k", 41, 50, 5), // W3 定稿
	}
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "k", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "k", start: 10}: {sum: 20, count: 1, seqs: []int64{2}},
		{key: "k", start: 20}: {sum: 30, count: 1, seqs: []int64{3}},
		{key: "k", start: 30}: {sum: 40, count: 1, seqs: []int64{4}},
		{key: "k", start: 40}: {sum: 50, count: 1, seqs: []int64{5}},
	})

	// 参考：一次完整运行。
	refOut, _, _, _ := runEngine(t, recoveryConfig(128), all)
	assertAggsEqual(t, want, refOut)

	// 第一次运行：持久下游 + 检查点存储，注入 3 条后显式检查点，再 Abort。
	store := checkpoint.NewMemoryStore()
	persistentSink := sink.NewCollectingSink(0)
	eng1 := New(recoveryConfig(128), persistentSink, store)
	if err := eng1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range all[:3] {
		eng1.Enqueue(ev)
	}
	drain(eng1)
	cp, err := eng1.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	logCheckpoint(t, cp)
	if cp.MaxSeq != 3 || len(cp.CommittedWindows) != 2 {
		t.Fatalf("检查点期望 maxSeq=3 已提交2窗口，实际 maxSeq=%d committed=%d",
			cp.MaxSeq, len(cp.CommittedWindows))
	}
	beforeWritten := persistentSink.Written()
	eng1.Abort()
	t.Logf("[崩溃] Abort 完成，崩溃前下游已有 %d 条窗口输出", beforeWritten)

	// 第二次运行：复用同一 store 与同一下游（模拟持久下游），从头重放全部事件。
	eng2 := New(recoveryConfig(128), persistentSink, store)
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.LoadLatest(context.Background())
	logCheckpoint(t, loaded)
	t.Logf("[恢复] 从 watermark=%ds、%d 个已提交窗口恢复；数据源从头重放 5 条事件",
		loaded.Watermark.Unix(), len(loaded.CommittedWindows))
	for _, ev := range all {
		eng2.Enqueue(ev)
	}
	if err := eng2.Close(); err != nil {
		t.Fatal(err)
	}
	out, st, drops, _ := collect(persistentSink, eng2)
	logResults(t, out, st, drops)
	assertAggsEqual(t, want, out)
	if persistentSink.Written() != 5 {
		t.Fatalf("崩溃前已写2 + 恢复后补3，期望总写出5，实际 %d", persistentSink.Written())
	}
	if persistentSink.DuplicateSkips() != 0 {
		t.Fatalf("引擎层已提交集合应拦截重复窗口，下游重复跳过期望0，实际 %d",
			persistentSink.DuplicateSkips())
	}
	if st.LateDropped != 2 {
		t.Fatalf("重放中 seq1/seq2 属于检查点已提交窗口，应按超范围迟到拒绝2条，实际 %d",
			st.LateDropped)
	}
	for _, d := range drops {
		if d.Event.Seq > 2 || d.Reason != event.ReasonLateBeyondAllowed {
			t.Fatalf("拒绝明细异常: %+v", d)
		}
	}
}

// TestRestartWithoutCheckpoint 无检查点时恢复等价于全新运行（位点界定：回退到起点）。
func TestRestartWithoutCheckpoint(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	snk := sink.NewCollectingSink(0)
	eng := New(recoveryConfig(128), snk, store)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng.Enqueue(e("k", 1, 10, 1))
	eng.Enqueue(e("k", 11, 20, 2)) // W0 已真正写到下游
	drain(eng)
	if snk.Written() != 1 {
		t.Fatalf("前置条件：崩溃前 W0 应已写出 1 条，实际 %d", snk.Written())
	}
	eng.Abort() // 无检查点：位点回退界定为“起点”，但下游 W0 已持久
	eng2 := New(recoveryConfig(128), snk, store)
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng2.Enqueue(e("k", 1, 10, 1))
	eng2.Enqueue(e("k", 11, 20, 2)) // 重放 W0：引擎无已提交集合，下游幂等跳过
	eng2.Enqueue(e("k", 21, 30, 3)) // 新窗口 W1 不丢失
	if err := eng2.Close(); err != nil {
		t.Fatal(err)
	}
	// 全量输入覆盖 W0/W1/W2：W0 崩溃前已写（幂等跳过），W1/W2 合法补出。
	if snk.Written() != 3 {
		t.Fatalf("无检查点回退到起点重放：期望共3条窗口（W0幂等、W1/W2新增），实际 written=%d",
			snk.Written())
	}
	if snk.DuplicateSkips() != 1 {
		t.Fatalf("W0 重放应由下游幂等跳过1次，实际 %d", snk.DuplicateSkips())
	}
	t.Logf("[回退界定] 无检查点 → 位点回退到起点重放；下游 W0 幂等跳过，W1/W2 正常输出，written=%d skips=%d",
		snk.Written(), snk.DuplicateSkips())
}
