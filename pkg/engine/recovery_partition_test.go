package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// finCfg 下 MaxOutOfOrderness=0、AllowedLateness=5s、空闲检测形同关闭（隔离变量）：
// 水位线随事件时间推进，窗口定稿线 = 窗口结束 + 5s。
func finCfg(parallelism int) Config {
	return Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 5 * time.Second, InputBuffer: 128,
		Parallelism: parallelism, IdleTimeout: time.Hour,
	}
}

// finEvents 终止恢复场景的事件流（按注入顺序）：
//   - seq1 a@1(p0)、seq2 b@2(p1)、seq3 a@11(p0)：终止宣告前注入，全部正常计入；
//   - seq4 b@12(p1)、seq5 c@5(p2)：FinishPartition(p1/p2) 之后“再来的事件”，
//     必须以 partition_finished 拒绝（恢复前如此，恢复后也必须如此）；
//   - seq6 a@21(p0)、seq7 a@31(p0)：检查点之后的续播事件。
func finEvents() []event.Event {
	return []event.Event{
		ep("a", "p0", 1, 1, 1),
		ep("b", "p1", 2, 20, 2),
		ep("a", "p0", 11, 2, 3),
		ep("b", "p1", 12, 99, 4),
		ep("c", "p2", 5, 77, 5),
		ep("a", "p0", 21, 3, 6),
		ep("a", "p0", 31, 4, 7),
	}
}

// finWant 为终止恢复场景的期望最终聚合（与参考运行一致）：
// W0a={seq1}、W0b={seq2}、W1a={seq3}、W2a={seq6}、W3a={seq7}；
// seq4/seq5 被 partition_finished 拒绝，不产生任何窗口。
func finWant() map[resultKey]expectedAgg {
	return expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 1, count: 1, seqs: []int64{1}},
		{key: "b", start: 0}:  {sum: 20, count: 1, seqs: []int64{2}},
		{key: "a", start: 10}: {sum: 2, count: 1, seqs: []int64{3}},
		{key: "a", start: 20}: {sum: 3, count: 1, seqs: []int64{6}},
		{key: "a", start: 30}: {sum: 4, count: 1, seqs: []int64{7}},
	})
}

// TestFinishedPartitionSurvivesRecovery 已宣告“彻底不再供数”的分片，
// 从检查点恢复（含换并行度扩缩容恢复）后必须保持终止：
//
//   - 终止分片（含一条数据都没出过就被停掉的 p2）恢复后不变回正常供数；
//   - 恢复后这些分片上“再来的事件”（序号越过终止位点）仍以 partition_finished
//     明确拒绝，与迟到/背压可区分，不被重新算进窗口；
//   - 终止前已计入、但检查点时尚未定稿的窗口（W0b），恢复重放时必须凭
//     终止位点重新接受其事件、完整重建——不多算也不少算；
//   - 恢复前后最终聚合与无崩溃参考运行完全一致。
func TestFinishedPartitionSurvivesRecovery(t *testing.T) {
	all := finEvents()
	preFinish := all[:3]   // seq1..3：终止宣告前
	postFinish := all[3:5] // seq4..5：终止宣告后来自 p1/p2 的事件
	want := finWant()

	// 参考运行：不崩溃，同一逻辑时序（同样的终止宣告点）跑完全部事件。
	refCfg := finCfg(2)
	snkRef, engRef := newTestEngine(t, refCfg)
	for _, ev := range preFinish {
		engRef.Enqueue(ev)
	}
	waitFor(t, "参考运行终止前事件处理完成", func() bool { return engRef.Stats().Received == 3 })
	engRef.FinishPartition("p1")
	engRef.FinishPartition("p2")
	for _, ev := range all[3:] {
		engRef.Enqueue(ev)
	}
	if err := engRef.Close(); err != nil {
		t.Fatal(err)
	}
	refOut, refSt, _, _ := collect(snkRef, engRef)
	assertAggsEqual(t, want, refOut)
	if refSt.PartitionFinishedDropped != 2 {
		t.Fatalf("参考运行应有2条 partition_finished 拒绝，实际 %d", refSt.PartitionFinishedDropped)
	}
	t.Logf("[参考运行] 无崩溃跑完全部事件：窗口=%d partition_finished拒绝=%d", len(refOut), refSt.PartitionFinishedDropped)

	for _, tc := range []struct{ from, to int }{{1, 1}, {2, 2}, {2, 4}, {4, 1}} {
		t.Run(fmt.Sprintf("并行度%d→%d", tc.from, tc.to), func(t *testing.T) {
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)

			// 第一次运行：终止宣告 → 拒绝终止后事件 → 检查点（此时无任何窗口定稿）→ 崩溃。
			eng1 := New(finCfg(tc.from), persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, ev := range preFinish {
				eng1.Enqueue(ev)
			}
			waitFor(t, "终止前事件处理完成", func() bool { return eng1.Stats().Received == 3 })
			eng1.FinishPartition("p1") // p1 终止前已供数（seq<=2）
			eng1.FinishPartition("p2") // p2 一条数据都没出过就被停掉
			t.Logf("[终止宣告] p1(已供数至seq=2) 与 p2(从未供数) 宣告彻底不再供数")
			for _, ev := range postFinish {
				eng1.Enqueue(ev)
			}
			waitFor(t, "终止后事件处理完成", func() bool { return eng1.Stats().Received == 5 })
			if got := eng1.Stats().PartitionFinishedDropped; got != 2 {
				t.Fatalf("崩溃前应有2条 partition_finished 拒绝，实际 %d", got)
			}
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			// 检查点必须记录两个终止分片及其终止位点（含从未供数的 p2）。
			p1st, ok := cp.Partitions["p1"]
			if !ok || !p1st.Finished || !p1st.HasFinishSeq || p1st.FinishSeq != 2 {
				t.Fatalf("检查点应记录 p1 finished 且 finishSeq=2，实际 %+v", p1st)
			}
			p2st, ok := cp.Partitions["p2"]
			if !ok || !p2st.Finished || p2st.HasFinishSeq {
				t.Fatalf("检查点应记录 p2 finished 且无终止位点（从未供数），实际 %+v", p2st)
			}
			if len(cp.CommittedWindows) != 0 {
				t.Fatalf("检查点时刻水位线11s 尚无任何窗口定稿，实际已提交 %d 个", len(cp.CommittedWindows))
			}
			eng1.Abort()
			if persistentSink.Written() != 0 {
				t.Fatalf("崩溃前不应有任何窗口写出，实际 %d", persistentSink.Written())
			}
			t.Logf("[崩溃] 并行度%d 下 Abort：检查点水位线=%ds，W0a/W0b/W1a 均未定稿，下游零写出", tc.from, cp.Watermark.Unix())

			// 第二次运行：换并行度恢复，先核对分片状态，再从头重放全部事件。
			eng2 := New(finCfg(tc.to), persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			snap0 := eng2.Snapshot()
			logSnapshot(t, snap0, "恢复后-未供数")
			status := map[string]watermark.PartitionStatus{}
			finSeq := map[string]int64{}
			for _, p := range snap0.Partitions {
				status[p.ID] = p.Status
				finSeq[p.ID] = p.FinishSeq
			}
			if status["p1"] != watermark.StatusFinished || status["p2"] != watermark.StatusFinished {
				t.Fatalf("恢复后 p1/p2 必须保持 finished（不得复活为正常供数），实际 %v", status)
			}
			if status["p0"] != watermark.StatusActive {
				t.Fatalf("恢复后 p0 应为 active，实际 %v", status)
			}
			if finSeq["p1"] != 2 {
				t.Fatalf("恢复后 p1 终止位点应为2，实际 %d", finSeq["p1"])
			}
			if snap0.Watermark != et(11) {
				t.Fatalf("恢复后水位线应为11s，实际 %ds", snap0.Watermark.Unix())
			}
			t.Logf("[恢复] 并行度%d→%d：p1/p2 保持 finished（p1位点=2，p2从未供数），水位线=11s，从头重放 %d 条事件",
				tc.from, tc.to, len(all))

			// 分阶段重放（阶段间排空）：与参考运行同一逻辑时序，
			// 保证任意并行度下处理顺序确定、结果可复现。
			for _, ev := range preFinish { // seq1..3：W0a/W0b/W1a 在水位线11s下重新打开
				eng2.Enqueue(ev)
			}
			drain(eng2)
			for _, ev := range postFinish { // seq4..5：恢复后仍以 partition_finished 拒绝
				eng2.Enqueue(ev)
			}
			drain(eng2)
			for _, ev := range all[5:] { // seq6..7：推进水位线，W0/W1 定稿
				eng2.Enqueue(ev)
			}
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
			logResults(t, out, st, drops)

			// 恢复前后最终聚合必须与参考运行完全一致（不多算、不少算）。
			assertAggsEqual(t, want, out)
			// 关键：W0b 在检查点时尚未定稿，恢复后靠终止位点重新接受 seq=2 重建。
			if got := out[resultKey{key: "b", start: 0}]; got.count != 1 || got.sum != 20 {
				t.Fatalf("W0(b) 必须由重放的终止前事件重建为 {sum=20,count=1}，实际 %+v", got)
			}
			// 恢复后 p1/p2 上“再来的事件”仍以 partition_finished 拒绝。
			if st.PartitionFinishedDropped != 2 {
				t.Fatalf("恢复后应有2条 partition_finished 拒绝，实际 %d", st.PartitionFinishedDropped)
			}
			if st.LateDropped != 0 {
				t.Fatalf("本场景不应有迟到拒绝（重放事件要么落入未定稿窗口、要么按终止位点判定），实际 %d", st.LateDropped)
			}
			for _, d := range drops {
				if d.Reason != event.ReasonPartitionFinished || (d.Event.Seq != 4 && d.Event.Seq != 5) {
					t.Fatalf("拒绝明细期望 seq=4/5 partition_finished，实际 %+v", d)
				}
			}
			if persistentSink.Written() != int64(len(want)) || persistentSink.DuplicateSkips() != 0 {
				t.Fatalf("恢复运行应恰好写出 %d 个窗口且零重复跳过，实际 written=%d skips=%d",
					len(want), persistentSink.Written(), persistentSink.DuplicateSkips())
			}
			t.Logf("[恢复结论] 并行度%d→%d：终止分片未复活，聚合与参考运行完全一致（窗口=%d 写出=%d）",
				tc.from, tc.to, len(out), persistentSink.Written())
		})
	}
}

// TestIdleVsFinishedAfterRecovery “只是暂时没数据”（idle）与“已经彻底不再供数”
// （finished）两种状态在恢复后必须一直分得开、表现不一样：
//
//   - 空闲分片 slow：恢复后静默一阵子照常转 idle、水位线跟着 fast 往前走、
//     到期窗口照常定稿；恢复供数后落在允许迟到范围内的事件正常计入、
//     重复投递不重复算、明显超范围的老事件照样拒绝；
//   - 终止分片 dead：从未供数就被停掉，恢复后任何事件都以 partition_finished 拒绝；
//   - 恢复前后最终聚合与无崩溃参考运行完全一致。
func TestIdleVsFinishedAfterRecovery(t *testing.T) {
	cfg := func(parallelism int) Config {
		return Config{
			WindowSize: winSize, MaxOutOfOrderness: 0,
			AllowedLateness: 5 * time.Second, InputBuffer: 128,
			Parallelism: parallelism, IdleTimeout: 200 * time.Millisecond,
		}
	}
	// 事件序列（seq 即注入顺序）：
	//   seq1 s@1(slow)  seq2 f@1(fast)   seq3 f@10(fast)  —— slow 沉默前的供数
	//   seq4 x@5(dead)                          —— dead 已终止 → partition_finished
	//   seq5 f@20       seq6 f@30               —— slow idle 期间水位线随 fast 推进
	//   seq7 s@22                               —— slow 恢复供数，落在允许范围内 → 计入
	//   seq8 s@8                                —— 窗口已定稿 → 超范围拒绝
	//   seq7(重投)                              —— 重复投递 → 去重跳过
	//   seq13 f@40                              —— slow 再次 idle 后水位线推进到40s
	//   seq14 s@35                              —— 恢复后再次供数，允许范围内 → 计入
	//   seq15 s@18                              —— 明显超范围老事件 → 拒绝
	//   seq16 x@6(dead)                         —— 恢复后 dead 的事件照样拒绝
	head := []event.Event{ep("s", "slow", 1, 10, 1), ep("f", "fast", 1, 1, 2), ep("f", "fast", 10, 1, 3)}
	dead1 := ep("x", "dead", 5, 99, 4)
	mid := []event.Event{ep("f", "fast", 20, 1, 5), ep("f", "fast", 30, 1, 6)}
	slowBack := []event.Event{
		ep("s", "slow", 22, 100, 7), ep("s", "slow", 8, 999, 8), ep("s", "slow", 22, 100, 7),
	}
	fastFar := ep("f", "fast", 40, 1, 13)
	tail := []event.Event{
		ep("s", "slow", 35, 7, 14), ep("s", "slow", 18, 5, 15), ep("x", "dead", 6, 1, 16),
	}

	want := expectedSet(map[resultKey]expectedAgg{
		{key: "s", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "f", start: 0}:  {sum: 1, count: 1, seqs: []int64{2}},
		{key: "f", start: 10}: {sum: 1, count: 1, seqs: []int64{3}},
		{key: "f", start: 20}: {sum: 1, count: 1, seqs: []int64{5}},
		{key: "s", start: 20}: {sum: 100, count: 1, seqs: []int64{7}},
		{key: "f", start: 30}: {sum: 1, count: 1, seqs: []int64{6}},
		{key: "f", start: 40}: {sum: 1, count: 1, seqs: []int64{13}},
		{key: "s", start: 30}: {sum: 7, count: 1, seqs: []int64{14}},
	})

	// feedAll 按统一时序注入：终止宣告后 dead 事件、两段 slow 静默（转 idle）、
	// slow 两次恢复供数。参考运行与两次恢复运行共用，保证可对比。
	feedAll := func(t *testing.T, eng *Engine, stopAtCheckpoint bool) {
		t.Helper()
		for _, ev := range head {
			eng.Enqueue(ev)
		}
		waitFor(t, "head 处理完成", func() bool { return eng.Stats().Received == 3 })
		eng.FinishPartition("dead") // 从未供数就被停掉
		eng.Enqueue(dead1)
		waitFor(t, "dead 事件处理完成", func() bool { return eng.Stats().Received == 4 })
		time.Sleep(300 * time.Millisecond) // slow 静默 → idle
		for _, ev := range mid {
			eng.Enqueue(ev)
		}
		waitFor(t, "mid 处理完成", func() bool { return eng.Stats().Received == 6 })
		if stopAtCheckpoint {
			return
		}
		for _, ev := range slowBack {
			eng.Enqueue(ev)
		}
		waitFor(t, "slowBack 处理完成", func() bool { return eng.Stats().Received == 9 })
		time.Sleep(300 * time.Millisecond) // slow 再次静默 → idle
		eng.Enqueue(fastFar)
		waitFor(t, "fastFar 处理完成", func() bool { return eng.Stats().Received == 10 })
		for _, ev := range tail {
			eng.Enqueue(ev)
		}
		waitFor(t, "tail 处理完成", func() bool { return eng.Stats().Received == 13 })
	}

	// 参考运行：不崩溃跑完全部流程。
	snkRef, engRef := newTestEngine(t, cfg(2))
	feedAll(t, engRef, false)
	if err := engRef.Close(); err != nil {
		t.Fatal(err)
	}
	refOut, refSt, _, _ := collect(snkRef, engRef)
	assertAggsEqual(t, want, refOut)
	t.Logf("[参考运行] 接收=%d 计入=%d 迟到拒绝=%d 终止拒绝=%d 重复跳过=%d",
		refSt.Received, refSt.Accepted, refSt.LateDropped, refSt.PartitionFinishedDropped, refSt.DuplicateSkipped)

	for _, parallelism := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("并行度%d", parallelism), func(t *testing.T) {
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)

			// 第一次运行：跑到 mid 结束（slow 处于 idle、水位线30s、W0/W1 已定稿）后检查点，
			// 再补 slowBack 三条（不计入检查点），然后崩溃。
			eng1 := New(cfg(parallelism), persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			feedAll(t, eng1, true)
			if wm := eng1.Watermark(); wm != et(30) {
				t.Fatalf("slow idle 期间水位线应随 fast 推进到30s，实际 %ds", wm.Unix())
			}
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			if cp.Partitions["dead"].Finished != true {
				t.Fatalf("检查点应记录 dead 为 finished，实际 %+v", cp.Partitions["dead"])
			}
			if cp.Partitions["slow"].Finished {
				t.Fatal("slow 只是暂时没数据，检查点不得把它记成 finished")
			}
			for _, ev := range slowBack {
				eng1.Enqueue(ev)
			}
			waitFor(t, "崩溃前 slowBack 处理完成", func() bool { return eng1.Stats().Received == 9 })
			eng1.Abort()
			crashWritten := persistentSink.Written()
			t.Logf("[崩溃] 检查点水位线=%ds 已提交=%d 个；崩溃前下游已写 %d 条（W2 待定稿部分丢失待重放重建）",
				cp.Watermark.Unix(), len(cp.CommittedWindows), crashWritten)

			// 第二次运行：恢复后重放全部事件 + 续播新事件。
			eng2 := New(cfg(parallelism), persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			snap0 := eng2.Snapshot()
			logSnapshot(t, snap0, "恢复后-未供数")
			st0 := map[string]watermark.PartitionStatus{}
			for _, p := range snap0.Partitions {
				st0[p.ID] = p.Status
			}
			if st0["dead"] != watermark.StatusFinished {
				t.Fatalf("恢复后 dead 必须保持 finished，实际 %v", st0)
			}
			if st0["slow"] != watermark.StatusActive || st0["fast"] != watermark.StatusActive {
				t.Fatalf("恢复后 slow/fast 应以 active 重新开始空闲计时，实际 %v", st0)
			}

			// 重放 head/dead 事件后让 slow 静默：恢复后 idle 判定照常生效。
			for _, ev := range head {
				eng2.Enqueue(ev)
			}
			eng2.Enqueue(dead1)
			waitFor(t, "恢复运行 head 处理完成", func() bool { return eng2.Stats().Received == 4 })
			time.Sleep(300 * time.Millisecond)
			snapIdle := eng2.Snapshot()
			logSnapshot(t, snapIdle, "恢复后-slow静默")
			stIdle := map[string]watermark.PartitionStatus{}
			for _, p := range snapIdle.Partitions {
				stIdle[p.ID] = p.Status
			}
			if stIdle["slow"] != watermark.StatusIdle {
				t.Fatalf("恢复后 slow 静默超时应照常判定 idle，实际 %v", stIdle)
			}
			if stIdle["dead"] != watermark.StatusFinished {
				t.Fatalf("slow 转 idle 不得影响 dead 的 finished 状态，实际 %v", stIdle)
			}
			t.Logf("[状态可分] 恢复后：slow=idle（暂时没数据，可回来）≠ dead=finished（彻底停掉，永不回来）")

			// 续播：mid/slowBack 重放 + fastFar/tail 新事件。
			for _, ev := range mid {
				eng2.Enqueue(ev)
			}
			for _, ev := range slowBack {
				eng2.Enqueue(ev)
			}
			waitFor(t, "恢复运行 slowBack 处理完成", func() bool { return eng2.Stats().Received == 9 })
			time.Sleep(300 * time.Millisecond)
			eng2.Enqueue(fastFar)
			waitFor(t, "恢复运行水位线推进到40s", func() bool { return eng2.Watermark() == et(40) })
			t.Logf("[水位线判定] 恢复后 slow 再次静默转 idle，水位线照常随 fast 推进到40s，W2 到期定稿")
			for _, ev := range tail {
				eng2.Enqueue(ev)
			}
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
			logResults(t, out, st, drops)

			// 恢复前后最终聚合必须与参考运行完全一致。
			assertAggsEqual(t, want, out)
			// 恢复运行重放了检查点前已计入的5条事件（落入已定稿窗口 → 超范围迟到拒绝），
			// 因此迟到拒绝比参考运行多；终止拒绝与重复跳过必须一致。
			if st.PartitionFinishedDropped != refSt.PartitionFinishedDropped {
				t.Fatalf("恢复前后 partition_finished 拒绝数应一致：参考=%d 恢复=%d",
					refSt.PartitionFinishedDropped, st.PartitionFinishedDropped)
			}
			if st.DuplicateSkipped != refSt.DuplicateSkipped {
				t.Fatalf("恢复前后重复跳过数应一致：参考=%d 恢复=%d",
					refSt.DuplicateSkipped, st.DuplicateSkipped)
			}
			pfDrops := 0
			for _, d := range drops {
				if d.Reason == event.ReasonPartitionFinished {
					pfDrops++
					if d.Event.Partition != "dead" {
						t.Fatalf("partition_finished 拒绝应全部来自 dead 分片，实际 %+v", d)
					}
				}
			}
			if pfDrops != 2 {
				t.Fatalf("恢复后 dead 分片的两条事件（seq=4/16）都应被 partition_finished 拒绝，实际 %d 条", pfDrops)
			}
			t.Logf("[恢复结论] 并行度%d：idle/finished 恢复后表现可分，聚合与参考运行完全一致（窗口=%d）",
				parallelism, len(out))
		})
	}
}
