package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// partitionStatuses 从快照提取 分片ID→状态 映射，便于断言。
func partitionStatuses(snap Snapshot) map[string]watermark.PartitionStatus {
	out := make(map[string]watermark.PartitionStatus, len(snap.Partitions))
	for _, p := range snap.Partitions {
		out[p.ID] = p.Status
	}
	return out
}

// TestFinishedPartitionSurvivesRecovery 已宣告“彻底不再供数”的分片在检查点恢复
// （含换并行度扩缩容）后必须保持 finished：
//   - 终止分片（含一条数据都没出过就被终止的 ghost 分片）恢复后依旧被永久排除，
//     其后续事件以 partition_finished 拒绝，不会被重新算进窗口；
//   - 恢复前后最终聚合与“无崩溃参考运行”完全一致（不多算、不少算）。
func TestFinishedPartitionSurvivesRecovery(t *testing.T) {
	cfg := func(par int) Config {
		return Config{
			WindowSize: winSize, MaxOutOfOrderness: 0,
			AllowedLateness: 0, InputBuffer: 128,
			Parallelism: par, IdleTimeout: time.Hour, // 关闭空闲检测，隔离变量
		}
	}
	// 期望聚合（与无崩溃单次运行一致）：p1/ghost 终止后不再贡献任何窗口。
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 10, count: 1, seqs: []int64{1}},
		{key: "b", start: 0}:  {sum: 20, count: 1, seqs: []int64{2}},
		{key: "a", start: 10}: {sum: 11, count: 1, seqs: []int64{3}},
		{key: "a", start: 20}: {sum: 21, count: 1, seqs: []int64{4}},
		{key: "a", start: 30}: {sum: 31, count: 1, seqs: []int64{7}},
		{key: "a", start: 40}: {sum: 41, count: 1, seqs: []int64{8}},
	})

	for _, tc := range []struct{ from, to int }{{1, 1}, {2, 4}, {4, 1}} {
		t.Run(fmt.Sprintf("并行度%d→%d", tc.from, tc.to), func(t *testing.T) {
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)

			// 第一次运行：p0/p1 供数；随后宣告 p1 与 ghost（从未供数）彻底终止；
			// p0 推进水位线使 W0/W1 定稿后做检查点，再模拟崩溃。
			eng1 := New(cfg(tc.from), persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			eng1.Enqueue(ep("a", "p0", 1, 10, 1))
			eng1.Enqueue(ep("b", "p1", 2, 20, 2))
			// 等 p0/p1 都注册（首条计入）后再推进水位线：
			// 晚注册分片的早事件会被已推进的水位线判定迟到（引擎既定语义），
			// 注册顺序必须由同步等待确定，否则用例本身引入不确定性。
			waitFor(t, "p0/p1 分片注册", func() bool { return eng1.Stats().Accepted == 2 })
			eng1.Enqueue(ep("a", "p0", 11, 11, 3))
			// 等前三条全部计入（Observe 完成）再宣告终止，保证终止时机确定。
			waitFor(t, "终止前三条事件计入", func() bool { return eng1.Stats().Accepted == 3 })
			eng1.FinishPartition("p1")
			eng1.FinishPartition("ghost") // 一条数据都没出过就被终止
			t.Logf("[终止宣告] p1（有历史数据）与 ghost（零数据）宣告彻底不再供数")
			eng1.Enqueue(ep("a", "p0", 21, 21, 4)) // 水位线→21s：W0(定稿线10s)/W1(定稿线20s) 定稿
			waitFor(t, "a@21 处理完成", func() bool { return eng1.Stats().Received == 4 })
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			t.Logf("[检查点] 已终止分片=%v 分片进展=%v", cp.FinishedPartitions, cp.Partitions)
			if len(cp.FinishedPartitions) != 2 || cp.FinishedPartitions[0] != "ghost" || cp.FinishedPartitions[1] != "p1" {
				t.Fatalf("检查点应记录已终止分片[ghost p1]（含零数据分片），实际 %v", cp.FinishedPartitions)
			}
			if len(cp.CommittedWindows) != 3 {
				t.Fatalf("检查点应含3个已提交窗口(aW0/bW0/aW1)，实际 %d", len(cp.CommittedWindows))
			}
			eng1.Abort()
			t.Logf("[崩溃] 并行度%d 下 Abort，崩溃前下游已写 %d 条", tc.from, persistentSink.Written())

			// 第二次运行：换并行度从同一检查点恢复。
			eng2 := New(cfg(tc.to), persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			snap0 := eng2.Snapshot()
			logSnapshot(t, snap0, "恢复后-未供数")
			st0 := partitionStatuses(snap0)
			if st0["p1"] != watermark.StatusFinished || st0["ghost"] != watermark.StatusFinished {
				t.Fatalf("恢复后 p1/ghost 必须保持 finished，实际 %v", st0)
			}
			if st0["p0"] != watermark.StatusActive {
				t.Fatalf("恢复后 p0 应为 active，实际 %v", st0)
			}
			if snap0.Watermark != et(21) {
				t.Fatalf("恢复后水位线应为21s（不回退），实际 %ds", snap0.Watermark.Unix())
			}
			t.Logf("[恢复] 并行度%d→%d：p1/ghost 保持 finished（零数据分片未丢失终止语义），水位线=21s",
				tc.from, tc.to)

			// 重放 + 新事件。判定依据：
			//   seq1 a@1  → W0 已提交 → 超范围迟到拒绝；
			//   seq2 b@2  → p1 已终止 → partition_finished（不是迟到！）；
			//   seq5 x@5  → ghost 零数据终止分片 → partition_finished；
			//   seq3 a@11 → W1 已提交 → 超范围迟到拒绝；
			//   seq4 a@21 → W2 未提交 → 正常计入（重放不丢）；
			//   seq6 b@22 → p1 终止后的新事件 → partition_finished；
			//   seq7/8    → 正常计入并推进水位线定稿 W2/W3。
			replay := []event.Event{
				ep("a", "p0", 1, 10, 1), ep("b", "p1", 2, 20, 2), ep("x", "ghost", 5, 1, 5),
				ep("a", "p0", 11, 11, 3), ep("a", "p0", 21, 21, 4), ep("b", "p1", 22, 99, 6),
				ep("a", "p0", 31, 31, 7), ep("a", "p0", 41, 41, 8),
			}
			for _, ev := range replay {
				eng2.Enqueue(ev)
			}
			waitFor(t, "重放8条处理完成", func() bool { return eng2.Stats().Received == 8 })
			logDecisions(t, eng2, replay)
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
			logResults(t, out, st, drops)

			assertAggsEqual(t, want, out)
			if persistentSink.Written() != int64(len(want)) {
				t.Fatalf("恢复前后总写出应等于窗口数%d（不重不丢），实际 %d", len(want), persistentSink.Written())
			}
			if persistentSink.DuplicateSkips() != 0 {
				t.Fatalf("已提交集合应拦截全部重复窗口，下游重复跳过期望0，实际 %d", persistentSink.DuplicateSkips())
			}
			if st.PartitionFinishedDropped != 3 {
				t.Fatalf("终止分片拒绝期望3（seq2/5/6），实际 %d", st.PartitionFinishedDropped)
			}
			if st.LateDropped != 2 {
				t.Fatalf("超范围迟到拒绝期望2（seq1/3），实际 %d", st.LateDropped)
			}
			for _, d := range drops {
				switch d.Event.Seq {
				case 2, 5, 6:
					if d.Reason != event.ReasonPartitionFinished {
						t.Fatalf("seq=%d 应以 partition_finished 拒绝，实际 %s", d.Event.Seq, d.Reason)
					}
				case 1, 3:
					if d.Reason != event.ReasonLateBeyondAllowed {
						t.Fatalf("seq=%d 应以 late_beyond_allowed_lateness 拒绝，实际 %s", d.Event.Seq, d.Reason)
					}
				default:
					t.Fatalf("非预期拒绝: %+v", d)
				}
			}
			t.Logf("[结论] 并行度%d→%d 恢复后：终止分片仍被拒（pf=3）、重放不重复（late=2）、聚合与参考一致（窗口=%d）",
				tc.from, tc.to, len(out))
		})
	}
}

// TestIdlePartitionSemanticsAfterRecovery “只是暂时没数据”的分片在恢复后仍与
// “彻底停掉”分得开：
//   - 恢复后 idle 分片以 active 重新计时；静默超阈值后再次转入 idle，
//     水位线照常跟随其他供数分片推进、该定稿的窗口照常定稿；
//   - 恢复供数时：允许迟到范围内的事件正常计入、重复投递不重复算、
//     明显超范围的老事件照样按 late_beyond_allowed_lateness 拒绝；
//   - 最终聚合与期望完全一致。
func TestIdlePartitionSemanticsAfterRecovery(t *testing.T) {
	for _, par := range []int{2, 4} {
		t.Run(fmt.Sprintf("并行度%d", par), func(t *testing.T) {
			cfg := Config{
				WindowSize: winSize, MaxOutOfOrderness: 0,
				AllowedLateness: 5 * time.Second, InputBuffer: 128,
				Parallelism: par, IdleTimeout: 400 * time.Millisecond,
			}
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)

			// 第一次运行：slow 一条后沉默转 idle；fast 推进水位线使 W0 定稿；
			// 在 slow 处于 idle 时做检查点，再模拟崩溃。
			eng1 := New(cfg, persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			eng1.Enqueue(ep("s", "slow", 1, 10, 1))
			waitFor(t, "slow 分片注册", func() bool { return eng1.Stats().Received == 1 })
			eng1.Enqueue(ep("f", "fast", 1, 1, 2))
			eng1.Enqueue(ep("f", "fast", 10, 1, 3))
			waitFor(t, "fast 前两条处理完成", func() bool { return eng1.Stats().Received == 3 })
			time.Sleep(500 * time.Millisecond) // slow 超过 400ms 未供数 → idle
			eng1.Enqueue(ep("f", "fast", 20, 1, 4)) // slow idle 被排除 → 水位线→20s，W0(定稿线15s) 定稿
			waitFor(t, "W0 两 key 定稿", func() bool { return persistentSink.Written() >= 2 })
			snapIdle := eng1.Snapshot()
			logSnapshot(t, snapIdle, "崩溃前-slow空闲")
			if st := partitionStatuses(snapIdle)["slow"]; st != watermark.StatusIdle {
				t.Fatalf("崩溃前 slow 应为 idle，实际 %s", st)
			}
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			eng1.Abort()
			t.Logf("[崩溃] slow 处于 idle 时 Abort；检查点水位线=%ds 已提交窗口=%d 个",
				cp.Watermark.Unix(), len(cp.CommittedWindows))

			// 第二次运行：从检查点恢复。
			eng2 := New(cfg, persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			snap0 := eng2.Snapshot()
			logSnapshot(t, snap0, "恢复后-未供数")
			st0 := partitionStatuses(snap0)
			// idle 是处理时间上的瞬时判定：恢复后重新计时，先表现为 active。
			if st0["slow"] != watermark.StatusActive || st0["fast"] != watermark.StatusActive {
				t.Fatalf("恢复后 idle 分片应以 active 重新计时，实际 %v", st0)
			}
			if snap0.Watermark != et(20) {
				t.Fatalf("恢复后水位线应由检查点兜底为20s，实际 %ds", snap0.Watermark.Unix())
			}
			if len(snap0.CommittedWindows) != 2 {
				t.Fatalf("恢复后已提交窗口应为2（sW0/fW0），实际 %d", len(snap0.CommittedWindows))
			}

			// 重放：已提交窗口事件按超范围迟到拒绝；未提交部分正常计入。
			eng2.Enqueue(ep("s", "slow", 1, 10, 1)) // sW0 已提交 → 迟到拒绝
			eng2.Enqueue(ep("f", "fast", 1, 1, 2))  // fW0 已提交 → 迟到拒绝
			eng2.Enqueue(ep("f", "fast", 10, 1, 3)) // fW1 未提交 → 计入
			eng2.Enqueue(ep("f", "fast", 20, 1, 4)) // fW2 未提交 → 计入
			// 等重放全部处理完（slow 的 Observe 已刷新其空闲计时），再断言水位线。
			waitFor(t, "重放4条处理完成", func() bool { return eng2.Stats().Received == 4 })
			// slow 恢复为 active 重新计时：水位线保持恢复下限 20s 不回退、也不冒进。
			if wm := eng2.Watermark(); wm != et(20) {
				t.Fatalf("slow 重新计时期间水位线应保持20s，实际 %ds", wm.Unix())
			}
			t.Logf("[恢复] idle 分片恢复为 active 重新计时；水位线保持20s（检查点兜底，不回退）")

			// slow 再次静默超阈值 → 重新判 idle：水位线照常随 fast 推进，fW1 定稿。
			time.Sleep(500 * time.Millisecond)
			eng2.Enqueue(ep("f", "fast", 30, 1, 5)) // 水位线→30s：fW1(定稿线25s) 定稿
			waitFor(t, "恢复后 fW1 定稿", func() bool { return persistentSink.Written() >= 3 })
			snap1 := eng2.Snapshot()
			logSnapshot(t, snap1, "恢复后-slow再空闲")
			if st := partitionStatuses(snap1)["slow"]; st != watermark.StatusIdle {
				t.Fatalf("slow 再次静默后应重新判 idle，实际 %s", st)
			}
			if snap1.Watermark != et(30) {
				t.Fatalf("slow 再空闲后水位线应随 fast 推进到30s，实际 %ds", snap1.Watermark.Unix())
			}
			t.Logf("[空闲判定] 恢复后 slow 再次静默超400ms → 重新 idle；水位线随 fast 到30s，fW1 照常定稿")

			// slow 恢复供数：范围内计入、重复去重、超范围拒绝。
			resume := []event.Event{
				ep("s", "slow", 22, 100, 6), // sW2(定稿线35s) 未定稿 → 计入
				ep("s", "slow", 8, 999, 7),  // sW0 已定稿 → 超范围迟到拒绝
				ep("s", "slow", 22, 100, 6), // 重复投递 → 去重跳过
			}
			for _, ev := range resume {
				eng2.Enqueue(ev)
			}
			waitFor(t, "slow 恢复供数3条处理完成", func() bool { return eng2.Stats().Received == 8 })
			logDecisions(t, eng2, resume)
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
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
			if st.LateDropped != 3 || st.DuplicateSkipped != 1 {
				t.Fatalf("恢复后期望迟到拒绝3（重放seq1/2+超范围seq7）、重复跳过1，实际 late=%d dup=%d",
					st.LateDropped, st.DuplicateSkipped)
			}
			if st.PartitionFinishedDropped != 0 {
				t.Fatalf("本场景无终止分片，partition_finished 拒绝应为0，实际 %d", st.PartitionFinishedDropped)
			}
			t.Logf("[结论] 并行度%d：idle 恢复前后语义保持（再空闲→水位线推进→恢复计入/去重/拒绝），聚合一致（窗口=%d）",
				par, len(out))
		})
	}
}

// checkSnapshotInvariants 校验快照内部一致性（同一时刻视图，各项不自相矛盾）：
//  1. 同一窗口不得同时出现在待定与已定稿清单；
//  2. 待定窗口的定稿线必须都 > 水位线，已定稿窗口的定稿线必须都 ≤ 水位线；
//  3. Drops 按原因计数与 Stats 中对应计数一致；
//  4. 事件守恒：接收 = 计入 + 重复跳过 + 迟到拒绝 + 终止分片拒绝；
//  5. 全局水位线不低于各 active 分片水位线的最小值（单调性）。
func checkSnapshotInvariants(cfg Config, snap Snapshot) error {
	pending := make(map[checkpoint.WindowRef]bool, len(snap.PendingWindows))
	for _, ref := range snap.PendingWindows {
		pending[ref] = true
		if !ref.End.Add(cfg.AllowedLateness).After(snap.Watermark) {
			return fmt.Errorf("待定窗口 %+v 定稿线 %v 未超过水位线 %v（应已定稿）",
				ref, ref.End.Add(cfg.AllowedLateness).Unix(), snap.Watermark.Unix())
		}
	}
	for _, ref := range snap.CommittedWindows {
		if pending[ref] {
			return fmt.Errorf("窗口 %+v 同时出现在待定与已定稿清单", ref)
		}
		if snap.Watermark.Before(ref.End.Add(cfg.AllowedLateness)) {
			return fmt.Errorf("已定稿窗口 %+v 定稿线 %v 超过水位线 %v（不应已定稿）",
				ref, ref.End.Add(cfg.AllowedLateness).Unix(), snap.Watermark.Unix())
		}
	}
	if snap.Drops[event.ReasonLateBeyondAllowed] != snap.Stats.LateDropped ||
		snap.Drops[event.ReasonBackpressureFull] != snap.Stats.BackpressureDropped ||
		snap.Drops[event.ReasonPartitionFinished] != snap.Stats.PartitionFinishedDropped {
		return fmt.Errorf("Drops 与 Stats 计数不一致: drops=%v stats=%+v", snap.Drops, snap.Stats)
	}
	if sum := snap.Stats.Accepted + snap.Stats.DuplicateSkipped +
		snap.Stats.LateDropped + snap.Stats.PartitionFinishedDropped; sum != snap.Stats.Received {
		return fmt.Errorf("事件不守恒: received=%d 但 accepted+dup+late+finished=%d", snap.Stats.Received, sum)
	}
	var minActive time.Time
	hasActive := false
	for _, p := range snap.Partitions {
		if p.Status != watermark.StatusActive || p.MaxObserved.IsZero() {
			continue
		}
		if !hasActive || p.Watermark.Before(minActive) {
			minActive = p.Watermark
			hasActive = true
		}
	}
	if hasActive && snap.Watermark.Before(minActive) {
		return fmt.Errorf("全局水位线 %v 低于 active 分片最小水位线 %v（违反单调性）",
			snap.Watermark.Unix(), minActive.Unix())
	}
	return nil
}

// TestSnapshotConsistentUnderConcurrency 并发处理期间反复取快照：
// 每份快照都必须是同一时刻的一致视图（满足 checkSnapshotInvariants 全部不变式），
// 且静止后连续两份快照内容稳定一致。换并行度（1/2/4）均成立。
func TestSnapshotConsistentUnderConcurrency(t *testing.T) {
	const total = 400 // 单写入者有序事件流：并发来自多分片处理 + 多快照协程
	for _, par := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("并行度%d", par), func(t *testing.T) {
			cfg := Config{
				WindowSize: winSize, MaxOutOfOrderness: 5 * time.Second,
				AllowedLateness: 5 * time.Second, InputBuffer: 2048,
				Parallelism: par, // IdleTimeout=0：关闭空闲检测，隔离变量
			}
			snk, eng := newTestEngine(t, cfg)

			// 先让 4 个数据源分片各出一条并完成注册（水位线单调不减，
			// 晚注册分片的早事件会被判迟到——注册必须先于水位线推进确定下来）。
			for i := 0; i < 4; i++ {
				eng.Enqueue(event.Event{
					Key: fmt.Sprintf("k%d", i%8), Partition: fmt.Sprintf("p%d", i%4),
					Timestamp: et(i), Value: 1, Seq: int64(i),
				})
			}
			waitFor(t, "4个分片完成注册", func() bool { return eng.Stats().Accepted == 4 })

			// 期望聚合：事件 i（ts=i 秒，value=1）按 key=i%8 归属窗口 [i/10*10,+10)。
			want := map[resultKey]expectedAgg{}
			for i := 0; i < total; i++ {
				k := resultKey{key: fmt.Sprintf("k%d", i%8), start: int64(i / 10 * 10)}
				agg := want[k]
				agg.sum++
				agg.count++
				agg.seqs = append(agg.seqs, int64(i))
				want[k] = agg
			}

			// 3 个快照协程在注入期间持续取快照并校验不变式。
			stopSnap := make(chan struct{})
			errCh := make(chan error, 8)
			var checked atomic.Int64
			var snapWg sync.WaitGroup
			for g := 0; g < 3; g++ {
				snapWg.Add(1)
				go func() {
					defer snapWg.Done()
					for {
						select {
						case <-stopSnap:
							return
						default:
						}
						snap := eng.Snapshot()
						if err := checkSnapshotInvariants(cfg, snap); err != nil {
							select {
							case errCh <- err:
							default:
							}
							return
						}
						checked.Add(1)
					}
				}()
			}

			// 单写入者有序注入（每10条重投一次，验证并发下去重不多算）。
			// 前4条已注入并完成注册，这里从 i=4 继续。
			dups := 0
			for i := 4; i < total; i++ {
				if i%10 == 0 {
					dups++
				}
			}
			go func() {
				for i := 4; i < total; i++ {
					ev := event.Event{
						Key: fmt.Sprintf("k%d", i%8), Partition: fmt.Sprintf("p%d", i%4),
						Timestamp: et(i), Value: 1, Seq: int64(i),
					}
					if !eng.Enqueue(ev) {
						select {
						case errCh <- fmt.Errorf("事件 seq=%d 被背压拒绝（缓冲足够大，不应发生）", i):
						default:
						}
						return
					}
					if i%10 == 0 {
						eng.Enqueue(ev) // 重复投递：应被 Seq 去重
					}
					if i%16 == 0 {
						time.Sleep(2 * time.Millisecond) // 让快照协程穿插执行
					}
				}
			}()

			// 等待注入与处理完成（received 目标 = 400 原始 + 39 重投）。
			waitFor(t, "全部事件处理完成", func() bool { return eng.Stats().Received == int64(total+dups) })
			close(stopSnap)
			snapWg.Wait()
			select {
			case err := <-errCh:
				t.Fatalf("并发快照一致性校验失败: %v", err)
			default:
			}
			if n := checked.Load(); n < 20 {
				t.Fatalf("并发期间有效快照次数过少（%d），覆盖不足", n)
			}
			t.Logf("[快照一致性] 并行度%d：并发处理期间共校验 %d 份快照，不变式全部成立", par, checked.Load())

			// 静止后连续两份快照应内容稳定一致（分片 LastEventAt 为墙钟，除外）。
			snapA := eng.Snapshot()
			snapB := eng.Snapshot()
			if err := checkSnapshotInvariants(cfg, snapA); err != nil {
				t.Fatalf("静止快照不一致: %v", err)
			}
			if snapA.Watermark != snapB.Watermark || snapA.MaxSeq != snapB.MaxSeq ||
				snapA.Stats != snapB.Stats ||
				fmt.Sprint(snapA.PendingWindows) != fmt.Sprint(snapB.PendingWindows) ||
				fmt.Sprint(snapA.CommittedWindows) != fmt.Sprint(snapB.CommittedWindows) ||
				fmt.Sprint(snapA.Drops) != fmt.Sprint(snapB.Drops) {
				t.Fatalf("静止后连续两份快照应稳定一致:\nA=%+v\nB=%+v", snapA, snapB)
			}
			logSnapshot(t, snapA, "静止-稳定")
			t.Logf("[快照稳定性] 静止后连续两份快照一致：水位线=%ds 待定=%d 已定稿=%d 拒绝计数=%v",
				snapA.Watermark.Unix(), len(snapA.PendingWindows), len(snapA.CommittedWindows), snapA.Drops)

			if err := eng.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(snk, eng)
			logResults(t, out, st, drops)
			assertAggsEqual(t, want, out)
			if st.DuplicateSkipped != int64(dups) {
				t.Fatalf("重复投递应去重 %d 次，实际 %d", dups, st.DuplicateSkipped)
			}
			if st.LateDropped != 0 || st.BackpressureDropped != 0 || st.PartitionFinishedDropped != 0 {
				t.Fatalf("有序流不应有任何拒绝: late=%d bp=%d pf=%d",
					st.LateDropped, st.BackpressureDropped, st.PartitionFinishedDropped)
			}
			t.Logf("[结论] 并行度%d：并发快照一致 + 最终聚合正确（窗口=%d 去重=%d）",
				par, len(out), st.DuplicateSkipped)
		})
	}
}
