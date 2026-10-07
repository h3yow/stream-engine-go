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

// validateSnapshot 校验一份快照是否构成“同一时刻的一致视图”：
// 各项之间不得自相矛盾。任何一条不满足都说明快照读到了撕裂的中间态。
// inRun=true 表示拍摄于运行期间（定稿不变式成立）；Close 冲刷后
// 已提交窗口的定稿线可以晚于水位线（冲刷不受水位线约束），不再校验第2/3条。
func validateSnapshot(t *testing.T, snap Snapshot, allowedLateness time.Duration, inRun bool, tag string) {
	t.Helper()
	// 1. 待定与已定稿清单不得重叠（同一窗口不能既待定又已定稿）。
	committed := make(map[checkpoint.WindowRef]struct{}, len(snap.CommittedWindows))
	for _, ref := range snap.CommittedWindows {
		committed[ref] = struct{}{}
		if inRun && snap.Watermark.Before(ref.End.Add(allowedLateness)) {
			t.Errorf("[快照一致性:%s] 已定稿窗口 key=%s [%d,%d)s 的定稿线(%ds)在水位线(%ds)之后",
				tag, ref.Key, ref.Start.Unix(), ref.End.Unix(),
				ref.End.Add(allowedLateness).Unix(), snap.Watermark.Unix())
		}
	}
	for _, ref := range snap.PendingWindows {
		if _, dup := committed[ref]; dup {
			t.Errorf("[快照一致性:%s] 窗口 key=%s [%d,%d)s 同时出现在待定与已定稿清单",
				tag, ref.Key, ref.Start.Unix(), ref.End.Unix())
		}
		if inRun && !snap.Watermark.Before(ref.End.Add(allowedLateness)) {
			t.Errorf("[快照一致性:%s] 待定窗口 key=%s [%d,%d)s 的定稿线(%ds)不晚于水位线(%ds)（应已定稿却不在已定稿清单）",
				tag, ref.Key, ref.Start.Unix(), ref.End.Unix(),
				ref.End.Add(allowedLateness).Unix(), snap.Watermark.Unix())
		}
	}
	// 2. 按原因的拒绝计数与 Stats 对应项必须一致。
	if snap.Drops[event.ReasonLateBeyondAllowed] != snap.Stats.LateDropped ||
		snap.Drops[event.ReasonBackpressureFull] != snap.Stats.BackpressureDropped ||
		snap.Drops[event.ReasonPartitionFinished] != snap.Stats.PartitionFinishedDropped {
		t.Errorf("[快照一致性:%s] Drops=%v 与 Stats=%+v 的拒绝计数不一致", tag, snap.Drops, snap.Stats)
	}
	// 3. 事件守恒：屏障点无在途事件，接收数必须精确分解为
	//    计入 + 重复跳过 + 迟到拒绝 + 终止拒绝（背压拒绝不进入处理管线）。
	st := snap.Stats
	if st.Received != st.Accepted+st.DuplicateSkipped+st.LateDropped+st.PartitionFinishedDropped {
		t.Errorf("[快照一致性:%s] 事件不守恒：接收=%d 计入=%d 重复=%d 迟到=%d 终止=%d",
			tag, st.Received, st.Accepted, st.DuplicateSkipped, st.LateDropped, st.PartitionFinishedDropped)
	}
	// 4. 分片状态枚举合法；未终止分片不得携带终止位点。
	for _, p := range snap.Partitions {
		switch p.Status {
		case watermark.StatusActive, watermark.StatusIdle:
			if p.HasFinishSeq {
				t.Errorf("[快照一致性:%s] 分片 %s 未终止却带终止位点 %+v", tag, p.ID, p)
			}
		case watermark.StatusFinished:
		default:
			t.Errorf("[快照一致性:%s] 分片 %s 状态非法: %q", tag, p.ID, p.Status)
		}
	}
}

// TestSnapshotConsistentUnderConcurrency 在并发注入 + 并发反复取快照的情况下，
// 每一份快照都必须是同一时刻的一致视图；最终聚合与确定性参考完全一致。
// 同一套用例在并行度 1/2/4 下分别验证。
func TestSnapshotConsistentUnderConcurrency(t *testing.T) {
	const allowedLateness = 5 * time.Second
	// 主事件：240 条，key 8 个、数据源分片 3 个轮换，事件时间 1..240s。
	var main []event.Event
	for i := 1; i <= 240; i++ {
		main = append(main, ep(fmt.Sprintf("k%d", i%8), fmt.Sprintf("p%d", i%3), i, 1, i))
	}
	// 期望聚合由事件流直接计算（每个窗口10条、覆盖全部8个key）。
	want := map[resultKey]expectedAgg{}
	for i := 1; i <= 240; i++ {
		rk := resultKey{key: fmt.Sprintf("k%d", i%8), start: int64(i / 10 * 10)}
		agg := want[rk]
		agg.sum++
		agg.count++
		agg.seqs = append(agg.seqs, int64(i)) // i 递增，seqs 天然有序
		want[rk] = agg
	}

	for _, parallelism := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("并行度%d", parallelism), func(t *testing.T) {
			cfg := Config{
				WindowSize: winSize, MaxOutOfOrderness: 5 * time.Second,
				AllowedLateness: allowedLateness, InputBuffer: 1024,
				Parallelism: parallelism, IdleTimeout: time.Hour, // 隔离空闲变量
			}
			// 轻微慢下游（1ms/次写出）放大处理与快照的并发交错。
			snk := sink.NewCollectingSink(time.Millisecond)
			eng := New(cfg, snk, nil)
			if err := eng.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			eng.FinishPartition("p9") // 从未供数就被停掉的分片

			// 快照 goroutine：整个注入期间反复取快照并逐份校验一致性。
			stopSnap := make(chan struct{})
			snapDone := make(chan struct{})
			snapCount := 0
			go func() {
				defer close(snapDone)
				prevWM := time.Time{}
				for {
					select {
					case <-stopSnap:
						return
					default:
					}
					snap := eng.Snapshot()
					validateSnapshot(t, snap, allowedLateness, true, fmt.Sprintf("并发#%d", snapCount))
					if snap.Watermark.Before(prevWM) {
						t.Errorf("[快照一致性:并发#%d] 水位线回退：%ds < %ds",
							snapCount, snap.Watermark.Unix(), prevWM.Unix())
					}
					prevWM = snap.Watermark
					for _, p := range snap.Partitions {
						if p.ID == "p9" && p.Status != watermark.StatusFinished {
							t.Errorf("[快照一致性:并发#%d] p9 必须始终 finished，实际 %s", snapCount, p.Status)
						}
					}
					snapCount++
					time.Sleep(2 * time.Millisecond)
				}
			}()

			// 分阶段并发注入：每阶段10条（时间跨度9s < 乱序包络），4 个写入 goroutine，
			// 阶段间排空——任意并行度/交叉调度下都不会误判迟到，结果可复现。
			for phase := 0; phase < 24; phase++ {
				concurrentEnqueue(eng, main[phase*10:(phase+1)*10], 4)
				drain(eng)
			}
			waitFor(t, "主事件全部计入", func() bool { return eng.Stats().Accepted == 240 })

			// 尾部事件（主事件全部计入后注入，判定确定）：
			//   5 条重复投递（最末窗口仍打开）→ 去重跳过；
			//   3 条超范围老事件（W0 已定稿）→ 迟到拒绝；
			//   10 条终止分片事件 → partition_finished 拒绝。
			for i := 235; i < 240; i++ {
				eng.Enqueue(main[i]) // seq236..240 原样重投
			}
			for i := 0; i < 3; i++ {
				eng.Enqueue(ep("k2", "p1", 1, 1, 241+i)) // ts=1s：W0 已定稿
			}
			for i := 0; i < 10; i++ {
				eng.Enqueue(ep("x", "p9", 2, 1, 244+i)) // p9 已终止
			}
			waitFor(t, "尾部事件全部处理", func() bool { return eng.Stats().Received == 258 })
			close(stopSnap)
			<-snapDone
			t.Logf("[快照并发] 并行度%d：处理期间共拍摄并校验 %d 份快照，全部一致", parallelism, snapCount)

			if err := eng.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(snk, eng)
			logResults(t, out, st, drops)
			assertAggsEqual(t, want, out)
			if st.Received != 258 || st.Accepted != 240 || st.DuplicateSkipped != 5 ||
				st.LateDropped != 3 || st.PartitionFinishedDropped != 10 || st.BackpressureDropped != 0 {
				t.Fatalf("统计异常: %+v", st)
			}

			// 关闭后的最终快照：无待定窗口，全部窗口已定稿，计数守恒。
			final := eng.Snapshot()
			logSnapshot(t, final, "关闭后")
			validateSnapshot(t, final, allowedLateness, false, "关闭后")
			if len(final.PendingWindows) != 0 {
				t.Fatalf("关闭后不应有待定窗口，实际 %d", len(final.PendingWindows))
			}
			if len(final.CommittedWindows) != len(want) {
				t.Fatalf("关闭后已定稿窗口应=%d，实际 %d", len(want), len(final.CommittedWindows))
			}
			t.Logf("[快照结论] 并行度%d：并发反复取快照均一致；最终聚合=%d 窗口与参考完全一致",
				parallelism, len(out))
		})
	}
}
