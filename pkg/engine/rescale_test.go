package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// rescaleConfig 下 MaxOutOfOrderness=0、AllowedLateness=0、关闭空闲检测：
// 水位线随事件时间推进，看到后续窗口首个事件时前一定稿。
func rescaleConfig(buf, parallelism int) Config {
	return Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: buf, Parallelism: parallelism,
	}
}

// rescaleEvents 生成 4 个 key × 4 个窗口的事件流（分属两个数据源分片），
// 按窗口分组连续排列：每 4 条同属一个事件时间。
func rescaleEvents() []event.Event {
	keys := []string{"a", "b", "c", "d"}
	var evs []event.Event
	seq := 0
	for w := 0; w < 4; w++ {
		for i, k := range keys {
			seq++
			part := "p0"
			if i%2 == 1 {
				part = "p1"
			}
			evs = append(evs, ep(k, part, w*10+1, w*10+i+1, seq))
		}
	}
	return evs
}

// feedPhases 按阶段（每 4 条同一事件时间）注入前 nPhases 个窗口的事件，
// 阶段间排空：保证任意并行度/交叉调度下处理顺序确定、结果可复现。
func feedPhases(eng *Engine, evs []event.Event, nPhases int) {
	const phaseSize = 4
	for p := 0; p < nPhases; p++ {
		for _, ev := range evs[p*phaseSize : (p+1)*phaseSize] {
			eng.Enqueue(ev)
		}
		drain(eng)
	}
}

// TestRescaleRecovery 从检查点以不同并行度恢复（扩容 2→4、缩容 4→1）：
// 已提交窗口不重复输出、未提交窗口不丢失，最终聚合与原并行度参考运行一致。
func TestRescaleRecovery(t *testing.T) {
	all := rescaleEvents()

	// 参考：并行度2 分阶段完整运行（与恢复运行同样的注入方式，保证可对比）。
	refCfg := rescaleConfig(128, 2)
	snkRef, engRef := newTestEngine(t, refCfg)
	feedPhases(engRef, all, 4)
	if err := engRef.Close(); err != nil {
		t.Fatal(err)
	}
	want, _, _, _ := collect(snkRef, engRef)
	if len(want) != 16 {
		t.Fatalf("参考运行应产出16个窗口，实际 %d", len(want))
	}

	for _, tc := range []struct{ from, to int }{{2, 4}, {4, 1}, {2, 2}} {
		t.Run(fmt.Sprintf("并行度%d→%d", tc.from, tc.to), func(t *testing.T) {
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)

			// 第一次运行：注入前两个窗口的事件后检查点，再模拟崩溃。
			eng1 := New(rescaleConfig(128, tc.from), persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			feedPhases(eng1, all, 2)
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			if cp.Parallelism != tc.from {
				t.Fatalf("检查点应记录拍摄时并行度%d，实际 %d", tc.from, cp.Parallelism)
			}
			if len(cp.CommittedWindows) != 4 {
				t.Fatalf("检查点应含4个已提交窗口(W0×4key)，实际 %d", len(cp.CommittedWindows))
			}
			beforeWritten := persistentSink.Written()
			eng1.Abort()
			t.Logf("[崩溃] 并行度%d 下 Abort，崩溃前下游已写 %d 条", tc.from, beforeWritten)

			// 第二次运行：换并行度从同一检查点恢复，从头分阶段重放全部事件。
			eng2 := New(rescaleConfig(128, tc.to), persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Logf("[扩缩容恢复] 检查点并行度=%d → 引擎并行度=%d；水位线=%ds 已提交窗口=%d 个，从头重放 %d 条事件",
				cp.Parallelism, tc.to, cp.Watermark.Unix(), len(cp.CommittedWindows), len(all))
			feedPhases(eng2, all, 4)
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
			logResults(t, out, st, drops)

			assertAggsEqual(t, want, out)
			if persistentSink.Written() != int64(len(want)) {
				t.Fatalf("总写出应等于窗口数%d（不重不丢），实际 %d", len(want), persistentSink.Written())
			}
			if persistentSink.DuplicateSkips() != 0 {
				t.Fatalf("已提交集合应拦截全部重复窗口，下游重复跳过期望0，实际 %d",
					persistentSink.DuplicateSkips())
			}
			if st.LateDropped != 4 {
				t.Fatalf("重放的4条已提交窗口事件应按超范围迟到拒绝，实际 %d", st.LateDropped)
			}
			t.Logf("[扩缩容结论] 并行度%d→%d 恢复后聚合与参考运行完全一致：窗口=%d 写出=%d 重放拒绝=%d",
				tc.from, tc.to, len(out), persistentSink.Written(), st.LateDropped)
		})
	}
}

// TestIncompatibleCheckpoint 窗口语义参数不一致的检查点必须被明确拒绝，
// 不得静默继续（否则窗口边界不同会算错）。旧格式检查点（未记录语义参数）同样拒绝。
func TestIncompatibleCheckpoint(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	snk := sink.NewCollectingSink(0)
	eng1 := New(rescaleConfig(128, 2), snk, store)
	if err := eng1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng1.Enqueue(ep("a", "p0", 1, 1, 1))
	eng1.Enqueue(ep("a", "p0", 11, 1, 2))
	drain(eng1)
	if _, err := eng1.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng1.Abort()

	// 窗口长度不同 → 不兼容。
	cfg2 := rescaleConfig(128, 2)
	cfg2.WindowSize = 20 * time.Second
	eng2 := New(cfg2, snk, store)
	err := eng2.Start(context.Background())
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("窗口长度变更后恢复应报 ErrIncompatibleCheckpoint，实际 %v", err)
	}
	t.Logf("[不兼容拒绝] %v", err)

	// 允许迟到时长不同 → 不兼容。
	cfg3 := rescaleConfig(128, 2)
	cfg3.AllowedLateness = time.Second
	eng3 := New(cfg3, snk, store)
	if err := eng3.Start(context.Background()); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("AllowedLateness 变更后恢复应报 ErrIncompatibleCheckpoint，实际 %v", err)
	}

	// 旧格式检查点（语义参数为零值）→ 不兼容，明确报错而非静默错算。
	legacyStore := checkpoint.NewMemoryStore()
	if err := legacyStore.Save(context.Background(), &checkpoint.Checkpoint{
		ID: 1, Watermark: et(11), MaxSeq: 2,
		CommittedWindows: []checkpoint.WindowRef{{Key: "a", Start: et(0), End: et(10)}},
	}); err != nil {
		t.Fatal(err)
	}
	eng4 := New(rescaleConfig(128, 2), snk, legacyStore)
	err = eng4.Start(context.Background())
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("旧格式检查点应报 ErrIncompatibleCheckpoint，实际 %v", err)
	}
	t.Logf("[旧格式拒绝] %v", err)

	// 相同语义参数（即使并行度不同）→ 兼容，正常恢复。
	eng5 := New(rescaleConfig(128, 4), snk, store)
	if err := eng5.Start(context.Background()); err != nil {
		t.Fatalf("语义参数一致（仅并行度不同）应允许恢复，实际 %v", err)
	}
	eng5.Abort()
	t.Logf("[兼容确认] 语义参数一致、并行度 2→4：正常恢复")
}
