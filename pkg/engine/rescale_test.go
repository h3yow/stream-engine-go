package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// rescaleKeys 与 rescaleRounds 定义 4 key × 5 时间轮次的确定性事件流。
var rescaleKeys = []string{"k0", "k1", "k2", "k3"}
var rescaleRounds = []int{1, 11, 21, 31, 41}

// rescaleEvents 按时间轮次交错生成事件（轮次 r、key 下标 i → seq = r*4+i+1）。
func rescaleEvents() []event.Event {
	var evs []event.Event
	for r, sec := range rescaleRounds {
		for i, key := range rescaleKeys {
			seq := r*len(rescaleKeys) + i + 1
			evs = append(evs, e(key, sec, seq, seq))
		}
	}
	return evs
}

// rescaleWant 手工构造全量事件的期望聚合（每 key 5 个窗口，每窗口 1 条事件）。
func rescaleWant() map[resultKey]expectedAgg {
	want := make(map[resultKey]expectedAgg)
	for r, sec := range rescaleRounds {
		for i, key := range rescaleKeys {
			seq := int64(r*len(rescaleKeys) + i + 1)
			want[resultKey{key, int64(sec / 10 * 10)}] =
				expectedAgg{sum: seq, count: 1, seqs: []int64{seq}}
		}
	}
	return want
}

func rescaleConfig(parallelism int) Config {
	return Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: 256, Parallelism: parallelism,
	}
}

// enqueueRounds 按轮次（每轮 4 条同时间戳事件）注入并逐轮排空，
// 使跨分片处理顺序不影响水位线判定，测试可确定性复现。
func enqueueRounds(t *testing.T, eng *Engine, evs []event.Event) {
	t.Helper()
	round := len(rescaleKeys)
	for i := 0; i < len(evs); i += round {
		end := i + round
		if end > len(evs) {
			end = len(evs)
		}
		for _, ev := range evs[i:end] {
			if !eng.Enqueue(ev) {
				t.Fatalf("Enqueue 失败: %+v", ev)
			}
		}
		drain(eng)
	}
}

// TestRescaleRecovery 验证扩缩容恢复：从并行度 P 的检查点以并行度 Q 恢复
// （扩容 2→4、缩容 4→2、同构 2→2），重放后最终结果与参考运行完全一致，
// 已提交窗口不重复输出也不丢失。
func TestRescaleRecovery(t *testing.T) {
	all := rescaleEvents() // 20 条：4 key × 5 窗口
	for _, tc := range []struct {
		name string
		from int
		to   int
	}{
		{"扩容2to4", 2, 4},
		{"缩容4to2", 4, 2},
		{"同构2to2", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 参考：目标并行度下一次完整运行（逐轮排空，确定性）。
			refSnk, refEng := newTestEngine(t, rescaleConfig(tc.to))
			enqueueRounds(t, refEng, all)
			if err := refEng.Close(); err != nil {
				t.Fatal(err)
			}
			refOut, _, _, _ := collect(refSnk, refEng)
			assertAggsEqual(t, rescaleWant(), refOut)

			// 第一次运行：from 并行度，注入前 3 轮（每 key 前 3 个窗口的事件），
			// 显式检查点后 Abort 模拟崩溃。
			store := checkpoint.NewMemoryStore()
			persistentSink := sink.NewCollectingSink(0)
			eng1 := New(rescaleConfig(tc.from), persistentSink, store)
			if err := eng1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			enqueueRounds(t, eng1, all[:12])
			cp, err := eng1.Checkpoint(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			logCheckpoint(t, cp)
			// 水位线=21s：W[0,10)、W[10,20) 已定稿（4 key × 2 窗口 = 8 个）。
			if len(cp.CommittedWindows) != 8 || cp.Parallelism != tc.from {
				t.Fatalf("检查点期望8个已提交窗口、并行度%d，实际 committed=%d parallelism=%d",
					tc.from, len(cp.CommittedWindows), cp.Parallelism)
			}
			beforeWritten := persistentSink.Written()
			eng1.Abort()
			t.Logf("[崩溃] 并行度%d 运行 Abort，崩溃前下游已写 %d 条", tc.from, beforeWritten)

			// 第二次运行：to 并行度，从同一检查点恢复，从头重放全部事件。
			eng2 := New(rescaleConfig(tc.to), persistentSink, store)
			if err := eng2.Start(context.Background()); err != nil {
				t.Fatalf("扩缩容恢复应成功: %v", err)
			}
			ri := eng2.RestoreInfo()
			t.Logf("[恢复] 检查点#%d 并行度%d→%d rescaled=%v 已提交窗口=%d 个",
				ri.CheckpointID, ri.FromParallelism, ri.ToParallelism,
				ri.Rescaled, ri.CommittedWindows)
			if !ri.Restored || ri.FromParallelism != tc.from || ri.ToParallelism != tc.to ||
				ri.Rescaled != (tc.from != tc.to) || ri.CommittedWindows != 8 {
				t.Fatalf("RestoreInfo 异常: %+v", ri)
			}
			enqueueRounds(t, eng2, all)
			if err := eng2.Close(); err != nil {
				t.Fatal(err)
			}
			out, st, drops, _ := collect(persistentSink, eng2)
			logResults(t, out, st, drops)

			// 结果等价：与参考运行（to 并行度下一次跑完）完全一致。
			assertAggsEqual(t, refOut, out)
			// 不重不丢：崩溃前8 + 恢复后12 = 20 条窗口输出，下游无重复跳过。
			if persistentSink.Written() != 20 {
				t.Fatalf("期望总写出20条窗口，实际=%d", persistentSink.Written())
			}
			if persistentSink.DuplicateSkips() != 0 {
				t.Fatalf("已提交窗口应由引擎层拦截，下游重复跳过期望0，实际=%d",
					persistentSink.DuplicateSkips())
			}
			// 重放的 ts=1/11 事件属于已提交窗口，按超范围迟到拒绝（4key×2轮=8条）。
			if st.LateDropped != 8 {
				t.Fatalf("重放中已提交窗口的事件应拒绝8条，实际=%d", st.LateDropped)
			}
			t.Logf("[扩缩容核对] 并行度%d→%d：20窗口结果与参考运行一致，不重不丢",
				tc.from, tc.to)
		})
	}
}

// TestIncompatibleCheckpointRejected 验证无法安全恢复的检查点被明确拒绝，
// 而不是悄悄接着算。
func TestIncompatibleCheckpointRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		cp   *checkpoint.Checkpoint
	}{
		{"旧格式缺少并行度元数据", &checkpoint.Checkpoint{
			ID: 1, Version: 1, Parallelism: 0, // 初版格式：未记录并行度
			Watermark: et(10), MaxSeq: 5,
			CommittedWindows: []checkpoint.WindowRef{{Key: "k", Start: et(0), End: et(10)}},
		}},
		{"版本高于当前引擎", &checkpoint.Checkpoint{
			ID: 2, Version: checkpoint.CurrentVersion + 1, Parallelism: 2,
			Watermark: et(10),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := checkpoint.NewMemoryStore()
			if err := store.Save(context.Background(), tc.cp); err != nil {
				t.Fatal(err)
			}
			eng := New(rescaleConfig(4), sink.NewCollectingSink(0), store)
			err := eng.Start(context.Background())
			if !errors.Is(err, ErrIncompatibleCheckpoint) {
				t.Fatalf("期望 ErrIncompatibleCheckpoint，实际=%v", err)
			}
			t.Logf("[恢复拒绝] %s → %v", tc.name, err)
		})
	}
}

// TestRescaleResultEqualsAnyParallelism 同一批数据在并行度 1/2/4 下分别从头跑，
// 再与“并行度2 检查点 → 并行度4 恢复”的结果对比，四者必须一致。
func TestRescaleResultEqualsAnyParallelism(t *testing.T) {
	all := rescaleEvents()
	want := rescaleWant()
	for _, p := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("并行度%d从头跑", p), func(t *testing.T) {
			snk, eng := newTestEngine(t, rescaleConfig(p))
			enqueueRounds(t, eng, all)
			if err := eng.Close(); err != nil {
				t.Fatal(err)
			}
			out, _, _, _ := collect(snk, eng)
			assertAggsEqual(t, want, out)
		})
	}

	// 并行度2 跑 7 条（第1轮 + 第2轮前3条）→ 检查点 → Abort → 并行度4 恢复重放。
	store := checkpoint.NewMemoryStore()
	persistentSink := sink.NewCollectingSink(0)
	eng1 := New(rescaleConfig(2), persistentSink, store)
	if err := eng1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	enqueueRounds(t, eng1, all[:4])
	enqueueRounds(t, eng1, all[4:7])
	cp, err := eng1.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[检查点] 并行度2 跑到第2轮中段：watermark=%ds 已提交窗口=%d 个",
		cp.Watermark.Unix(), len(cp.CommittedWindows))
	eng1.Abort()

	eng2 := New(rescaleConfig(4), persistentSink, store)
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ri := eng2.RestoreInfo(); !ri.Rescaled {
		t.Fatalf("应识别为扩缩容恢复: %+v", ri)
	}
	enqueueRounds(t, eng2, all)
	if err := eng2.Close(); err != nil {
		t.Fatal(err)
	}
	out, _, _, _ := collect(persistentSink, eng2)
	assertAggsEqual(t, want, out)
	t.Logf("[扩缩容核对] 2→4恢复结果 == 并行度1/2/4各自从头跑的结果")
}
