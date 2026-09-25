package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

const winSize = 10 * time.Second

func et(n int) time.Time { return time.Unix(int64(n), 0).UTC() }

func e(key string, sec, value, seq int) event.Event {
	return event.Event{Key: key, Timestamp: et(sec), Value: int64(value), Seq: int64(seq)}
}

// resultKey 标识一个聚合结果。
type resultKey struct {
	key   string
	start int64
}

type expectedAgg struct {
	sum   int64
	count int64
	seqs  []int64
}

// runEngine 用给定配置注入事件（有序），Close 后返回聚合 map、统计与明细。
func runEngine(t *testing.T, cfg Config, events []event.Event,
) (map[resultKey]expectedAgg, Stats, []event.DropRecord, *sink.CollectingSink) {
	t.Helper()
	snk := sink.NewCollectingSink(0)
	eng := New(cfg, snk, nil)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if !eng.Enqueue(ev) {
			t.Errorf("Enqueue 返回 false（小缓冲测试不应背压丢弃）: %+v", ev)
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	return collect(snk, eng)
}

func collect(snk *sink.CollectingSink, eng *Engine) (map[resultKey]expectedAgg, Stats, []event.DropRecord, *sink.CollectingSink) {
	out := make(map[resultKey]expectedAgg)
	for _, r := range snk.Results() {
		k := resultKey{r.Key, r.Window.Start.Unix()}
		if _, dup := out[k]; dup {
			panic(fmt.Sprintf("窗口结果重复输出: %+v", k))
		}
		out[k] = expectedAgg{sum: r.Sum, count: r.Count, seqs: append([]int64(nil), r.Seqs...)}
	}
	return out, eng.Stats(), eng.Drops(), snk
}

func logResults(t *testing.T, out map[resultKey]expectedAgg, st Stats, drops []event.DropRecord) {
	t.Helper()
	keys := slices.SortedFunc(maps.Keys(out), func(a, b resultKey) int {
		if a.key != b.key {
			if a.key < b.key {
				return -1
			}
			return 1
		}
		if a.start < b.start {
			return -1
		}
		return 1
	})
	for _, k := range keys {
		v := out[k]
		t.Logf("[窗口结果] key=%s window=[%d,%d)s sum=%d count=%d seqs=%v",
			k.key, k.start, k.start+10, v.sum, v.count, v.seqs)
	}
	t.Logf("[引擎统计] 接收=%d 计入=%d 重复跳过=%d 迟到拒绝=%d 背压拒绝=%d 窗口输出=%d 检查点=%d",
		st.Received, st.Accepted, st.DuplicateSkipped, st.LateDropped,
		st.BackpressureDropped, st.WindowsEmitted, st.Checkpoints)
	for _, d := range drops {
		t.Logf("[拒绝明细] seq=%d key=%s eventTime=%ds 原因=%s",
			d.Event.Seq, d.Event.Key, d.Event.Timestamp.Unix(), d.Reason)
	}
}

// expectedSet 手工构造期望聚合。
func expectedSet(m map[resultKey]expectedAgg) map[resultKey]expectedAgg { return m }

// TestOutOfOrderAggregation 验证“在允许乱序包络内”的乱序到达不改变最终聚合。
//
// MaxOutOfOrderness 覆盖输入时间跨度（21s），因此任意排列下水位线都不会
// 在注入期间提前定稿窗口，所有窗口在 Close 时冲刷输出。
func TestOutOfOrderAggregation(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 25 * time.Second,
		AllowedLateness: 2 * time.Second, InputBuffer: 128, Parallelism: 1,
	}
	// 同一批事件，三种注入顺序。
	inOrder := []event.Event{
		e("a", 1, 10, 1), e("a", 6, 20, 2), e("a", 11, 30, 3),
		e("a", 16, 40, 4), e("a", 21, 50, 5),
	}
	outOfOrder := []event.Event{
		e("a", 21, 50, 5), e("a", 1, 10, 1), e("a", 16, 40, 4),
		e("a", 6, 20, 2), e("a", 11, 30, 3),
	}
	scrambled := []event.Event{
		e("a", 11, 30, 3), e("a", 1, 10, 1), e("a", 21, 50, 5),
		e("a", 16, 40, 4), e("a", 6, 20, 2),
	}
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "a", start: 0}:  {sum: 30, count: 2, seqs: []int64{1, 2}},
		{key: "a", start: 10}: {sum: 70, count: 2, seqs: []int64{3, 4}},
		{key: "a", start: 20}: {sum: 50, count: 1, seqs: []int64{5}},
	})
	for name, evs := range map[string][]event.Event{
		"顺序": inOrder, "倒乱序": outOfOrder, "完全打乱": scrambled,
	} {
		t.Run(name, func(t *testing.T) {
			got, st, drops, _ := runEngine(t, cfg, evs)
			logResults(t, got, st, drops)
			assertAggsEqual(t, want, got)
			if st.LateDropped != 0 || st.BackpressureDropped != 0 {
				t.Fatalf("本场景不应有拒绝：late=%d backpressure=%d",
					st.LateDropped, st.BackpressureDropped)
			}
		})
	}
}

func assertAggsEqual(t *testing.T, want, got map[resultKey]expectedAgg) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("窗口数量期望 %d，实际 %d", len(want), len(got))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("缺少窗口 %+v", k)
		}
		if g.sum != w.sum || g.count != w.count || !slices.Equal(g.seqs, w.seqs) {
			t.Fatalf("窗口 %+v 期望 sum=%d count=%d seqs=%v；实际 sum=%d count=%d seqs=%v",
				k, w.sum, w.count, w.seqs, g.sum, g.count, g.seqs)
		}
	}
}
