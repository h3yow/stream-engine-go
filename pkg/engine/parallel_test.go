package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

// concurrentEnqueue 用 n 个 goroutine 并发注入事件（每个事件投一次）。
func concurrentEnqueue(eng *Engine, evs []event.Event, n int) {
	jobs := make(chan event.Event)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range jobs {
				eng.Enqueue(ev)
			}
		}()
	}
	for _, ev := range evs {
		jobs <- ev
	}
	close(jobs)
	wg.Wait()
}

// TestSameResultAcrossParallelism 同一事件集合在不同分片数/并发注入度下
// 必须得到完全相同的窗口聚合，且无迟到、无背压丢弃。
func TestSameResultAcrossParallelism(t *testing.T) {
	base := []event.Event{
		e("alpha", 1, 10, 1), e("alpha", 6, 20, 2), e("alpha", 12, 30, 3),
		e("beta", 3, 1, 101), e("beta", 17, 2, 102), e("beta", 28, 3, 103),
		e("gamma", 5, 7, 201), e("gamma", 25, 9, 202),
	}
	// 打乱顺序 + 部分事件重复投递（去重后集合相同）。
	shuffled := []event.Event{
		base[5], base[0], base[7], base[2], base[1], base[6], base[3], base[4],
		base[0], base[4], base[7], // 重复：seq 1 / 102 / 202
	}
	want := expectedSet(map[resultKey]expectedAgg{
		{key: "alpha", start: 0}:  {sum: 30, count: 2, seqs: []int64{1, 2}},
		{key: "alpha", start: 10}: {sum: 30, count: 1, seqs: []int64{3}},
		{key: "beta", start: 0}:   {sum: 1, count: 1, seqs: []int64{101}},
		{key: "beta", start: 10}:  {sum: 2, count: 1, seqs: []int64{102}},
		{key: "beta", start: 20}:  {sum: 3, count: 1, seqs: []int64{103}},
		{key: "gamma", start: 0}:  {sum: 7, count: 1, seqs: []int64{201}},
		{key: "gamma", start: 20}: {sum: 9, count: 1, seqs: []int64{202}},
	})
	for _, shards := range []int{1, 2, 4, 8} {
		for _, writers := range []int{1, 4, 16} {
			name := fmt.Sprintf("分片%2d/并发写入%2d", shards, writers)
			t.Run(name, func(t *testing.T) {
				cfg := Config{
					WindowSize: winSize, MaxOutOfOrderness: 40 * time.Second,
					AllowedLateness: 5 * time.Second,
					InputBuffer:     256, Parallelism: shards,
				}
				snk, eng := newTestEngine(t, cfg)
				if writers == 1 {
					for _, ev := range shuffled {
						eng.Enqueue(ev)
					}
				} else {
					concurrentEnqueue(eng, shuffled, writers)
				}
				if err := eng.Close(); err != nil {
					t.Fatal(err)
				}
				got, st, drops, _ := collect(snk, eng)
				assertAggsEqual(t, want, got)
				if st.Received != int64(len(shuffled)) || st.Accepted != int64(len(base)) ||
					st.DuplicateSkipped != 3 || st.LateDropped != 0 || st.BackpressureDropped != 0 {
					t.Fatalf("统计异常: %+v drops=%+v", st, drops)
				}
				logResults(t, got, st, drops)
			})
		}
	}
}
