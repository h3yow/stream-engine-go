package engine

import (
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

// TestBackpressureDropsBounded 下游变慢时：
//   - 入口/分片缓冲有界，注入远快于消费时必须产生背压拒绝，而非无限增长；
//   - 每条事件有且仅有一个归宿：计入/重复/迟到/背压拒绝，无静默丢失；
//   - 拒绝原因可区分（backpressure_buffer_full）且有计数与明细；
//   - Close 必须在合理时间内完成（无死锁）。
func TestBackpressureDropsBounded(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0,
		InputBuffer:     2, // in 与每个分片 ch 均为 2：在途上限 2 + 分片数*2
		Parallelism:     1,
	}
	snk := sink.NewCollectingSink(5 * time.Millisecond) // 每窗口写出 5ms：慢下游
	eng := New(cfg, snk, nil)
	if err := eng.Start(tBackground()); err != nil {
		t.Fatal(err)
	}

	const attempts = 300
	var enqueued int64
	for i := 0; i < attempts; i++ {
		// 时间戳每个窗口递增一条：处理每条都会定稿前一窗口并阻塞写下游。
		if eng.Enqueue(e("k", i*10, i+1, i+1)) {
			enqueued++
		}
	}
	st := eng.Stats()
	t.Logf("[背压注入] 尝试=%d 进入缓冲=%d 已背压拒绝=%d（注入瞬间快照）",
		attempts, enqueued, st.BackpressureDropped)
	if st.BackpressureDropped == 0 {
		t.Fatal("慢下游 + 极小有界缓冲下必须出现背压拒绝，实际为 0（缓冲可能无界）")
	}

	closed := make(chan struct{})
	go func() { _ = eng.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close 在慢下游下超时：疑似死锁")
	}

	out, st2, drops, _ := collect(snk, eng)
	logResults(t, out, st2, drops)

	// 恒等式：尝试次数 = 已接收处理 + 背压拒绝（每条事件恰好一个归宿）。
	if st2.Received+st2.BackpressureDropped != attempts {
		t.Fatalf("事件守恒被破坏：接收=%d 背压拒绝=%d，尝试=%d",
			st2.Received, st2.BackpressureDropped, attempts)
	}
	if st2.LateDropped != 0 || st2.DuplicateSkipped != 0 {
		t.Fatalf("单调递增时间戳流不应有迟到/重复：late=%d dup=%d",
			st2.LateDropped, st2.DuplicateSkipped)
	}
	// 每个被接收的事件都对应一个窗口（运行中定稿或 Close 冲刷），不重不漏。
	if st2.WindowsEmitted != st2.Accepted || snk.Written() != st2.Accepted {
		t.Fatalf("窗口/写出数应等于接收数：accepted=%d emitted=%d written=%d",
			st2.Accepted, st2.WindowsEmitted, snk.Written())
	}
	for _, d := range drops {
		if d.Reason != event.ReasonBackpressureFull {
			t.Fatalf("本场景拒绝原因应全部为背压，实际 %s", d.Reason)
		}
	}
	if int64(len(drops)) != st2.BackpressureDropped {
		t.Fatalf("背压明细数=%d 与计数=%d 不一致", len(drops), st2.BackpressureDropped)
	}
	t.Logf("[有界性结论] 在途上限 = in(%d) + 分片数%d*ch(%d) = %d 条事件；拒绝=%d 全部可区分计数",
		cfg.InputBuffer, cfg.Parallelism, cfg.InputBuffer,
		cfg.InputBuffer+cfg.Parallelism*cfg.InputBuffer, st2.BackpressureDropped)
}

// TestInOrderPerKeyDelivery 多 key、多分片并发下，同一 key 的窗口结果必须按
// 窗口起始时间升序交付（不同 key 之间允许交错）。
func TestInOrderPerKeyDelivery(t *testing.T) {
	cfg := Config{
		WindowSize: winSize, MaxOutOfOrderness: 0,
		AllowedLateness: 0, InputBuffer: 64, Parallelism: 4,
	}
	snk, eng := newTestEngine(t, cfg)
	keys := []string{"a", "b", "c", "d"}
	// 每个 key 10 个窗口，事件乱序穿插注入；时间戳单调推进以定稿旧窗口。
	for w := 0; w < 10; w++ {
		for j, k := range keys {
			eng.Enqueue(e(k, w*10+1, 1, w*len(keys)+j+1))
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	last := map[string]int64{}
	for _, r := range snk.Results() {
		if prev, ok := last[r.Key]; ok && r.Window.Start.Unix() <= prev {
			t.Fatalf("key=%s 窗口乱序交付：上一窗口起始=%ds，当前=%ds",
				r.Key, prev, r.Window.Start.Unix())
		}
		last[r.Key] = r.Window.Start.Unix()
	}
	if len(last) != 4 {
		t.Fatalf("期望 4 个 key 都有输出，实际 %d 个", len(last))
	}
	for k, end := range last {
		if end != 90 {
			t.Fatalf("key=%s 最后窗口起始期望90s，实际 %ds", k, end)
		}
	}
	t.Logf("[顺序交付] 4 个 key 各 10 个窗口均按窗口起始时间升序写出")
}
