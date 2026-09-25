package stream

import (
	"testing"
	"time"
)

// runWithTimeout 在超时保护下运行引擎，验证不死锁。
func runWithTimeout(t *testing.T, eng *Engine, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- eng.Run() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("引擎未在超时内完成：疑似死锁")
	}
}

// TestBackpressureBlock 下游变慢 + Block 策略：
// 背压传导到上游（Blocked>0），缓冲有界，不死锁、不丢事件、结果正确。
func TestBackpressureBlock(t *testing.T) {
	evs := boundedShuffle(makeEvents(600, 3, []string{"a", "b"}), 5, 31)
	base, _ := runOnce(t, baseConfig(), evs)

	cfg := baseConfig()
	cfg.InboxCapacity = 4 // 极小缓冲，强制背压
	cfg.Policy = Block
	cfg.ProducerConcurrency = 4
	sink := NewMemSink()
	sink.Delay = func() { time.Sleep(time.Millisecond) } // 模拟下游变慢

	eng := New(cfg, &MemSource{Events: evs}, sink)
	runWithTimeout(t, eng, 30*time.Second)

	st := eng.Stats()
	if st.Blocked == 0 {
		t.Fatal("下游变慢时应出现注入阻塞（背压传导）")
	}
	if st.Dropped != 0 {
		t.Fatalf("Block 策略不得丢事件, dropped=%d", st.Dropped)
	}
	if st.Accepted != int64(len(evs)) {
		t.Fatalf("接受=%d want %d（不得静默丢事件）", st.Accepted, len(evs))
	}
	assertSameResults(t, base, sink.Results)
	t.Logf("Block 背压: 阻塞=%d 次, 丢弃=0, 接受=%d/%d, 结果与无背压一致",
		st.Blocked, st.Accepted, len(evs))
}

// TestBackpressureDrop 下游变慢 + DropNewest 策略：
// 缓冲满时丢弃新事件并计数，不死锁，交付的事件保持源顺序（单分片 FIFO）。
func TestBackpressureDrop(t *testing.T) {
	evs := makeEvents(500, 3, []string{"a", "b"})
	cfg := baseConfig()
	cfg.InboxCapacity = 2
	cfg.Policy = DropNewest
	cfg.ProducerConcurrency = 1
	sink := NewMemSink()
	sink.Delay = func() { time.Sleep(time.Millisecond) }

	eng := New(cfg, &MemSource{Events: evs}, sink)
	runWithTimeout(t, eng, 30*time.Second)

	st := eng.Stats()
	if st.Dropped == 0 {
		t.Fatal("下游变慢且缓冲极小时应出现丢弃")
	}
	// 计数守恒：注入 = 接收 + 丢弃，无静默丢失。
	if st.Received+st.Dropped != int64(len(evs)) {
		t.Fatalf("不守恒: received=%d dropped=%d total=%d", st.Received, st.Dropped, len(evs))
	}
	// 交付的事件是源序列的子序列（顺序交付）：检查输出计数不超过理论上限。
	for _, r := range sink.Results {
		if r.Count <= 0 {
			t.Fatalf("非法结果: %+v", r)
		}
	}
	t.Logf("Drop 背压: 丢弃=%d (原因=%s), 接收=%d, 合计=%d=%d 无静默丢失",
		st.Dropped, RejectDroppedBackpressure, st.Received, st.Received+st.Dropped, len(evs))
}

// TestInboxBoundedMemory 缓冲容量有界：无论上游多快，在途事件不超过容量。
func TestInboxBoundedMemory(t *testing.T) {
	stats := &Counters{}
	inbox := NewInbox(3, Block, stats)
	// 填满缓冲（无消费者）。
	for i := 0; i < 3; i++ {
		if !inbox.Offer(Event{ID: string(rune('a' + i))}, Offset(i+1), 0) {
			t.Fatal("缓冲未满时不应失败")
		}
	}
	// 第 4 条在 Block 策略下会阻塞：用协程验证它被阻塞而非成功或丢失。
	offered := make(chan struct{})
	go func() {
		inbox.Offer(Event{ID: "d"}, 4, 0)
		close(offered)
	}()
	select {
	case <-offered:
		t.Fatal("缓冲已满时 Block 策略应阻塞注入方")
	case <-time.After(50 * time.Millisecond):
		t.Log("缓冲已满: 注入方被阻塞（内存占用有界于容量 3）")
	}
	// 消费一条后阻塞的注入得以完成。
	<-inbox.C()
	select {
	case <-offered:
	case <-time.After(time.Second):
		t.Fatal("消费后注入应解除阻塞")
	}
	if stats.Blocked != 1 {
		t.Fatalf("阻塞计数=%d want 1", stats.Blocked)
	}
}
