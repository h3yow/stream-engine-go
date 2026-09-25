package stream

import (
	"fmt"
	"testing"
)

// baseConfig 返回测试用基准配置。
// 确定性契约：实际乱序幅度 < OutOfOrderness+AllowedLateness(=80ms) 时，
// 结果与到达顺序、注入并发度无关。
func baseConfig() Config {
	return Config{
		WindowSize:      100,
		OutOfOrderness:  50,
		AllowedLateness: 30,
		InboxCapacity:   16,
		Policy:          Block,
	}
}

// runOnce 以给定事件序列与配置跑一次引擎，返回结果与计数。
func runOnce(t *testing.T, cfg Config, evs []Event) ([]WindowResult, Counters) {
	t.Helper()
	sink := NewMemSink()
	eng := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return sink.Results, eng.Stats()
}

// TestOrderIndependence 声明乱序上界内的任意到达顺序，结果一致。
func TestOrderIndependence(t *testing.T) {
	keys := []string{"a", "b", "c"}
	evs := makeEvents(300, 7, keys) // 事件时间 0..2093，跨 21 个窗口
	base, baseStats := runOnce(t, baseConfig(), evs)
	t.Logf("基准(顺序到达): 结果=%d 接受=%d", len(base), baseStats.Accepted)

	// 位移 7 个位置 * 7ms = 49ms < 80ms 上界
	for _, seed := range []int64{1, 7, 42, 1999} {
		got, gotStats := runOnce(t, baseConfig(), boundedShuffle(evs, 7, seed))
		assertSameResults(t, base, got)
		if gotStats.Accepted != baseStats.Accepted {
			t.Fatalf("seed=%d 接受数不同: want %d got %d", seed, baseStats.Accepted, gotStats.Accepted)
		}
		t.Logf("seed=%d 有界乱序到达: 结果与基准一致 (接受=%d)", seed, gotStats.Accepted)
	}
}

// TestConcurrencyIndependence 同一输入在不同注入并发度与缓冲容量下结果一致。
func TestConcurrencyIndependence(t *testing.T) {
	keys := []string{"a", "b"}
	evs := boundedShuffle(makeEvents(500, 5, keys), 7, 9) // 35ms < 80ms
	base, _ := runOnce(t, baseConfig(), evs)

	for _, shards := range []int{1, 2, 4, 8} {
		for _, cap_ := range []int{1, 3, 64} {
			cfg := baseConfig()
			cfg.ProducerConcurrency = shards
			cfg.InboxCapacity = cap_
			got, _ := runOnce(t, cfg, evs)
			assertSameResults(t, base, got)
			t.Logf("并发度=%d 缓冲=%d: 结果一致", shards, cap_)
		}
	}
}

// TestDuplicateDelivery 重复投递与只投递一次结果一致。
func TestDuplicateDelivery(t *testing.T) {
	keys := []string{"x", "y"}
	evs := boundedShuffle(makeEvents(200, 11, keys), 5, 3) // 55ms < 80ms
	base, baseStats := runOnce(t, baseConfig(), evs)

	got, gotStats := runOnce(t, baseConfig(), duplicated(evs, 5, 5))
	assertSameResults(t, base, got)
	if gotStats.Duplicates != int64(len(evs)) {
		t.Fatalf("重复计数=%d want %d", gotStats.Duplicates, len(evs))
	}
	if gotStats.Accepted != baseStats.Accepted {
		t.Fatalf("重复投递后接受数变化: want %d got %d", baseStats.Accepted, gotStats.Accepted)
	}
	t.Logf("重复投递: 去重=%d 接受=%d, 结果与单次投递一致", gotStats.Duplicates, gotStats.Accepted)
}

// TestLateEventsEndToEnd 端到端迟到处理：范围内计入、范围外拒绝且原因可区分。
func TestLateEventsEndToEnd(t *testing.T) {
	cfg := baseConfig()
	cfg.Logger = testLogger(t)
	evs := []Event{
		{ID: "e1", Key: "k", EventTime: 1000, Value: 1},
		{ID: "e2", Key: "k", EventTime: 1090, Value: 2},
		{ID: "e3", Key: "k", EventTime: 1180, Value: 4},  // 水位线推进到 1180-50=1130，下限 1100
		{ID: "e4", Key: "k", EventTime: 1110, Value: 8},  // 迟到但在允许范围 (1100,1130]
		{ID: "e5", Key: "k", EventTime: 1099, Value: 16}, // <= 1100：超出允许范围，拒绝
	}
	sink := NewMemSink()
	eng := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng.Run(); err != nil {
		t.Fatal(err)
	}
	st := eng.Stats()
	if st.Accepted != 4 || st.Rejected != 1 {
		t.Fatalf("接受=%d 拒绝=%d, want 4/1", st.Accepted, st.Rejected)
	}
	rej := eng.Rejections()
	if len(rej) != 1 || rej[0].Reason != RejectLateBeyondAllowed || rej[0].Event.ID != "e5" {
		t.Fatalf("拒绝记录不符: %+v", rej)
	}
	m := resultsByKey(sink.Results)
	if r := m["1000-1100/k"]; r.Sum != 3 || r.Count != 2 {
		t.Fatalf("窗口[1000,1100) 结果不符: %+v", r)
	}
	// 窗口 [1100,1200)：e3+e4 = 4+8 = 12（范围内迟到被正确计入且只计一次）
	if r := m["1100-1200/k"]; r.Sum != 12 || r.Count != 2 {
		t.Fatalf("窗口[1100,1200) 结果不符: %+v", r)
	}
	t.Logf("迟到端到端: 范围内迟到 e4 计入([1100,1200) sum=12), 范围外 e5 拒绝(%s)", rej[0].Reason)
}

// TestBeyondBoundRejection 超界乱序（违反声明上界）被明确拒绝，不静默错算。
func TestBeyondBoundRejection(t *testing.T) {
	cfg := baseConfig()
	evs := makeEvents(100, 10, []string{"a"})
	// 把最早的事件移到最后：位移 990ms >> 80ms 上界
	late := append(evs[1:], evs[0])
	_, st := runOnce(t, cfg, late)
	if st.Rejected != 1 {
		t.Fatalf("超界事件应被拒绝 1 次, got %d", st.Rejected)
	}
	t.Logf("超界乱序: 拒绝=%d 原因=%s（明确拒绝而非静默错算）", st.Rejected, RejectLateBeyondAllowed)
}

// TestNoSilentMiscount 计数守恒：接收 = 接受 + 去重 + 拒绝。
func TestNoSilentMiscount(t *testing.T) {
	evs := duplicated(boundedShuffle(makeEvents(400, 3, []string{"a", "b", "c"}), 5, 11), 5, 13)
	_, st := runOnce(t, baseConfig(), evs)
	if st.Received != st.Accepted+st.Duplicates+st.Rejected {
		t.Fatalf("计数不守恒: received=%d accepted=%d dup=%d rejected=%d",
			st.Received, st.Accepted, st.Duplicates, st.Rejected)
	}
	t.Logf("计数守恒: received=%d = accepted=%d + dup=%d + rejected=%d",
		st.Received, st.Accepted, st.Duplicates, st.Rejected)
}

// Example 展示结果幂等键格式，供文档与调试参考。
func ExampleDedupKey() {
	fmt.Println(DedupKey(WindowResult{Window: Window{Start: 0, End: 100}, Key: "a"}))
	// Output: 0-100/a
}
