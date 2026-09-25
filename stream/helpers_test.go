package stream

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

// testLogger 把引擎日志接入 testing 日志，单测日志中可看到
// 水位线推进、窗口触发与迟到/去重判定依据。
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(string(p[:len(p)-1])) // 去掉换行
	return len(p), nil
}

// makeEvents 生成确定性事件集：n 条事件，事件时间落在 [0, n*step)，
// Key 在 keys 中轮转，Value 固定为 1（Count 即和）。
func makeEvents(n int, step int64, keys []string) []Event {
	out := make([]Event, n)
	for i := 0; i < n; i++ {
		out[i] = Event{
			ID:        fmt.Sprintf("e%04d", i),
			Key:       keys[i%len(keys)],
			EventTime: int64(i) * step,
			Value:     1,
		}
	}
	return out
}

// boundedShuffle 生成“位移有界”的随机排列：每个元素移动不超过 maxDisp 个位置。
// 这模拟水位线语义所声明的乱序上界：只要实际乱序幅度（换算成事件时间）
// 小于 OutOfOrderness+AllowedLateness，引擎保证结果与到达顺序无关。
func boundedShuffle(evs []Event, maxDisp int, seed int64) []Event {
	out := make([]Event, len(evs))
	copy(out, evs)
	r := rand.New(rand.NewSource(seed))
	// 分块打乱：每块 maxDisp 个元素，块内随机排列，
	// 保证任意元素的前向/后向位移都不超过 maxDisp-1 个位置。
	for start := 0; start < len(out); start += maxDisp {
		end := start + maxDisp
		if end > len(out) {
			end = len(out)
		}
		blk := out[start:end]
		r.Shuffle(len(blk), func(i, j int) { blk[i], blk[j] = blk[j], blk[i] })
	}
	return out
}

// shuffled 返回事件的完全随机排列（用于构造超界乱序，验证拒绝路径）。
func shuffled(evs []Event, seed int64) []Event {
	out := make([]Event, len(evs))
	copy(out, evs)
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// duplicated 返回每条事件投递两次的事件流（模拟重复投递）：
// 两份拷贝相邻成对，对整体做位移有界打乱，
// 保证重复拷贝在窗口关闭前到达，走的是幂等去重路径而非迟到拒绝路径。
func duplicated(evs []Event, maxDisp int, seed int64) []Event {
	pairs := make([][2]Event, len(evs))
	for i, e := range evs {
		pairs[i] = [2]Event{e, e}
	}
	r := rand.New(rand.NewSource(seed))
	for start := 0; start < len(pairs); start += maxDisp {
		end := start + maxDisp
		if end > len(pairs) {
			end = len(pairs)
		}
		blk := pairs[start:end]
		r.Shuffle(len(blk), func(i, j int) { blk[i], blk[j] = blk[j], blk[i] })
	}
	out := make([]Event, 0, len(evs)*2)
	for _, p := range pairs {
		out = append(out, p[0], p[1])
	}
	return out
}

// resultKey 生成结果比较键。
func resultKey(r WindowResult) string {
	return fmt.Sprintf("%d-%d/%s", r.Window.Start, r.Window.End, r.Key)
}

// resultsByKey 把结果切片转成 map 便于比较。
func resultsByKey(rs []WindowResult) map[string]WindowResult {
	m := make(map[string]WindowResult, len(rs))
	for _, r := range rs {
		m[resultKey(r)] = r
	}
	return m
}

// assertSameResults 断言两组结果完全一致（键集合与数值）。
func assertSameResults(t *testing.T, want, got []WindowResult) {
	t.Helper()
	wm, gm := resultsByKey(want), resultsByKey(got)
	if len(wm) != len(gm) {
		t.Fatalf("结果条数不同: want %d got %d\nwant=%v\ngot=%v", len(wm), len(gm), want, got)
	}
	for k, w := range wm {
		g, ok := gm[k]
		if !ok {
			t.Fatalf("缺少结果 %s\nwant=%v\ngot=%v", k, want, got)
		}
		if w.Sum != g.Sum || w.Count != g.Count {
			t.Fatalf("结果 %s 数值不同: want %+v got %+v", k, w, g)
		}
	}
}
