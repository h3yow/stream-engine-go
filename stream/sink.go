package stream

import "fmt"

// Sink 是下游写出接口。Write 必须幂等：
// 同一 (窗口, Key) 的结果重复写出时不得产生重复记录。
type Sink interface {
	Write(res WindowResult) error
}

// MemSink 是本地模拟下游，按 (窗口, Key) 幂等去重，
// 并可通过 Delay 模拟下游变慢以触发背压。
type MemSink struct {
	Results []WindowResult
	seen    map[string]struct{}
	Delay   func() // 可选：每次写出前调用以模拟下游变慢
}

// NewMemSink 创建内存下游。
func NewMemSink() *MemSink { return &MemSink{seen: make(map[string]struct{})} }

// Write 实现 Sink（幂等）：重复结果直接忽略，不追加记录。
func (s *MemSink) Write(res WindowResult) error {
	if s.Delay != nil {
		s.Delay()
	}
	key := DedupKey(res)
	if _, ok := s.seen[key]; ok {
		return nil // 幂等：恢复重放导致的重复写出被去重
	}
	s.seen[key] = struct{}{}
	s.Results = append(s.Results, res)
	return nil
}

// DedupKey 返回结果的幂等键。
func DedupKey(res WindowResult) string {
	return fmt.Sprintf("%d-%d/%s", res.Window.Start, res.Window.End, res.Key)
}
