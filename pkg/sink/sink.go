// Package sink 提供本地模拟的下游写出，具备幂等提交与可观测的阻塞行为。
package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/window"
)

// Sink 为下游写出接口。Write 返回 committed=true 表示本次真正写出；
// 若同一窗口结果已提交过则返回 (false, nil)，保证重复投递与一次投递等价。
type Sink interface {
	Write(ctx context.Context, r window.Result) (committed bool, err error)
}

// CollectingSink 是内存收集型下游，可通过 writeDelay 模拟下游变慢。
type CollectingSink struct {
	mu             sync.Mutex
	results        []window.Result
	committed      map[string]bool
	writeDelay     time.Duration
	written        int64
	duplicateSkips int64
}

// NewCollectingSink 创建收集型下游；writeDelay>0 时每次写出阻塞该时长以模拟慢下游。
func NewCollectingSink(writeDelay time.Duration) *CollectingSink {
	return &CollectingSink{committed: make(map[string]bool), writeDelay: writeDelay}
}

// Write 幂等写出窗口结果。
//
// 幂等键为 (key, windowStart, windowEnd)：同一窗口重复提交返回 (false, nil)，
// 因此恢复重放或重复触发不会产生重复下游记录。
func (s *CollectingSink) Write(ctx context.Context, r window.Result) (bool, error) {
	id := fmt.Sprintf("%s|%d|%d", r.Key, r.Window.Start.UnixNano(), r.Window.End.UnixNano())

	s.mu.Lock()
	if s.committed[id] {
		s.duplicateSkips++
		s.mu.Unlock()
		return false, nil
	}
	delay := s.writeDelay
	s.mu.Unlock()

	// 模拟慢下游写出；ctx 取消时不标记已提交，避免未写出却被视为幂等命中。
	if delay > 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(delay):
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed[id] { // 延迟期间可能有并发写入先完成
		s.duplicateSkips++
		return false, nil
	}
	s.committed[id] = true
	s.results = append(s.results, r)
	s.written++
	return true, nil
}

// Results 返回已提交结果的快照副本。
func (s *CollectingSink) Results() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]window.Result(nil), s.results...)
}

// Written 返回真正写出次数。
func (s *CollectingSink) Written() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written
}

// DuplicateSkips 返回因重复提交被跳过的次数。
func (s *CollectingSink) DuplicateSkips() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duplicateSkips
}
