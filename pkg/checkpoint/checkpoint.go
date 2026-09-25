// Package checkpoint 定义引擎状态检查点及其存储。
package checkpoint

import (
	"context"
	"sync"
	"time"
)

// WindowRef 唯一标识一个已提交窗口。
type WindowRef struct {
	Key   string
	Start time.Time
	End   time.Time
}

// Checkpoint 是某一时刻引擎状态的不可变快照。
type Checkpoint struct {
	ID int64
	// Watermark 为快照时刻的全局事件时间水位线。
	Watermark time.Time
	// MaxSeq 为已处理（去重判定前）事件的最大序号。
	MaxSeq int64
	// CommittedWindows 为已向下游提交的窗口集合，恢复后用于幂等去重。
	CommittedWindows []WindowRef
	CreatedAt        time.Time
}

// Store 为检查点存储。
type Store interface {
	Save(ctx context.Context, cp *Checkpoint) error
	LoadLatest(ctx context.Context) (*Checkpoint, error)
}

// MemoryStore 是进程内检查点存储，主要用于测试。
type MemoryStore struct {
	mu     sync.Mutex
	latest *Checkpoint
}

// NewMemoryStore 创建空的内存检查点存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Save 保存检查点快照；内部深拷贝，调用方后续修改不影响已存快照。
func (s *MemoryStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = clone(cp)
	return nil
}

// LoadLatest 返回最近一次检查点；无检查点时返回 (nil, nil)。
func (s *MemoryStore) LoadLatest(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		return nil, nil
	}
	return clone(s.latest), nil
}

// clone 深拷贝检查点，保证快照不可变。
func clone(cp *Checkpoint) *Checkpoint {
	out := *cp
	out.CommittedWindows = append([]WindowRef(nil), cp.CommittedWindows...)
	return &out
}
