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
	// Parallelism 为拍摄时的处理分片数。
	// 恢复时允许与引擎当前并行度不同（扩缩容）：已提交窗口集合与
	// 水位线都是并行度无关的状态，换并行度恢复语义不变。
	Parallelism int
	// 以下三个字段为窗口语义参数，决定窗口边界与定稿判定，
	// 恢复时必须与引擎配置完全一致，否则检查点不兼容、必须拒绝恢复。
	WindowSize        time.Duration
	MaxOutOfOrderness time.Duration
	AllowedLateness   time.Duration
	// Partitions 为各数据源分片已观测的最大事件时间，
	// 用于恢复分片级水位线跟踪（空 map 表示无分片信息）。
	Partitions map[string]time.Time
	CreatedAt  time.Time
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
	if cp.Partitions != nil {
		out.Partitions = make(map[string]time.Time, len(cp.Partitions))
		for k, v := range cp.Partitions {
			out.Partitions[k] = v
		}
	}
	return &out
}
