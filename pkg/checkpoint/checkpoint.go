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

// CurrentVersion 是当前写出的检查点格式版本。
// 版本 1：无 Parallelism/Partitions 字段（初版格式）。
// 版本 2：记录并行度与分片水位线状态，支持扩缩容恢复校验。
const CurrentVersion = 2

// PartitionState 记录检查点时刻单个数据源分片的水位线跟踪状态。
type PartitionState struct {
	ID          int
	Status      string // active / idle / finished（语义见 watermark 包）
	MaxObserved time.Time
	HasData     bool
}

// Checkpoint 是某一时刻引擎状态的不可变快照。
type Checkpoint struct {
	ID int64
	// Version 为检查点格式版本，恢复时据此判定兼容性。
	Version int
	// Parallelism 为拍摄检查点时引擎的处理分片数。
	// 恢复时允许以不同并行度继续（状态均为并行度无关的全局状态），
	// 但 Parallelism<=0 的旧格式检查点无法验证这一点，会被明确拒绝。
	Parallelism int
	// Watermark 为快照时刻的全局事件时间水位线。
	Watermark time.Time
	// MaxSeq 为已处理（去重判定前）事件的最大序号。
	MaxSeq int64
	// CommittedWindows 为已向下游提交的窗口集合，恢复后用于幂等去重。
	CommittedWindows []WindowRef
	// Partitions 为各数据源分片的水位线跟踪状态（含 idle/finished 标记）。
	Partitions []PartitionState
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
	out.Partitions = append([]PartitionState(nil), cp.Partitions...)
	return &out
}
