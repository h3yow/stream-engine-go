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

// PartitionState 为单个数据源分片的可持久化供数状态。
type PartitionState struct {
	// MaxObserved 为该分片已观测的最大事件时间；零值表示从未观测到事件。
	MaxObserved time.Time
	// Finished 表示该分片已被显式宣告彻底不再供数（不可逆）。
	// 该标记必须随检查点持久化：否则恢复后已终止分片会“复活”为正常供数，
	// 其后续事件被重新算进窗口，导致恢复前后结果不一致。
	Finished bool
	// FinishSeq 为终止宣告时刻该分片已观测的最大事件序号：
	// 恢复重放时 Seq <= FinishSeq 的事件是终止前已计入的，需重新接受以重建
	// 未定稿窗口；Seq > FinishSeq 的事件是终止后到达的，以 partition_finished 拒绝。
	FinishSeq int64
	// HasFinishSeq=false 表示终止前未观测到任何事件
	// （一条数据都没出过就被停掉的分片：恢复后其任何事件都拒绝）。
	HasFinishSeq bool
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
	// Partitions 为各数据源分片的供数状态（含已观测最大事件时间与终止标记），
	// 用于恢复分片级水位线跟踪；已终止分片（含从未供数的）也在其中，
	// 保证恢复后 idle/finished 两种状态仍然可分、表现不变（空 map 表示无分片信息）。
	Partitions map[string]PartitionState
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
		out.Partitions = make(map[string]PartitionState, len(cp.Partitions))
		for k, v := range cp.Partitions {
			out.Partitions[k] = v
		}
	}
	return &out
}
