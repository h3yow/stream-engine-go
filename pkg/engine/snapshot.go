package engine

import (
	"sort"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// Snapshot 为引擎运行期状态快照，供排障与验收使用。
//
// 字段含义稳定（语义不随版本变化）：
//   - Watermark：当前全局事件时间水位线；零值表示尚未形成水位线；
//   - MaxSeq：已处理事件的最大序号；
//   - Partitions：各数据源分片的供数状态与水位线进展（含 idle/finished 判定）；
//   - PendingWindows：已打开但尚未定稿的窗口（等待水位线越过定稿线）；
//   - CommittedWindows：已定稿并提交下游的窗口（含检查点恢复出的部分）；
//   - Drops：被拒绝事件按原因的精确计数，键集合固定为全部三种原因；
//   - Stats：引擎累计计数（与 Drops 中对应项一致）。
type Snapshot struct {
	Watermark        time.Time
	MaxSeq           int64
	Partitions       []watermark.PartitionInfo
	PendingWindows   []checkpoint.WindowRef
	CommittedWindows []checkpoint.WindowRef
	Drops            map[event.DropReason]int64
	Stats            Stats
}

// Snapshot 拍摄当前状态快照；可在运行中随时调用。
//
// 一致性保证：引擎运行中时，快照经路由线程在与检查点相同的屏障静态点
// 拍摄（短暂暂停各分片），因此 Watermark、Partitions、PendingWindows、
// CommittedWindows、Drops、Stats 互为同一时刻的一致视图——不会出现
// “计数已加上、对应窗口还没出现”这类自相矛盾；在静态点上还满足
// “定稿线 ≤ 水位线的窗口都已在 CommittedWindows 中”。
// 引擎已停止（Close/Abort）时处理管线已静止，直接读取即可，同样一致。
// 反复调用读到的清单均排序、计数单调，可直接对比核对。
func (e *Engine) Snapshot() Snapshot {
	if e.started.Load() && !e.closed.Load() {
		req := snapRequest{reply: make(chan Snapshot, 1)}
		select {
		case e.snapReq <- req:
			select {
			case snap := <-req.reply:
				return snap
			case <-e.quit: // 引擎正在关闭：转为静止后直读
			}
		case <-e.quit: // 引擎正在关闭：转为静止后直读
		}
		// 等待处理管线完全停下，此后状态不再变化，直读即一致。
		e.routerWg.Wait()
		e.shardWg.Wait()
	}
	return e.buildSnapshot(e.wm.Current())
}

// buildSnapshot 以稳定水位线 wm 直接读取各状态组装快照。
// 调用前提：处于屏障静态点（分片全部暂停）或处理管线已停止，
// 即读数期间没有任何分片在修改这些状态；wm 为静态点读取的水位线，
// 与窗口清单、计数同属同一时刻。
func (e *Engine) buildSnapshot(wm time.Time) Snapshot {
	e.openMu.Lock()
	pending := make([]checkpoint.WindowRef, 0, len(e.openRefs))
	for ref := range e.openRefs {
		pending = append(pending, ref)
	}
	e.openMu.Unlock()

	e.committedMu.Lock()
	committed := make([]checkpoint.WindowRef, 0, len(e.committed))
	for ref := range e.committed {
		committed = append(committed, ref)
	}
	e.committedMu.Unlock()

	less := func(refs []checkpoint.WindowRef) {
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].Key != refs[j].Key {
				return refs[i].Key < refs[j].Key
			}
			return refs[i].Start.Before(refs[j].Start)
		})
	}
	less(pending)
	less(committed)

	return Snapshot{
		Watermark:        wm,
		MaxSeq:           e.maxSeq.Load(),
		Partitions:       e.wm.Partitions(),
		PendingWindows:   pending,
		CommittedWindows: committed,
		Drops: map[event.DropReason]int64{
			event.ReasonLateBeyondAllowed: e.lateDropped.Load(),
			event.ReasonBackpressureFull:  e.backpressureDropped.Load(),
			event.ReasonPartitionFinished: e.partitionFinishedDropped.Load(),
		},
		Stats: e.Stats(),
	}
}
