package stream

// Source 是本地模拟数据源，支持按位点读取与回退。
// 位点即事件在源中的下标，恢复时从检查点记录的 NextOffset 继续，
// 保证位点回退可界定：重放范围 = [检查点位点, 故障点)。
type Source interface {
	// Read 从位点 off 读取一条事件；ok=false 表示源已耗尽。
	Read(off Offset) (ev Event, next Offset, ok bool)
	// Len 返回源中事件总数。
	Len() int
}

// MemSource 是内存中的确定性数据源。
type MemSource struct {
	Events []Event
}

// Read 实现 Source。
func (s *MemSource) Read(off Offset) (Event, Offset, bool) {
	if off < 0 || int(off) >= len(s.Events) {
		return Event{}, off, false
	}
	return s.Events[off], off + 1, true
}

// Len 实现 Source。
func (s *MemSource) Len() int { return len(s.Events) }
