package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Checkpoint 是引擎状态的一致性快照，用于故障恢复。
type Checkpoint struct {
	ID         int64   // 单调递增的检查点编号
	NextOffset Offset  // 下一条待读取的源位点（位点回退的边界）
	Watermark  int64   // 快照时刻的水位线
	MaxSeen    []int64 // 快照时刻各注入分区已见到的最大事件时间
	// Windows 保存尚未触发的窗口状态；已触发窗口的结果由 Sink 幂等保证不重复输出。
	Windows map[Window]map[string]WindowStateSnapshot
}

// WindowStateSnapshot 是单个 (窗口, Key) 聚合状态的可序列化形式。
type WindowStateSnapshot struct {
	Sum   int64
	Count int64
	Seen  []string // 已计入的事件 ID
}

// CheckpointStore 持久化与加载检查点。
type CheckpointStore interface {
	Save(cp Checkpoint) error
	LoadLatest() (Checkpoint, bool, error)
}

// FileCheckpointStore 以本地文件保存检查点（JSON，写临时文件后原子改名）。
type FileCheckpointStore struct {
	Dir string
}

// NewFileCheckpointStore 创建文件检查点存储。
func NewFileCheckpointStore(dir string) *FileCheckpointStore { return &FileCheckpointStore{Dir: dir} }

// checkpointFile 是磁盘上的可序列化形式（Window 不能直接做 JSON map 键）。
type checkpointFile struct {
	ID         int64                   `json:"id"`
	NextOffset Offset                  `json:"next_offset"`
	Watermark  int64                   `json:"watermark"`
	MaxSeen    []int64                 `json:"max_seen"`
	Windows    []checkpointWindowEntry `json:"windows"`
}

type checkpointWindowEntry struct {
	Start int64                          `json:"start"`
	End   int64                          `json:"end"`
	Keys  map[string]WindowStateSnapshot `json:"keys"`
}

// Save 实现 CheckpointStore。
func (s *FileCheckpointStore) Save(cp Checkpoint) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	f := checkpointFile{
		ID:         cp.ID,
		NextOffset: cp.NextOffset,
		Watermark:  cp.Watermark,
		MaxSeen:    cp.MaxSeen,
	}
	for w, keys := range cp.Windows {
		f.Windows = append(f.Windows, checkpointWindowEntry{Start: w.Start, End: w.End, Keys: keys})
	}
	sort.Slice(f.Windows, func(i, j int) bool { return f.Windows[i].Start < f.Windows[j].Start })
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.Dir, fmt.Sprintf("chk-%06d.tmp", cp.ID))
	final := filepath.Join(s.Dir, fmt.Sprintf("chk-%06d.json", cp.ID))
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, final) // 原子提交，避免半截检查点
}

// LoadLatest 实现 CheckpointStore：加载编号最大的检查点。
func (s *FileCheckpointStore) LoadLatest() (Checkpoint, bool, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, err
	}
	bestID := int64(-1)
	var bestPath string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "chk-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "chk-"), ".json"), 10, 64)
		if err != nil {
			continue
		}
		if id > bestID {
			bestID, bestPath = id, filepath.Join(s.Dir, name)
		}
	}
	if bestID < 0 {
		return Checkpoint{}, false, nil
	}
	data, err := os.ReadFile(bestPath)
	if err != nil {
		return Checkpoint{}, false, err
	}
	var f checkpointFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Checkpoint{}, false, fmt.Errorf("损坏的检查点 %s: %w", bestPath, err)
	}
	cp := Checkpoint{
		ID:         f.ID,
		NextOffset: f.NextOffset,
		Watermark:  f.Watermark,
		MaxSeen:    f.MaxSeen,
		Windows:    make(map[Window]map[string]WindowStateSnapshot, len(f.Windows)),
	}
	for _, we := range f.Windows {
		cp.Windows[Window{Start: we.Start, End: we.End}] = we.Keys
	}
	return cp, true, nil
}
