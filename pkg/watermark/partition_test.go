package watermark

import (
	"testing"
	"time"
)

// TestIdlePartitionExcluded 验证空闲分片被排除后，水位线跟随活跃分片推进。
func TestIdlePartitionExcluded(t *testing.T) {
	g := New(0) // 无乱序余量，便于观察精确值
	// 两个分片都供数：水位线 = min(两者最大事件时间)。
	if wm := g.ObservePartition(1, secTime(5)).Unix(); wm != 5 {
		t.Fatalf("分片1观测5s后期望水位线5s，实际=%ds", wm)
	}
	if wm := g.ObservePartition(0, secTime(10)).Unix(); wm != 5 {
		t.Fatalf("分片1只到5s，水位线应被拖在5s，实际=%ds", wm)
	}
	t.Logf("[水位线判定] 分片0@10s 分片1@5s → 全局水位线=5s（活跃分片取最小）")

	// 分片1 标记空闲：水位线应立刻能跟分片0 推进。
	g.MarkIdle(1)
	if wm := g.ObservePartition(0, secTime(20)).Unix(); wm != 20 {
		t.Fatalf("分片1空闲后水位线应跟随分片0到20s，实际=%ds", wm)
	}
	t.Logf("[水位线判定] 分片1标记idle → 分片0观测20s → 全局水位线=20s")

	// 空闲分片回来：自动恢复活跃，其旧时间戳把水位线贡献拉低，
	// 但全局水位线单调不回退（floor 保持在20s）。
	if wm := g.ObservePartition(1, secTime(8)).Unix(); wm != 20 {
		t.Fatalf("空闲分片恢复后水位线不应回退，期望20s，实际=%ds", wm)
	}
	st := g.Partitions()
	if len(st) != 2 || st[1].Status != StatusActive {
		t.Fatalf("空闲分片来数据应自动恢复active，实际=%+v", st)
	}
	t.Logf("[水位线判定] 分片1恢复供数@8s → 自动active；全局水位线保持20s（单调不回退）")

	// 分片1 追上后，水位线重新受较慢分片约束。
	if wm := g.ObservePartition(1, secTime(25)).Unix(); wm != 20 {
		t.Fatalf("分片0仍在20s，水位线应=20s，实际=%ds", wm)
	}
	if wm := g.ObservePartition(0, secTime(30)).Unix(); wm != 25 {
		t.Fatalf("分片1到25s后水位线应=25s，实际=%ds", wm)
	}
}

// TestFinishedPartitionPermanent 验证终结分片被永久排除且不会因新事件复活。
func TestFinishedPartitionPermanent(t *testing.T) {
	g := New(0)
	g.ObservePartition(0, secTime(10))
	g.ObservePartition(1, secTime(6))
	g.MarkFinished(1)
	if wm := g.ObservePartition(0, secTime(15)).Unix(); wm != 15 {
		t.Fatalf("分片1终结后水位线应跟随分片0到15s，实际=%ds", wm)
	}
	// 终结分片的迟到事件被生成器忽略：水位线不受其影响，状态保持 finished。
	if wm := g.ObservePartition(1, secTime(100)).Unix(); wm != 15 {
		t.Fatalf("已终结分片的观测应被忽略，水位线应保持15s，实际=%ds", wm)
	}
	if !g.IsFinished(1) {
		t.Fatal("终结分片不应因新事件复活")
	}
	// 终结不可逆：MarkIdle 对 finished 无效。
	g.MarkIdle(1)
	if !g.IsFinished(1) {
		t.Fatal("finished 应优先于 idle 标记，不可回退")
	}
	t.Logf("[水位线判定] 分片1 finished → 其观测被忽略，状态不可回退，水位线=15s")
}

// TestAllPartitionsIdle 验证所有分片空闲时水位线停在历史最高值。
func TestAllPartitionsIdle(t *testing.T) {
	g := New(0)
	g.ObservePartition(0, secTime(10))
	g.ObservePartition(1, secTime(12))
	g.MarkIdle(0)
	g.MarkIdle(1)
	if wm := g.Current().Unix(); wm != 10 {
		t.Fatalf("全部空闲时水位线应停在历史最高10s，实际=%ds", wm)
	}
	t.Logf("[水位线判定] 所有分片idle → 水位线保持10s（floor），不再推进也不回退")
}

// TestIdleTimeout 验证超过空闲超时的分片被自动标记为 idle。
func TestIdleTimeout(t *testing.T) {
	g := NewWithIdleTimeout(0, 50*time.Millisecond)
	now := time.Now()
	g.now = func() time.Time { return now } // 注入可控时钟
	g.ObservePartition(0, secTime(10))
	g.ObservePartition(1, secTime(6))

	now = now.Add(100 * time.Millisecond) // 两个分片都沉默超时时长
	// 分片0 来新事件触发清扫：分片1 沉默超时 → 自动idle，水位线跟分片0走。
	if wm := g.ObservePartition(0, secTime(20)).Unix(); wm != 20 {
		t.Fatalf("分片1超时idle后水位线应到20s，实际=%ds", wm)
	}
	st := g.Partitions()
	if st[1].Status != StatusIdle {
		t.Fatalf("分片1应被自动标记idle，实际=%s", st[1].Status)
	}
	t.Logf("[水位线判定] 分片1沉默超50ms → 自动idle；分片0@20s → 水位线=20s")
}

// TestRestorePartitions 验证从检查点恢复各分片状态（含 idle/finished）。
func TestRestorePartitions(t *testing.T) {
	g := New(0)
	g.RestorePartitions(secTime(8), []PartitionState{
		{ID: 0, Status: StatusActive, MaxObserved: secTime(10), HasData: true},
		{ID: 1, Status: StatusFinished, MaxObserved: secTime(6), HasData: true},
	})
	if wm := g.Current().Unix(); wm != 10 {
		t.Fatalf("恢复后水位线应=活跃分片0的10s，实际=%ds", wm)
	}
	if !g.IsFinished(1) {
		t.Fatal("finished 状态应随检查点恢复")
	}
	// 恢复 floor=8s：即使分片状态重建异常，水位线也不会低于8s。
	g2 := New(0)
	g2.RestorePartitions(secTime(8), nil)
	if wm := g2.Current().Unix(); wm != 8 {
		t.Fatalf("无分片明细恢复时水位线应保持floor=8s，实际=%ds", wm)
	}
	t.Logf("[水位线判定] 恢复：分片0 active@10s、分片1 finished → 水位线=10s；空明细恢复 → floor=8s")
}
