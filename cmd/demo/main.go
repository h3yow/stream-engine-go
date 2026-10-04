// demo 演示本地事件流引擎：多分片事件源 → 空闲分片水位线推进 → 窗口聚合 → 幂等下游。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/engine"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
	"github.com/h3yow/stream-engine-go/pkg/source"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func main() {
	// 两个数据源分片：分片1 只有一条早期事件，之后长期沉默。
	events := []event.Event{
		{Key: "user-1", Timestamp: ts(1), Value: 10, Seq: 1, Partition: 0},
		{Key: "user-2", Timestamp: ts(2), Value: 5, Seq: 2, Partition: 1},
		{Key: "user-1", Timestamp: ts(9), Value: 20, Seq: 3, Partition: 0},
		{Key: "user-1", Timestamp: ts(10), Value: 1, Seq: 4, Partition: 0},
		{Key: "user-1", Timestamp: ts(7), Value: 3, Seq: 5, Partition: 0}, // 允许迟到范围内
		{Key: "user-1", Timestamp: ts(20), Value: 1, Seq: 6, Partition: 0},
		{Key: "user-1", Timestamp: ts(3), Value: 99, Seq: 7, Partition: 0}, // 超出允许迟到 → 拒绝
		{Key: "user-1", Timestamp: ts(30), Value: 1, Seq: 8, Partition: 0},
	}

	cfg := engine.Config{
		WindowSize:        10 * time.Second,
		MaxOutOfOrderness: 0,
		AllowedLateness:   5 * time.Second,
		InputBuffer:       64,
		Parallelism:       2,
		CheckpointEveryN:  4,
		SourcePartitions:  2, // 声明分片数：分片1 沉默期间水位线不盲目推进
	}
	store := checkpoint.NewMemoryStore()
	snk := sink.NewCollectingSink(0)
	eng := engine.New(cfg, snk, store)
	if err := eng.Start(context.Background()); err != nil {
		panic(err)
	}

	src := source.New(events)
	in := make(chan event.Event)
	go func() {
		_ = src.Emit(context.Background(), in)
		close(in)
	}()
	i := 0
	for ev := range in {
		ok := eng.Enqueue(ev)
		fmt.Printf("[注入] seq=%d key=%s partition=%d eventTime=%ds value=%d → enqueued=%v\n",
			ev.Seq, ev.Key, ev.Partition, ev.Timestamp.Unix(), ev.Value, ok)
		i++
		if i == 4 {
			// 分片1 的事件（seq=2）已处理完：确认其暂时无数据，标记空闲，
			// 之后水位线跟随分片0 推进，不再被分片1 拖住。
			time.Sleep(100 * time.Millisecond)
			eng.MarkPartitionIdle(1)
			fmt.Println("[标记] 分片1 暂时无数据 → 标记为空闲（idle）")
		}
	}
	time.Sleep(100 * time.Millisecond) // 等待处理排空

	snap, err := eng.Snapshot(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n== 状态快照 ==\n水位线=%ds 并行度=%d 待定稿窗口=%d 已提交窗口=%d 拒绝分类=%v\n",
		snap.Watermark.Unix(), snap.Parallelism, len(snap.PendingWindows),
		snap.CommittedWindows, snap.Drops)
	for _, p := range snap.Partitions {
		fmt.Printf("  分片%d 状态=%s maxObserved=%ds\n", p.ID, p.Status, p.MaxObserved.Unix())
	}
	for _, w := range snap.PendingWindows {
		fmt.Printf("  待定稿 key=%s window=[%d,%d)s sum=%d count=%d 定稿线=%ds\n",
			w.Key, w.Start.Unix(), w.End.Unix(), w.Sum, w.Count, w.Deadline.Unix())
	}

	if err := eng.Close(); err != nil {
		panic(err)
	}

	fmt.Println("\n== 下游窗口结果 ==")
	for _, r := range snk.Results() {
		fmt.Printf("key=%s window=[%d,%d)s sum=%d count=%d seqs=%v\n",
			r.Key, r.Window.Start.Unix(), r.Window.End.Unix(), r.Sum, r.Count, r.Seqs)
	}
	st := eng.Stats()
	fmt.Printf("\n== 引擎统计 ==\n接收=%d 计入=%d 重复跳过=%d 迟到拒绝=%d 背压拒绝=%d 终结分片拒绝=%d 窗口输出=%d 检查点=%d\n",
		st.Received, st.Accepted, st.DuplicateSkipped, st.LateDropped,
		st.BackpressureDropped, st.PartitionFinishedDropped, st.WindowsEmitted, st.Checkpoints)
	for _, d := range eng.Drops() {
		fmt.Printf("[拒绝] seq=%d 原因=%s\n", d.Event.Seq, d.Reason)
	}
	if cp, err := store.LoadLatest(context.Background()); err == nil && cp != nil {
		fmt.Printf("\n== 最终检查点 ==\nid=%d version=%d parallelism=%d watermark=%ds maxSeq=%d 已提交窗口=%d 分片状态=%d 个\n",
			cp.ID, cp.Version, cp.Parallelism, cp.Watermark.Unix(), cp.MaxSeq,
			len(cp.CommittedWindows), len(cp.Partitions))
	}
}
