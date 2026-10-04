// demo 演示本地事件流引擎：乱序事件源 → 窗口聚合 → 幂等下游。
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
	// 乱序事件源：事件时间不按到达顺序递增。
	events := []event.Event{
		{Key: "user-1", Timestamp: ts(1), Value: 10, Seq: 1},
		{Key: "user-1", Timestamp: ts(9), Value: 20, Seq: 2},
		{Key: "user-2", Timestamp: ts(4), Value: 5, Seq: 3},
		{Key: "user-1", Timestamp: ts(10), Value: 1, Seq: 4}, // 推进水位线到 10s
		{Key: "user-1", Timestamp: ts(7), Value: 3, Seq: 5},  // 允许迟到范围内
		{Key: "user-2", Timestamp: ts(20), Value: 1, Seq: 6}, // 水位线到 20s：定稿 [0,10)
		{Key: "user-1", Timestamp: ts(3), Value: 99, Seq: 7}, // 超出允许迟到 → 拒绝
		{Key: "user-2", Timestamp: ts(30), Value: 1, Seq: 8}, // 水位线到 30s：定稿 [10,20)
	}

	cfg := engine.Config{
		WindowSize:        10 * time.Second,
		MaxOutOfOrderness: 0,
		AllowedLateness:   5 * time.Second,
		InputBuffer:       64,
		Parallelism:       2,
		CheckpointEveryN:  4,
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
	for ev := range in {
		ok := eng.Enqueue(ev)
		fmt.Printf("[注入] seq=%d key=%s eventTime=%ds value=%d → enqueued=%v\n",
			ev.Seq, ev.Key, ev.Timestamp.Unix(), ev.Value, ok)
		// 小幅间隔让处理跟上注入顺序，演示输出与注释的逐条判定一一对应。
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // 等待处理排空
	fmt.Printf("[排空后] 全局水位线=%ds（定稿条件：wm >= windowEnd + allowedLateness=5s）\n",
		eng.Watermark().Unix())

	// 运行期状态快照：水位线、分片状态、待定/已定稿窗口、按原因的拒绝计数。
	snap := eng.Snapshot()
	fmt.Println("\n== 状态快照 ==")
	fmt.Printf("水位线=%ds maxSeq=%d 待定窗口=%d 已定稿窗口=%d 拒绝计数=%v\n",
		snap.Watermark.Unix(), snap.MaxSeq, len(snap.PendingWindows),
		len(snap.CommittedWindows), snap.Drops)
	for _, p := range snap.Partitions {
		fmt.Printf("  分片=%q 状态=%s 分片水位线=%ds\n", p.ID, p.Status, p.Watermark.Unix())
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
	fmt.Printf("\n== 引擎统计 ==\n接收=%d 计入=%d 重复跳过=%d 迟到拒绝=%d 背压拒绝=%d 窗口输出=%d 检查点=%d\n",
		st.Received, st.Accepted, st.DuplicateSkipped, st.LateDropped,
		st.BackpressureDropped, st.WindowsEmitted, st.Checkpoints)
	for _, d := range eng.Drops() {
		fmt.Printf("[拒绝] seq=%d 原因=%s\n", d.Event.Seq, d.Reason)
	}
	if cp, err := store.LoadLatest(context.Background()); err == nil && cp != nil {
		fmt.Printf("\n== 最终检查点 ==\nid=%d watermark=%ds maxSeq=%d 已提交窗口=%d\n",
			cp.ID, cp.Watermark.Unix(), cp.MaxSeq, len(cp.CommittedWindows))
	}
}
