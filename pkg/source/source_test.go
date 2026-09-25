package source

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

// TestEmitReplaysInOrder 验证按给定顺序完整重放。
func TestEmitReplaysInOrder(t *testing.T) {
	evs := []event.Event{
		{Key: "k", Timestamp: time.Unix(1, 0), Value: 1, Seq: 1},
		{Key: "k", Timestamp: time.Unix(2, 0), Value: 2, Seq: 2},
	}
	out := make(chan event.Event, len(evs))
	if err := New(evs).Emit(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	for i, want := range evs {
		got := <-out
		if got.Seq != want.Seq {
			t.Fatalf("第%d条期望 seq=%d，实际 %d", i, want.Seq, got.Seq)
		}
	}
}

// TestEmitRespectsCancel 验证下游阻塞时 ctx 取消能中断重放。
func TestEmitRespectsCancel(t *testing.T) {
	evs := make([]event.Event, 100)
	out := make(chan event.Event) // 无缓冲：必然阻塞
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(evs).Emit(ctx, out) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("取消后 Emit 应返回错误")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Emit 在取消后未返回")
	}
}
