package sink

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/window"
)

func res(key string, startSec, sum int64) window.Result {
	return window.Result{
		Key:    key,
		Window: window.Window{Start: time.Unix(startSec, 0).UTC(), End: time.Unix(startSec+10, 0).UTC()},
		Sum:    sum,
		Count:  1,
	}
}

// TestIdempotentWrite 验证同一窗口重复提交只真正写出一次。
func TestIdempotentWrite(t *testing.T) {
	s := NewCollectingSink(0)
	ctx := context.Background()
	r := res("a", 0, 100)
	for i := 0; i < 3; i++ {
		committed, err := s.Write(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("[下游幂等] 第%d次提交窗口 a|0s → committed=%v", i+1, committed)
	}
	if s.Written() != 1 {
		t.Fatalf("真正写出次数期望 1，实际 %d", s.Written())
	}
	if s.DuplicateSkips() != 2 {
		t.Fatalf("重复跳过期望 2，实际 %d", s.DuplicateSkips())
	}
	if len(s.Results()) != 1 || s.Results()[0].Sum != 100 {
		t.Fatalf("下游结果期望仅 1 条 sum=100，实际 %+v", s.Results())
	}
}

// TestWriteDelayRespectsContext 验证慢下游在 ctx 取消时不永久阻塞。
func TestWriteDelayRespectsContext(t *testing.T) {
	s := NewCollectingSink(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := s.Write(ctx, res("b", 10, 1)); err == nil {
		t.Fatal("慢下游在 ctx 取消后应返回错误，实际 nil")
	}
}
