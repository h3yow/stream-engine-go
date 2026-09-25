// Package source 提供本地模拟事件数据源。
package source

import (
	"context"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

// Source 按给定顺序向引擎重放一组事件，用于确定性测试。
type Source struct {
	events []event.Event
}

// New 使用给定事件序列创建数据源。
func New(events []event.Event) *Source { return &Source{events: events} }

// Emit 将事件依次写入 out；ctx 取消或 out 关闭时返回。
//
// 写入采用阻塞式发送：若下游（引擎入口缓冲）满，Emit 会阻塞，
// 由调用方（引擎 Enqueue 的非阻塞策略）决定背压行为。
func (s *Source) Emit(ctx context.Context, out chan<- event.Event) error {
	for _, ev := range s.events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- ev:
		}
	}
	return nil
}
