package stream

import "sync/atomic"

// OverflowPolicy 决定缓冲满时的行为。
type OverflowPolicy int

const (
	// Block 阻塞注入方直到有空间（背压传导到上游）。
	Block OverflowPolicy = iota
	// DropNewest 丢弃新事件并记录（有界资源 + 可区分计数）。
	DropNewest
)

// Envelope 是缓冲中传递的单元：事件 + 读取它之后的源位点。
// 位点随事件一起流动，使消费端能做与处理进度一致的检查点。
// Done=true 是分区结束控制消息（必须送达，不走丢弃策略）。
type Envelope struct {
	Ev        Event
	Next      Offset // 读取本事件后的下一条源位点
	Partition int    // 注入分区编号
	Done      bool   // 该分区已耗尽
}

// Inbox 是源与引擎之间的有界缓冲，承载背压策略。
// 容量固定，内存占用有界；满时按策略阻塞或丢弃，
// 被阻塞/被丢弃的事件都有可区分的计数。
type Inbox struct {
	ch     chan Envelope
	policy OverflowPolicy
	stats  *Counters
}

// NewInbox 创建容量为 capacity 的有界缓冲。
func NewInbox(capacity int, policy OverflowPolicy, stats *Counters) *Inbox {
	if capacity <= 0 {
		capacity = 1
	}
	return &Inbox{ch: make(chan Envelope, capacity), policy: policy, stats: stats}
}

// Offer 注入一条事件。返回 false 表示按丢弃策略被丢弃。
// Block 策略下会阻塞直到有空间（并计入 Blocked），不会死锁：
// 引擎消费循环不依赖注入方完成。
func (b *Inbox) Offer(ev Event, next Offset, partition int) bool {
	env := Envelope{Ev: ev, Next: next, Partition: partition}
	if b.policy == DropNewest {
		select {
		case b.ch <- env:
			return true
		default:
			atomic.AddInt64(&b.stats.Dropped, 1)
			return false
		}
	}
	// Block 策略：若已满则计入一次阻塞，再阻塞写入。
	select {
	case b.ch <- env:
	default:
		atomic.AddInt64(&b.stats.Blocked, 1)
		b.ch <- env
	}
	return true
}

// Finish 投递分区结束控制消息。控制消息恒被送达，不参与丢弃。
func (b *Inbox) Finish(partition int) {
	b.ch <- Envelope{Partition: partition, Done: true}
}

// C 返回消费通道。
func (b *Inbox) C() <-chan Envelope { return b.ch }

// Close 关闭缓冲，表示注入结束。
func (b *Inbox) Close() { close(b.ch) }
