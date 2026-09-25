package checkpoint

import (
	"context"
	"testing"
	"time"
)

// TestSaveLoadLatest 验证存取最新检查点且返回深拷贝。
func TestSaveLoadLatest(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	if cp, err := st.LoadLatest(ctx); err != nil || cp != nil {
		t.Fatalf("空存储应返回 (nil,nil)，实际 cp=%v err=%v", cp, err)
	}
	cp := &Checkpoint{
		ID: 1, Watermark: time.Unix(5, 0).UTC(), MaxSeq: 10,
		CommittedWindows: []WindowRef{{Key: "k", Start: time.Unix(0, 0).UTC(), End: time.Unix(10, 0).UTC()}},
	}
	if err := st.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	// 修改原对象，确认存储内快照不受影响。
	cp.CommittedWindows[0].Key = "mutated"
	cp.MaxSeq = 999
	got, err := st.LoadLatest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxSeq != 10 || got.CommittedWindows[0].Key != "k" {
		t.Fatalf("快照应不可变，实际 MaxSeq=%d ref=%+v", got.MaxSeq, got.CommittedWindows[0])
	}
	got.CommittedWindows[0].Key = "mutated2"
	got2, _ := st.LoadLatest(ctx)
	if got2.CommittedWindows[0].Key != "k" {
		t.Fatal("LoadLatest 返回的应为深拷贝")
	}
}
