package stream

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// recoveryConfig 返回带检查点的测试配置。
func recoveryConfig(dir string) Config {
	cfg := baseConfig()
	cfg.CheckpointEvery = 50
	cfg.CheckpointStore = NewFileCheckpointStore(dir)
	return cfg
}

// TestCrashRecovery 崩溃后从检查点恢复：结果与不崩溃的基准一致，
// 已提交窗口不重复输出、不丢失。
func TestCrashRecovery(t *testing.T) {
	evs := boundedShuffle(makeEvents(400, 5, []string{"a", "b"}), 7, 17)
	base, _ := runOnce(t, baseConfig(), evs) // 无检查点基准

	dir := t.TempDir()
	sink := NewMemSink() // 崩溃前后共用同一下游，验证幂等

	// 第一次运行：处理 273 条后模拟崩溃。
	cfg := recoveryConfig(dir)
	cfg.FailAfter = 273
	cfg.Logger = testLogger(t)
	eng1 := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng1.Run(); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}
	emittedBeforeCrash := len(sink.Results)
	t.Logf("崩溃前已输出 %d 条窗口结果", emittedBeforeCrash)

	// 第二次运行：从最新检查点恢复。
	cfg2 := recoveryConfig(dir)
	cfg2.Logger = testLogger(t)
	eng2 := New(cfg2, &MemSource{Events: evs}, sink)
	if err := eng2.Run(); err != nil {
		t.Fatalf("恢复运行失败: %v", err)
	}

	assertSameResults(t, base, sink.Results)
	if len(sink.Results) != len(base) {
		t.Fatalf("下游记录数=%d want %d：恢复后存在重复输出或丢失", len(sink.Results), len(base))
	}
	t.Logf("恢复后最终结果与基准一致: %d 条（崩溃前 %d 条未重复）", len(sink.Results), emittedBeforeCrash)
}

// TestRollbackBounded 位点回退可界定：重放范围 = (检查点连续位点, 崩溃点]，
// 不超过一个检查点间隔；重放事件在回滚后的状态中只被计入一次。
func TestRollbackBounded(t *testing.T) {
	evs := boundedShuffle(makeEvents(400, 5, []string{"a", "b"}), 7, 19)
	base, _ := runOnce(t, baseConfig(), evs)

	dir := t.TempDir()
	sink := NewMemSink()
	cfg := recoveryConfig(dir)
	cfg.FailAfter = 273
	eng1 := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng1.Run(); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}

	cp, ok, err := NewFileCheckpointStore(dir).LoadLatest()
	if err != nil || !ok {
		t.Fatalf("加载检查点失败: ok=%v err=%v", ok, err)
	}
	const crashPoint = 273
	if int(cp.NextOffset) != crashPoint-crashPoint%50 {
		t.Fatalf("检查点连续位点=%d want %d", cp.NextOffset, crashPoint-crashPoint%50)
	}
	replay := crashPoint - int(cp.NextOffset)
	if replay > cfg.CheckpointEvery {
		t.Fatalf("回退范围=%d 超过检查点间隔=%d：位点回退不可界定", replay, cfg.CheckpointEvery)
	}

	cfg2 := recoveryConfig(dir)
	eng2 := New(cfg2, &MemSource{Events: evs}, sink)
	if err := eng2.Run(); err != nil {
		t.Fatal(err)
	}
	st2 := eng2.Stats()
	if st2.Received != int64(len(evs)-int(cp.NextOffset)) {
		t.Fatalf("恢复后接收=%d want %d（严格从检查点位点续读）", st2.Received, len(evs)-int(cp.NextOffset))
	}
	// 250..272 在检查点之后处理、状态随崩溃回滚，恢复时是“首次计入”而非重复计入。
	if st2.Accepted != st2.Received {
		t.Fatalf("恢复后接受=%d 接收=%d：重放事件被错误重复计数", st2.Accepted, st2.Received)
	}
	assertSameResults(t, base, sink.Results)
	if len(sink.Results) != len(base) {
		t.Fatalf("下游记录数=%d want %d：存在重复输出", len(sink.Results), len(base))
	}
	t.Logf("位点回退有界: 从 %d 恢复, 重放 %d 条 (<= 间隔 50), 每条只计入一次, 输出不重复不丢失",
		cp.NextOffset, replay)
}

// TestRecoverFromEarlierCheckpoint 从较早检查点恢复：回退更远但结果仍一致。
func TestRecoverFromEarlierCheckpoint(t *testing.T) {
	evs := boundedShuffle(makeEvents(400, 5, []string{"a", "b"}), 7, 23)
	base, _ := runOnce(t, baseConfig(), evs)

	dir := t.TempDir()
	sink := NewMemSink()
	cfg := recoveryConfig(dir)
	cfg.FailAfter = 273
	eng1 := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng1.Run(); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}

	// 删掉最新检查点，模拟只能回到更早的检查点（chk-5, 位点 250 -> chk-4, 位点 200）。
	latest := filepath.Join(dir, "chk-000005.json")
	if _, err := os.Stat(latest); err != nil {
		t.Fatalf("预期存在 %s: %v", latest, err)
	}
	if err := os.Remove(latest); err != nil {
		t.Fatal(err)
	}

	cfg2 := recoveryConfig(dir)
	eng2 := New(cfg2, &MemSource{Events: evs}, sink)
	if err := eng2.Run(); err != nil {
		t.Fatal(err)
	}
	if got := eng2.Stats().Received; got != 400-200 {
		t.Fatalf("从较早检查点恢复接收=%d want %d", got, 400-200)
	}
	assertSameResults(t, base, sink.Results)
	if len(sink.Results) != len(base) {
		t.Fatalf("下游记录数=%d want %d：存在重复输出", len(sink.Results), len(base))
	}
	t.Logf("从较早检查点(位点200)恢复: 重放 73 条(回退到可用的最近检查点, 有界), 结果仍与基准一致")
}

// TestRecoveryWithDifferentConcurrency 恢复运行使用不同并发度，结果仍一致。
func TestRecoveryWithDifferentConcurrency(t *testing.T) {
	evs := boundedShuffle(makeEvents(300, 7, []string{"x", "y", "z"}), 5, 29)
	base, _ := runOnce(t, baseConfig(), evs)

	dir := t.TempDir()
	sink := NewMemSink()
	cfg := recoveryConfig(dir)
	cfg.ProducerConcurrency = 1
	cfg.FailAfter = 200
	eng1 := New(cfg, &MemSource{Events: evs}, sink)
	if err := eng1.Run(); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}

	cfg2 := recoveryConfig(dir)
	cfg2.ProducerConcurrency = 4 // 恢复时改变并发度
	eng2 := New(cfg2, &MemSource{Events: evs}, sink)
	if err := eng2.Run(); err != nil {
		t.Fatal(err)
	}
	assertSameResults(t, base, sink.Results)
	if len(sink.Results) != len(base) {
		t.Fatalf("下游记录数=%d want %d：存在重复输出", len(sink.Results), len(base))
	}
	t.Logf("并发度 1 崩溃、并发度 4 恢复: 结果一致")
}
