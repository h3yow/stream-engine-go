package engine

import (
	"context"
	"testing"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/sink"
)

func tBackground() context.Context { return context.Background() }

func newTestEngine(t *testing.T, cfg Config) (*sink.CollectingSink, *Engine) {
	t.Helper()
	return newTestEngineStore(t, cfg, nil)
}

func newTestEngineStore(t *testing.T, cfg Config, store checkpoint.Store) (*sink.CollectingSink, *Engine) {
	t.Helper()
	snk := sink.NewCollectingSink(0)
	eng := New(cfg, snk, store)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return snk, eng
}
