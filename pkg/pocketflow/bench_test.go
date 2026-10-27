package pocketflow_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/dag"
	"github.com/kemranbatcaev02-gif/pocketflow/pkg/pocketflow"
)

// BenchmarkDAG_TopologicalSort measures graph resolution performance for 100-node DAGs.
func BenchmarkDAG_TopologicalSort(b *testing.B) {
	g := dag.NewGraph()
	_ = g.AddNode("node_0")
	for i := 1; i < 100; i++ {
		prev := fmt.Sprintf("node_%d", i-1)
		curr := fmt.Sprintf("node_%d", i)
		_ = g.AddNode(curr, prev)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := g.TopologicalSort()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWorkflowExecution measures full durable execution speed with disk checkpoints.
func BenchmarkWorkflowExecution(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.db")
	app, err := pocketflow.NewApp(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer app.Close()

	_ = app.Workflow("bench-flow").
		Step("step_1", func(ctx *pocketflow.StepContext) (any, error) {
			return 100, nil
		}).
		Step("step_2", func(ctx *pocketflow.StepContext) (any, error) {
			return 200, nil
		}).After("step_1").
		Step("step_3", func(ctx *pocketflow.StepContext) (any, error) {
			return 300, nil
		}).After("step_2").
		Done()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := app.Run(ctx, "bench-flow", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
