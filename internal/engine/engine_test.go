package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kemra/pocketflow/internal/models"
	"github.com/kemra/pocketflow/internal/storage"
)

func setupTestEngine(t *testing.T) (*Engine, storage.Storage) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "engine_test.db")
	store, err := storage.NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	eng := NewEngine(store, WithConcurrency(4))
	return eng, store
}

func TestEngine_DiamondWorkflowSuccess(t *testing.T) {
	eng, store := setupTestEngine(t)
	defer store.Close()

	var startExecuted, stepAExecuted, stepBExecuted, aggExecuted int32

	steps := []*StepConfig{
		{
			Name: "start",
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&startExecuted, 1)
				return map[string]int{"base": 10}, nil
			},
		},
		{
			Name:         "step_a",
			Dependencies: []string{"start"},
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&stepAExecuted, 1)
				var input map[string]int
				if err := ctx.BindInput("start", &input); err != nil {
					return nil, err
				}
				return map[string]int{"val": input["base"] + 5}, nil
			},
		},
		{
			Name:         "step_b",
			Dependencies: []string{"start"},
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&stepBExecuted, 1)
				var input map[string]int
				if err := ctx.BindInput("start", &input); err != nil {
					return nil, err
				}
				return map[string]int{"val": input["base"] * 2}, nil
			},
		},
		{
			Name:         "aggregator",
			Dependencies: []string{"step_a", "step_b"},
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&aggExecuted, 1)
				var a, b map[string]int
				_ = ctx.BindInput("step_a", &a)
				_ = ctx.BindInput("step_b", &b)
				return map[string]int{"total": a["val"] + b["val"]}, nil
			},
		},
	}

	err := eng.RegisterWorkflow("diamond-test", steps)
	if err != nil {
		t.Fatalf("register workflow failed: %v", err)
	}

	wfState, err := eng.Run(context.Background(), "diamond-test", nil)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if wfState.Status != models.StatusCompleted {
		t.Errorf("expected workflow status COMPLETED, got %s", wfState.Status)
	}

	if atomic.LoadInt32(&startExecuted) != 1 ||
		atomic.LoadInt32(&stepAExecuted) != 1 ||
		atomic.LoadInt32(&stepBExecuted) != 1 ||
		atomic.LoadInt32(&aggExecuted) != 1 {
		t.Errorf("steps execution count mismatch: start=%d, a=%d, b=%d, agg=%d",
			startExecuted, stepAExecuted, stepBExecuted, aggExecuted)
	}
}

func TestEngine_RetrySucceeds(t *testing.T) {
	eng, store := setupTestEngine(t)
	defer store.Close()

	var attempts int32

	steps := []*StepConfig{
		{
			Name: "flaky_step",
			RetryPolicy: models.RetryPolicy{
				MaxRetries:      3,
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				Multiplier:      1.5,
			},
			Handler: func(ctx *models.StepContext) (any, error) {
				count := atomic.AddInt32(&attempts, 1)
				if count < 2 {
					return nil, errors.New("transient network timeout")
				}
				return "success", nil
			},
		},
	}

	_ = eng.RegisterWorkflow("retry-test", steps)

	wfState, err := eng.Run(context.Background(), "retry-test", nil)
	if err != nil {
		t.Fatalf("expected workflow to succeed after retry, got: %v", err)
	}

	if wfState.Status != models.StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", wfState.Status)
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestEngine_ResumeSkipsCompletedSteps(t *testing.T) {
	eng, store := setupTestEngine(t)
	defer store.Close()

	var step1Runs, step2Runs int32
	var shouldFailStep2 int32 = 1

	steps := []*StepConfig{
		{
			Name: "step_1",
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&step1Runs, 1)
				return "step1_data", nil
			},
		},
		{
			Name:         "step_2",
			Dependencies: []string{"step_1"},
			RetryPolicy: models.RetryPolicy{
				MaxRetries: 1,
			},
			Handler: func(ctx *models.StepContext) (any, error) {
				atomic.AddInt32(&step2Runs, 1)
				if atomic.LoadInt32(&shouldFailStep2) == 1 {
					return nil, errors.New("simulated fatal step 2 failure")
				}
				return "step2_fixed", nil
			},
		},
	}

	_ = eng.RegisterWorkflow("resume-test", steps)

	// 1. Initial run: fails at step_2
	wfState, err := eng.Run(context.Background(), "resume-test", nil)
	if err == nil {
		t.Fatal("expected run to fail, got nil")
	}
	if wfState.Status != models.StatusFailed {
		t.Fatalf("expected status FAILED, got %s", wfState.Status)
	}
	if atomic.LoadInt32(&step1Runs) != 1 || atomic.LoadInt32(&step2Runs) != 1 {
		t.Fatalf("run counts before resume: step1=%d, step2=%d", step1Runs, step2Runs)
	}

	// 2. Fix condition and resume workflow
	atomic.StoreInt32(&shouldFailStep2, 0)
	resumedState, err := eng.Resume(context.Background(), wfState.RunID)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}

	if resumedState.Status != models.StatusCompleted {
		t.Errorf("expected COMPLETED status after resume, got %s", resumedState.Status)
	}

	// Step 1 MUST NOT be re-executed! It was already COMPLETED.
	if atomic.LoadInt32(&step1Runs) != 1 {
		t.Errorf("step 1 was re-executed during resume! got %d runs, want 1", step1Runs)
	}
	// Step 2 should have been executed a second time and succeeded
	if atomic.LoadInt32(&step2Runs) != 2 {
		t.Errorf("step 2 was not retried properly: got %d runs, want 2", step2Runs)
	}
}
