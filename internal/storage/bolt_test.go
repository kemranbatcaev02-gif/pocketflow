package storage

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/models"
)

func TestBoltStorage_WorkflowLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create bolt storage: %v", err)
	}
	defer store.Close()

	wf := &models.WorkflowState{
		WorkflowID: "order-flow",
		RunID:      "run-12345",
		Status:     models.StatusRunning,
		CreatedAt:  time.Now().UTC(),
		StartedAt:  time.Now().UTC(),
	}

	if err := store.SaveWorkflow(wf); err != nil {
		t.Fatalf("failed to save workflow: %v", err)
	}

	retrieved, err := store.GetWorkflow("run-12345")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if retrieved.WorkflowID != wf.WorkflowID || retrieved.Status != models.StatusRunning {
		t.Errorf("mismatched workflow data: got %+v, want %+v", retrieved, wf)
	}

	_, err = store.GetWorkflow("non-existent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	wf.Status = models.StatusCompleted
	wf.CompletedAt = time.Now().UTC()
	if err := store.SaveWorkflow(wf); err != nil {
		t.Fatalf("failed to update workflow: %v", err)
	}

	updated, _ := store.GetWorkflow("run-12345")
	if updated.Status != models.StatusCompleted {
		t.Errorf("expected completed status, got %v", updated.Status)
	}

	list, err := store.ListWorkflows(10)
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(list))
	}
}

func TestBoltStorage_StepStates(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "steps.db")

	store, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create bolt storage: %v", err)
	}
	defer store.Close()

	runID := "run-abc"

	step1 := &models.StepState{
		StepName: "step_one",
		Status:   models.StatusCompleted,
		Output:   json.RawMessage(`{"result": "ok", "value": 42}`),
	}
	step2 := &models.StepState{
		StepName: "step_two",
		Status:   models.StatusRunning,
		Attempt:  1,
	}

	if err := store.SaveStepState(runID, step1); err != nil {
		t.Fatalf("save step1 failed: %v", err)
	}
	if err := store.SaveStepState(runID, step2); err != nil {
		t.Fatalf("save step2 failed: %v", err)
	}

	s1, err := store.GetStepState(runID, "step_one")
	if err != nil {
		t.Fatalf("get step1 failed: %v", err)
	}
	if s1.Status != models.StatusCompleted {
		t.Errorf("expected status COMPLETED, got %v", s1.Status)
	}

	steps, err := store.ListStepStates(runID)
	if err != nil {
		t.Fatalf("list steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(steps))
	}
}
