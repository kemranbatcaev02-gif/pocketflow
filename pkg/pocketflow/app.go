package pocketflow

import (
	"context"
	"fmt"

	"github.com/kemra/pocketflow/internal/engine"
	"github.com/kemra/pocketflow/internal/storage"
)

// StepFunc is the function signature executed by each step in a workflow.
type StepFunc func(ctx *StepContext) (any, error)

// App manages the embedded database and execution engine for workflows.
type App struct {
	storage storage.Storage
	engine  *engine.Engine
}

// AppOption allows configuring the App instance.
type AppOption func(*engine.Engine)

// NewApp initializes a PocketFlow application backed by an embedded database.
func NewApp(dbPath string, opts ...AppOption) (*App, error) {
	if dbPath == "" {
		dbPath = "pocketflow.db"
	}

	store, err := storage.NewBoltStorage(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open embedded database: %w", err)
	}

	eng := engine.NewEngine(store)
	for _, opt := range opts {
		opt(eng)
	}

	return &App{
		storage: store,
		engine:  eng,
	}, nil
}

// Workflow starts building a new workflow definition with a fluent API.
func (a *App) Workflow(id string) *WorkflowBuilder {
	return &WorkflowBuilder{
		app: a,
		id:  id,
	}
}

// Run executes a registered workflow run.
func (a *App) Run(ctx context.Context, workflowID string, initialInputs map[string]any) (*WorkflowState, error) {
	return a.engine.Run(ctx, workflowID, initialInputs)
}

// Resume re-runs an interrupted workflow, skipping already completed steps.
func (a *App) Resume(ctx context.Context, runID string) (*WorkflowState, error) {
	return a.engine.Resume(ctx, runID)
}

// GetWorkflow retrieves the state and step checkpoints of a specific run.
func (a *App) GetWorkflow(runID string) (*WorkflowState, error) {
	wf, err := a.storage.GetWorkflow(runID)
	if err != nil {
		return nil, err
	}
	steps, err := a.storage.ListStepStates(runID)
	if err == nil {
		wf.StepStates = make(map[string]*StepState)
		for _, s := range steps {
			wf.StepStates[s.StepName] = s
		}
	}
	return wf, nil
}

// ListWorkflows returns the most recent workflow executions.
func (a *App) ListWorkflows(limit int) ([]*WorkflowState, error) {
	return a.storage.ListWorkflows(limit)
}

// Close flushes data and closes the embedded database.
func (a *App) Close() error {
	return a.storage.Close()
}
