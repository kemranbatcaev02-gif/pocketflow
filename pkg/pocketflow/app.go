package pocketflow

import (
	"context"
	"fmt"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/engine"
	"github.com/kemranbatcaev02-gif/pocketflow/internal/storage"
)

type StepFunc func(ctx *StepContext) (any, error)

type App struct {
	storage storage.Storage
	engine  *engine.Engine
}

type AppOption func(*engine.Engine)

func NewApp(dbPath string, opts ...AppOption) (*App, error) {
	if dbPath == "" {
		dbPath = "pocketflow.db"
	}

	store, err := storage.NewBoltStorage(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
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

func (a *App) Workflow(id string) *WorkflowBuilder {
	return &WorkflowBuilder{
		app: a,
		id:  id,
	}
}

func (a *App) Run(ctx context.Context, workflowID string, initialInputs map[string]any) (*WorkflowState, error) {
	return a.engine.Run(ctx, workflowID, initialInputs)
}

func (a *App) Resume(ctx context.Context, runID string) (*WorkflowState, error) {
	return a.engine.Resume(ctx, runID)
}

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

func (a *App) ListWorkflows(limit int) ([]*WorkflowState, error) {
	return a.storage.ListWorkflows(limit)
}

func (a *App) Close() error {
	return a.storage.Close()
}
