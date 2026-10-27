package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kemra/pocketflow/internal/dag"
	"github.com/kemra/pocketflow/internal/models"
	"github.com/kemra/pocketflow/internal/storage"
)

// StepHandler is the function signature executed by each workflow step.
type StepHandler func(ctx *models.StepContext) (any, error)

// StepConfig defines configuration and execution logic for a single step.
type StepConfig struct {
	Name         string
	Handler      StepHandler
	Dependencies []string
	RetryPolicy  models.RetryPolicy
	Timeout      time.Duration
}

// WorkflowDefinition contains registered workflow configuration and its validated DAG.
type WorkflowDefinition struct {
	ID    string
	Steps map[string]*StepConfig
	Graph *dag.Graph
}

// EngineCallbacks allows hooking into step and workflow lifecycle events.
type EngineCallbacks struct {
	OnStepStarted   func(runID, stepName string)
	OnStepCompleted func(runID, stepName string, duration time.Duration)
	OnStepFailed    func(runID, stepName string, err error)
	OnStepRetrying  func(runID, stepName string, attempt int, backoff time.Duration)
}

// Engine orchestrates DAG workflows, handles worker concurrency and ensures state durability.
type Engine struct {
	storage     storage.Storage
	concurrency int
	workflows   map[string]*WorkflowDefinition
	callbacks   EngineCallbacks
	mu          sync.RWMutex
}

// Option configures the Engine.
type Option func(*Engine)

// WithConcurrency limits the maximum number of concurrent steps across executions.
func WithConcurrency(limit int) Option {
	return func(e *Engine) {
		if limit > 0 {
			e.concurrency = limit
		}
	}
}

// WithCallbacks attaches event hooks to the engine.
func WithCallbacks(cb EngineCallbacks) Option {
	return func(e *Engine) {
		e.callbacks = cb
	}
}

// NewEngine initializes an Engine with the provided persistence storage.
func NewEngine(store storage.Storage, opts ...Option) *Engine {
	e := &Engine{
		storage:     store,
		concurrency: 8, // default worker concurrency
		workflows:   make(map[string]*WorkflowDefinition),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// RegisterWorkflow adds a workflow definition and validates its dependency graph.
func (e *Engine) RegisterWorkflow(id string, steps []*StepConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if id == "" {
		return fmt.Errorf("workflow id cannot be empty")
	}
	if len(steps) == 0 {
		return fmt.Errorf("workflow %q must have at least one step", id)
	}

	g := dag.NewGraph()
	stepMap := make(map[string]*StepConfig)

	for _, step := range steps {
		if step == nil || step.Name == "" {
			return fmt.Errorf("invalid step configuration in workflow %q", id)
		}
		if _, exists := stepMap[step.Name]; exists {
			return fmt.Errorf("duplicate step name %q in workflow %q", step.Name, id)
		}
		stepMap[step.Name] = step

		if err := g.AddNode(step.Name, step.Dependencies...); err != nil {
			return fmt.Errorf("failed to add step %q: %w", step.Name, err)
		}
	}

	if err := g.Validate(); err != nil {
		return fmt.Errorf("invalid workflow graph for %q: %w", id, err)
	}

	e.workflows[id] = &WorkflowDefinition{
		ID:    id,
		Steps: stepMap,
		Graph: g,
	}
	return nil
}

// Run executes a registered workflow from the beginning.
func (e *Engine) Run(ctx context.Context, workflowID string, initialInputs map[string]any) (*models.WorkflowState, error) {
	e.mu.RLock()
	def, exists := e.workflows[workflowID]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow %q is not registered", workflowID)
	}

	runID := uuid.New().String()
	now := time.Now().UTC()

	wfState := &models.WorkflowState{
		WorkflowID: workflowID,
		RunID:      runID,
		Status:     models.StatusRunning,
		CreatedAt:  now,
		StartedAt:  now,
		StepStates: make(map[string]*models.StepState),
	}

	// Prepare initial step states
	for name, cfg := range def.Steps {
		stepState := &models.StepState{
			StepName:   name,
			Status:     models.StatusPending,
			MaxRetries: cfg.RetryPolicy.MaxRetries,
		}
		wfState.StepStates[name] = stepState
		_ = e.storage.SaveStepState(runID, stepState)
	}

	if err := e.storage.SaveWorkflow(wfState); err != nil {
		return nil, fmt.Errorf("failed to save initial workflow state: %w", err)
	}

	return e.executeWorkflow(ctx, def, wfState, initialInputs)
}

// Resume re-executes an interrupted or failed workflow run, skipping already completed steps.
func (e *Engine) Resume(ctx context.Context, runID string) (*models.WorkflowState, error) {
	wfState, err := e.storage.GetWorkflow(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for resume: %w", err)
	}

	e.mu.RLock()
	def, exists := e.workflows[wfState.WorkflowID]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow %q is not registered", wfState.WorkflowID)
	}

	// Populate latest step states from storage
	stepList, err := e.storage.ListStepStates(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to load step states: %w", err)
	}

	wfState.StepStates = make(map[string]*models.StepState)
	for _, s := range stepList {
		wfState.StepStates[s.StepName] = s
	}

	wfState.Status = models.StatusRunning
	_ = e.storage.SaveWorkflow(wfState)

	return e.executeWorkflow(ctx, def, wfState, nil)
}

// executeWorkflow carries out stage-by-stage execution.
func (e *Engine) executeWorkflow(ctx context.Context, def *WorkflowDefinition, wfState *models.WorkflowState, initialInputs map[string]any) (*models.WorkflowState, error) {
	stages, err := def.Graph.ComputeStages()
	if err != nil {
		return nil, err
	}

	sem := make(chan struct{}, e.concurrency)
	completedOutputs := make(map[string]json.RawMessage)
	var outputMu sync.RWMutex

	// Pre-fill outputs from already completed steps (e.g. during Resume)
	for name, s := range wfState.StepStates {
		if s.Status == models.StatusCompleted && len(s.Output) > 0 {
			completedOutputs[name] = s.Output
		}
	}

	// Pre-fill initial inputs if provided
	for k, v := range initialInputs {
		raw, err := json.Marshal(v)
		if err == nil {
			completedOutputs[k] = raw
		}
	}

	for _, stage := range stages {
		var wg sync.WaitGroup
		stageErrChan := make(chan error, len(stage))

		for _, stepName := range stage {
			stepCfg := def.Steps[stepName]
			stepState := wfState.StepStates[stepName]

			// Skip steps that were already completed in previous runs
			if stepState.Status == models.StatusCompleted {
				continue
			}

			wg.Add(1)
			go func(name string, cfg *StepConfig, st *models.StepState) {
				defer wg.Done()

				// Acquire worker slot
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					stageErrChan <- ctx.Err()
					return
				}

				// Prepare inputs from upstream dependencies
				outputMu.RLock()
				inputs := make(map[string]json.RawMessage)
				for _, dep := range cfg.Dependencies {
					if out, ok := completedOutputs[dep]; ok {
						inputs[dep] = out
					}
				}
				outputMu.RUnlock()

				err := e.executeStepWithRetry(ctx, wfState.RunID, cfg, st, inputs)
				if err != nil {
					stageErrChan <- fmt.Errorf("step %q failed: %w", name, err)
					return
				}

				outputMu.Lock()
				completedOutputs[name] = st.Output
				outputMu.Unlock()
			}(stepName, stepCfg, stepState)
		}

		wg.Wait()
		close(stageErrChan)

		// Check if any step in this stage failed
		var firstErr error
		for err := range stageErrChan {
			if firstErr == nil {
				firstErr = err
			}
		}

		if firstErr != nil {
			// Mark remaining uncompleted steps as SKIPPED
			for _, st := range wfState.StepStates {
				if st.Status == models.StatusPending {
					st.Status = models.StatusSkipped
					_ = e.storage.SaveStepState(wfState.RunID, st)
				}
			}

			wfState.Status = models.StatusFailed
			wfState.CompletedAt = time.Now().UTC()
			wfState.Error = firstErr.Error()
			_ = e.storage.SaveWorkflow(wfState)
			return wfState, firstErr
		}
	}

	// All stages succeeded
	wfState.Status = models.StatusCompleted
	wfState.CompletedAt = time.Now().UTC()
	_ = e.storage.SaveWorkflow(wfState)

	return wfState, nil
}

// executeStepWithRetry handles attempt counts, backoff delay, and timeout context for a single step.
func (e *Engine) executeStepWithRetry(ctx context.Context, runID string, cfg *StepConfig, st *models.StepState, inputs map[string]json.RawMessage) error {
	policy := cfg.RetryPolicy
	if policy.MaxRetries <= 0 {
		policy.MaxRetries = 1
	}

	for {
		st.Attempt++
		st.StartedAt = time.Now().UTC()
		st.Status = models.StatusRunning
		_ = e.storage.SaveStepState(runID, st)

		if e.callbacks.OnStepStarted != nil {
			e.callbacks.OnStepStarted(runID, cfg.Name)
		}

		start := time.Now()
		stepCtx := models.NewStepContext(ctx, runID, runID, cfg.Name, st.Attempt, inputs)

		var cancel context.CancelFunc = func() {}
		if cfg.Timeout > 0 {
			stepCtx.Context, cancel = context.WithTimeout(ctx, cfg.Timeout)
		}

		result, err := cfg.Handler(stepCtx)
		cancel()

		if err == nil {
			st.Status = models.StatusCompleted
			st.CompletedAt = time.Now().UTC()
			st.Error = ""
			if result != nil {
				raw, marshalErr := json.Marshal(result)
				if marshalErr == nil {
					st.Output = raw
				}
			}
			_ = e.storage.SaveStepState(runID, st)

			if e.callbacks.OnStepCompleted != nil {
				e.callbacks.OnStepCompleted(runID, cfg.Name, time.Since(start))
			}
			return nil
		}

		// Handle failure & retry
		st.Error = err.Error()

		if st.Attempt >= policy.MaxRetries || ctx.Err() != nil {
			st.Status = models.StatusFailed
			st.CompletedAt = time.Now().UTC()
			_ = e.storage.SaveStepState(runID, st)

			if e.callbacks.OnStepFailed != nil {
				e.callbacks.OnStepFailed(runID, cfg.Name, err)
			}
			return err
		}

		// Calculate backoff before retry
		backoff := ComputeBackoff(st.Attempt, policy)
		st.Status = models.StatusRetrying
		_ = e.storage.SaveStepState(runID, st)

		if e.callbacks.OnStepRetrying != nil {
			e.callbacks.OnStepRetrying(runID, cfg.Name, st.Attempt, backoff)
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			st.Status = models.StatusCanceled
			_ = e.storage.SaveStepState(runID, st)
			return ctx.Err()
		}
	}
}
