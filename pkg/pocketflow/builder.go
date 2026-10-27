package pocketflow

import (
	"fmt"
	"time"

	"github.com/kemra/pocketflow/internal/engine"
)

// WorkflowBuilder allows fluent chaining of workflow steps.
type WorkflowBuilder struct {
	app   *App
	id    string
	steps []*engine.StepConfig
	err   error
}

// Step registers a new step with the given name and handler.
func (wb *WorkflowBuilder) Step(name string, handler StepFunc) *StepBuilder {
	cfg := &engine.StepConfig{
		Name: name,
		Handler: func(ctx *StepContext) (any, error) {
			return handler(ctx)
		},
		RetryPolicy: DefaultRetryPolicy(),
	}
	wb.steps = append(wb.steps, cfg)

	return &StepBuilder{
		wfBuilder: wb,
		current:   cfg,
	}
}

// Done validates and registers the workflow with the engine.
func (wb *WorkflowBuilder) Done() error {
	if wb.err != nil {
		return wb.err
	}
	if len(wb.steps) == 0 {
		return fmt.Errorf("workflow %q has no steps defined", wb.id)
	}
	return wb.app.engine.RegisterWorkflow(wb.id, wb.steps)
}

// StepBuilder configures an individual step in the workflow.
type StepBuilder struct {
	wfBuilder *WorkflowBuilder
	current   *engine.StepConfig
}

// After specifies the upstream steps that must finish before this step can execute.
func (sb *StepBuilder) After(dependencies ...string) *StepBuilder {
	sb.current.Dependencies = append(sb.current.Dependencies, dependencies...)
	return sb
}

// WithRetries sets the maximum number of retry attempts on failure.
func (sb *StepBuilder) WithRetries(maxRetries int) *StepBuilder {
	sb.current.RetryPolicy.MaxRetries = maxRetries
	return sb
}

// WithBackoff configures the initial retry delay and multiplier.
func (sb *StepBuilder) WithBackoff(initial time.Duration, multiplier float64, max time.Duration) *StepBuilder {
	sb.current.RetryPolicy.InitialInterval = initial
	sb.current.RetryPolicy.Multiplier = multiplier
	sb.current.RetryPolicy.MaxInterval = max
	return sb
}

// WithTimeout sets a maximum execution duration for this step.
func (sb *StepBuilder) WithTimeout(timeout time.Duration) *StepBuilder {
	sb.current.Timeout = timeout
	return sb
}

// Step adds another step to the workflow chain.
func (sb *StepBuilder) Step(name string, handler StepFunc) *StepBuilder {
	return sb.wfBuilder.Step(name, handler)
}

// Done finalizes the workflow registration.
func (sb *StepBuilder) Done() error {
	return sb.wfBuilder.Done()
}
