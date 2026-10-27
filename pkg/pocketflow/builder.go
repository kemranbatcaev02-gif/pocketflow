package pocketflow

import (
	"fmt"
	"time"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/engine"
)

type WorkflowBuilder struct {
	app   *App
	id    string
	steps []*engine.StepConfig
	err   error
}

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

func (wb *WorkflowBuilder) Done() error {
	if wb.err != nil {
		return wb.err
	}
	if len(wb.steps) == 0 {
		return fmt.Errorf("workflow %q has no steps defined", wb.id)
	}
	return wb.app.engine.RegisterWorkflow(wb.id, wb.steps)
}

type StepBuilder struct {
	wfBuilder *WorkflowBuilder
	current   *engine.StepConfig
}

func (sb *StepBuilder) After(dependencies ...string) *StepBuilder {
	sb.current.Dependencies = append(sb.current.Dependencies, dependencies...)
	return sb
}

func (sb *StepBuilder) WithRetries(maxRetries int) *StepBuilder {
	sb.current.RetryPolicy.MaxRetries = maxRetries
	return sb
}

func (sb *StepBuilder) WithBackoff(initial time.Duration, multiplier float64, max time.Duration) *StepBuilder {
	sb.current.RetryPolicy.InitialInterval = initial
	sb.current.RetryPolicy.Multiplier = multiplier
	sb.current.RetryPolicy.MaxInterval = max
	return sb
}

func (sb *StepBuilder) WithTimeout(timeout time.Duration) *StepBuilder {
	sb.current.Timeout = timeout
	return sb
}

func (sb *StepBuilder) Step(name string, handler StepFunc) *StepBuilder {
	return sb.wfBuilder.Step(name, handler)
}

func (sb *StepBuilder) Done() error {
	return sb.wfBuilder.Done()
}
