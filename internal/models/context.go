package models

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type StepContext struct {
	context.Context
	WorkflowID string
	RunID      string
	StepName   string
	Attempt    int

	mu      sync.RWMutex
	inputs  map[string]json.RawMessage
	outputs any
	logs    []string
}

func NewStepContext(ctx context.Context, workflowID, runID, stepName string, attempt int, inputs map[string]json.RawMessage) *StepContext {
	if ctx == nil {
		ctx = context.Background()
	}
	in := make(map[string]json.RawMessage)
	for k, v := range inputs {
		in[k] = v
	}
	return &StepContext{
		Context:    ctx,
		WorkflowID: workflowID,
		RunID:      runID,
		StepName:   stepName,
		Attempt:    attempt,
		inputs:     in,
	}
}

func (sc *StepContext) GetInput(stepName string) (json.RawMessage, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	val, ok := sc.inputs[stepName]
	return val, ok
}

func (sc *StepContext) BindInput(stepName string, target any) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	val, ok := sc.inputs[stepName]
	if !ok {
		return fmt.Errorf("no output found from upstream step %q", stepName)
	}
	return json.Unmarshal(val, target)
}

func (sc *StepContext) SetOutput(output any) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.outputs = output
}

func (sc *StepContext) GetOutput() any {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.outputs
}

func (sc *StepContext) Logf(format string, args ...any) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	msg := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
	sc.logs = append(sc.logs, msg)
}

func (sc *StepContext) GetLogs() []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	out := make([]string, len(sc.logs))
	copy(out, sc.logs)
	return out
}
