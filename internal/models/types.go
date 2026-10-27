package models

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	StatusRetrying  Status = "RETRYING"
	StatusSkipped   Status = "SKIPPED"
	StatusCanceled  Status = "CANCELED"
)

func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusSkipped || s == StatusCanceled
}

type RetryPolicy struct {
	MaxRetries      int           `json:"max_retries"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	Multiplier      float64       `json:"multiplier"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:      3,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
	}
}

type StepState struct {
	StepName    string          `json:"step_name"`
	Status      Status          `json:"status"`
	Attempt     int             `json:"attempt"`
	MaxRetries  int             `json:"max_retries"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	StartedAt   time.Time       `json:"started_at,omitempty"`
	CompletedAt time.Time       `json:"completed_at,omitempty"`
}

type WorkflowState struct {
	WorkflowID  string                `json:"workflow_id"`
	RunID       string                `json:"run_id"`
	Status      Status                `json:"status"`
	CreatedAt   time.Time             `json:"created_at"`
	StartedAt   time.Time             `json:"started_at,omitempty"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
	StepStates  map[string]*StepState `json:"step_states"`
	Error       string                `json:"error,omitempty"`
}
