package pocketflow

import (
	"github.com/kemra/pocketflow/internal/models"
)

// Re-export core types so consumers only need to import "github.com/kemra/pocketflow/pkg/pocketflow"
type (
	Status        = models.Status
	RetryPolicy   = models.RetryPolicy
	StepState     = models.StepState
	WorkflowState = models.WorkflowState
	StepContext   = models.StepContext
)

const (
	StatusPending   = models.StatusPending
	StatusRunning   = models.StatusRunning
	StatusCompleted = models.StatusCompleted
	StatusFailed    = models.StatusFailed
	StatusRetrying  = models.StatusRetrying
	StatusSkipped   = models.StatusSkipped
	StatusCanceled  = models.StatusCanceled
)

// DefaultRetryPolicy returns standard retry settings.
func DefaultRetryPolicy() RetryPolicy {
	return models.DefaultRetryPolicy()
}

// NewStepContext creates an initialized StepContext.
var NewStepContext = models.NewStepContext
