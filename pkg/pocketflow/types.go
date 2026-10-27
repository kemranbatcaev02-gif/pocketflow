package pocketflow

import (
	"github.com/kemranbatcaev02-gif/pocketflow/internal/models"
)

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

func DefaultRetryPolicy() RetryPolicy {
	return models.DefaultRetryPolicy()
}

var NewStepContext = models.NewStepContext
