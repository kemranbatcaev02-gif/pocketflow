package storage

import (
	"errors"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/models"
)

var (
	ErrNotFound      = errors.New("entity not found")
	ErrAlreadyExists = errors.New("entity already exists")
)

type Storage interface {
	SaveWorkflow(state *models.WorkflowState) error
	GetWorkflow(runID string) (*models.WorkflowState, error)
	ListWorkflows(limit int) ([]*models.WorkflowState, error)
	SaveStepState(runID string, step *models.StepState) error
	GetStepState(runID string, stepName string) (*models.StepState, error)
	ListStepStates(runID string) ([]*models.StepState, error)
	Close() error
}
