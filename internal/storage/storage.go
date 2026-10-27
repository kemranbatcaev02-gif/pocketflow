package storage

import (
	"errors"

	"github.com/kemra/pocketflow/internal/models"
)

var (
	// ErrNotFound is returned when a requested entity does not exist.
	ErrNotFound = errors.New("entity not found")
	// ErrAlreadyExists is returned when attempting to create an entity with an existing key.
	ErrAlreadyExists = errors.New("entity already exists")
)

// Storage defines the interface for persisting workflows and step checkpoints.
type Storage interface {
	// SaveWorkflow stores or updates the workflow state.
	SaveWorkflow(state *models.WorkflowState) error

	// GetWorkflow retrieves a workflow state by its run ID.
	GetWorkflow(runID string) (*models.WorkflowState, error)

	// ListWorkflows retrieves the latest workflow runs.
	ListWorkflows(limit int) ([]*models.WorkflowState, error)

	// SaveStepState persists a checkpoint for an individual step.
	SaveStepState(runID string, step *models.StepState) error

	// GetStepState retrieves a step's latest checkpoint state.
	GetStepState(runID string, stepName string) (*models.StepState, error)

	// ListStepStates retrieves all step states for a given workflow run.
	ListStepStates(runID string) ([]*models.StepState, error)

	// Close safely flushes buffers and closes the database connection.
	Close() error
}
