package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kemra/pocketflow/internal/models"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketWorkflows  = []byte("workflows")
	bucketStepStates = []byte("step_states")
)

// BoltStorage implements Storage using the pure-Go embedded bbolt engine.
type BoltStorage struct {
	db *bolt.DB
}

// NewBoltStorage opens or creates a bbolt database at the given path.
func NewBoltStorage(dbPath string) (*BoltStorage, error) {
	if dir := filepath.Dir(dbPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	opts := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(dbPath, 0600, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database at %s: %w", dbPath, err)
	}

	// Initialize buckets
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketWorkflows); err != nil {
			return fmt.Errorf("create bucket workflows: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(bucketStepStates); err != nil {
			return fmt.Errorf("create bucket step_states: %w", err)
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &BoltStorage{db: db}, nil
}

// SaveWorkflow persists the workflow state.
func (s *BoltStorage) SaveWorkflow(state *models.WorkflowState) error {
	if state == nil || state.RunID == "" {
		return fmt.Errorf("invalid workflow state: missing run_id")
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow state: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketWorkflows)
		return b.Put([]byte(state.RunID), data)
	})
}

// GetWorkflow retrieves the workflow state by run ID.
func (s *BoltStorage) GetWorkflow(runID string) (*models.WorkflowState, error) {
	var state models.WorkflowState

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketWorkflows)
		data := b.Get([]byte(runID))
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &state)
	})
	if err != nil {
		return nil, err
	}

	return &state, nil
}

// ListWorkflows retrieves up to limit workflows, ordered from newest to oldest.
func (s *BoltStorage) ListWorkflows(limit int) ([]*models.WorkflowState, error) {
	var list []*models.WorkflowState

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketWorkflows)
		c := b.Cursor()

		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var st models.WorkflowState
			if err := json.Unmarshal(v, &st); err != nil {
				continue
			}
			list = append(list, &st)
			if limit > 0 && len(list) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

func makeStepKey(runID, stepName string) []byte {
	return []byte(fmt.Sprintf("%s:%s", runID, stepName))
}

// SaveStepState persists an individual step checkpoint.
func (s *BoltStorage) SaveStepState(runID string, step *models.StepState) error {
	if runID == "" || step == nil || step.StepName == "" {
		return fmt.Errorf("invalid step state or run_id")
	}

	data, err := json.Marshal(step)
	if err != nil {
		return fmt.Errorf("failed to marshal step state: %w", err)
	}

	key := makeStepKey(runID, step.StepName)
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStepStates)
		return b.Put(key, data)
	})
}

// GetStepState retrieves a step's state by run ID and step name.
func (s *BoltStorage) GetStepState(runID string, stepName string) (*models.StepState, error) {
	var step models.StepState
	key := makeStepKey(runID, stepName)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStepStates)
		data := b.Get(key)
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &step)
	})
	if err != nil {
		return nil, err
	}

	return &step, nil
}

// ListStepStates returns all step states for a given run ID.
func (s *BoltStorage) ListStepStates(runID string) ([]*models.StepState, error) {
	prefix := []byte(runID + ":")
	var steps []*models.StepState

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStepStates)
		c := b.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var st models.StepState
			if err := json.Unmarshal(v, &st); err != nil {
				continue
			}
			steps = append(steps, &st)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return steps, nil
}

// Close flushes buffers and closes the bbolt database.
func (s *BoltStorage) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
