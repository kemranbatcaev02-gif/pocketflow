package pocketflow_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kemranbatcaev02-gif/pocketflow/pkg/pocketflow"
)

func TestApp_FluentAPISuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app_test.db")
	app, err := pocketflow.NewApp(dbPath)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}
	defer app.Close()

	err = app.Workflow("ecommerce-checkout").
		Step("cart_validate", func(ctx *pocketflow.StepContext) (any, error) {
			return map[string]int{"items_count": 2, "total_price": 1500}, nil
		}).
		Step("charge_payment", func(ctx *pocketflow.StepContext) (any, error) {
			var cart map[string]int
			if err := ctx.BindInput("cart_validate", &cart); err != nil {
				return nil, err
			}
			return map[string]string{"txn_id": "tx_998877", "status": "approved"}, nil
		}).After("cart_validate").WithRetries(2).
		Step("generate_invoice", func(ctx *pocketflow.StepContext) (any, error) {
			return map[string]string{"invoice_url": "https://cdn.example.com/inv/123.pdf"}, nil
		}).After("charge_payment").
		Done()

	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	result, err := app.Run(context.Background(), "ecommerce-checkout", nil)
	if err != nil {
		t.Fatalf("workflow execution failed: %v", err)
	}

	if result.Status != pocketflow.StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", result.Status)
	}

	fetched, err := app.GetWorkflow(result.RunID)
	if err != nil {
		t.Fatalf("failed to fetch workflow: %v", err)
	}
	if len(fetched.StepStates) != 3 {
		t.Errorf("expected 3 step states, got %d", len(fetched.StepStates))
	}
}
