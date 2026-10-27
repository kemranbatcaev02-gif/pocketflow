package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kemra/pocketflow/internal/tui"
	"github.com/kemra/pocketflow/pkg/pocketflow"
)

func main() {
	// Initialize PocketFlow with a local database file
	app, err := pocketflow.NewApp("ecommerce.db")
	if err != nil {
		log.Fatalf("failed to initialize pocketflow: %v", err)
	}
	defer app.Close()

	// Build the order fulfillment workflow
	err = app.Workflow("order-fulfillment").
		Step("validate_order", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Validating shopping cart contents...")
			return map[string]any{
				"order_id": "ORD-5544",
				"total":    149.99,
				"customer": "alice@example.com",
			}, nil
		}).
		Step("charge_payment", func(ctx *pocketflow.StepContext) (any, error) {
			var order map[string]any
			if err := ctx.BindInput("validate_order", &order); err != nil {
				return nil, err
			}
			ctx.Logf("Charging $%.2f via Payment Gateway...", order["total"])
			return map[string]string{
				"payment_status": "captured",
				"transaction_id": "ch_3N4jklMnoPqr",
			}, nil
		}).After("validate_order").WithRetries(3).
		Step("reserve_inventory", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Reserving stock in primary warehouse...")
			return map[string]string{
				"stock_status": "allocated",
				"warehouse":    "US-EAST-1",
			}, nil
		}).After("validate_order").
		Step("generate_receipt", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Generating PDF receipt...")
			return map[string]string{
				"receipt_pdf": "https://s3.amazonaws.com/receipts/ORD-5544.pdf",
			}, nil
		}).After("charge_payment").
		Step("send_confirmation", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Sending confirmation email and tracking link...")
			return map[string]bool{"notified": true}, nil
		}).After("generate_receipt", "reserve_inventory").
		Done()

	if err != nil {
		log.Fatalf("failed to register workflow: %v", err)
	}

	// Execute workflow
	state, err := app.Run(context.Background(), "order-fulfillment", nil)
	if err != nil {
		log.Fatalf("workflow execution failed: %v", err)
	}

	summary, _ := app.GetWorkflow(state.RunID)
	fmt.Println(tui.RenderWorkflowSummary(summary))
}
