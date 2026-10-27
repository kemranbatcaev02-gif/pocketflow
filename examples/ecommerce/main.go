package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/tui"
	"github.com/kemranbatcaev02-gif/pocketflow/pkg/pocketflow"
)

func main() {
	app, err := pocketflow.NewApp("ecommerce.db")
	if err != nil {
		log.Fatalf("failed to initialize pocketflow: %v", err)
	}
	defer app.Close()

	err = app.Workflow("order-fulfillment").
		Step("validate_order", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Validating cart")
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
			ctx.Logf("Charging $%.2f", order["total"])
			return map[string]string{
				"payment_status": "captured",
				"transaction_id": "ch_3N4jklMnoPqr",
			}, nil
		}).After("validate_order").WithRetries(3).
		Step("reserve_inventory", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Reserving warehouse stock")
			return map[string]string{
				"stock_status": "allocated",
				"warehouse":    "US-EAST-1",
			}, nil
		}).After("validate_order").
		Step("generate_receipt", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Generating receipt")
			return map[string]string{
				"receipt_pdf": "https://s3.amazonaws.com/receipts/ORD-5544.pdf",
			}, nil
		}).After("charge_payment").
		Step("send_confirmation", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Dispatching customer confirmation")
			return map[string]bool{"notified": true}, nil
		}).After("generate_receipt", "reserve_inventory").
		Done()

	if err != nil {
		log.Fatalf("failed to register workflow: %v", err)
	}

	state, err := app.Run(context.Background(), "order-fulfillment", nil)
	if err != nil {
		log.Fatalf("workflow execution failed: %v", err)
	}

	summary, _ := app.GetWorkflow(state.RunID)
	fmt.Println(tui.RenderWorkflowSummary(summary))
}
