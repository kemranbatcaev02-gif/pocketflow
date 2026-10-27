package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/kemra/pocketflow/internal/tui"
	"github.com/kemra/pocketflow/pkg/pocketflow"
	"github.com/spf13/cobra"
)

var (
	dbPath  string
	version = "0.1.0"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "pocketflow",
		Short: "PocketFlow — Durable, embedded workflow & task orchestration engine",
		Long: `PocketFlow is a lightweight, zero-dependency durable execution engine.
It guarantees task execution across restarts with an embedded database,
DAG dependency scheduling, and automatic retries.`,
	}

	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "pocketflow.db", "Path to embedded database file")

	// 1. Version command
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print PocketFlow version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("PocketFlow v%s (built with %s)\n", version, "Go 1.27")
		},
	}

	// 2. List command
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List recent workflow runs",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := pocketflow.NewApp(dbPath)
			if err != nil {
				return err
			}
			defer app.Close()

			runs, err := app.ListWorkflows(20)
			if err != nil {
				return err
			}

			fmt.Println(tui.RenderBanner())
			if len(runs) == 0 {
				fmt.Println("No workflows recorded yet.")
				return nil
			}

			fmt.Printf("%-38s %-20s %-16s %s\n", "RUN ID", "WORKFLOW", "STATUS", "STARTED AT")
			fmt.Println(string(make([]rune, 85)))
			for _, r := range runs {
				fmt.Printf("%-38s %-20s %-25s %s\n",
					r.RunID,
					r.WorkflowID,
					tui.FormatStatus(r.Status),
					r.StartedAt.Format("2006-01-02 15:04:05"),
				)
			}
			return nil
		},
	}

	// 3. Status command
	statusCmd := &cobra.Command{
		Use:   "status <run-id>",
		Short: "Inspect a specific workflow execution",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := pocketflow.NewApp(dbPath)
			if err != nil {
				return err
			}
			defer app.Close()

			wf, err := app.GetWorkflow(args[0])
			if err != nil {
				return fmt.Errorf("failed to get workflow %s: %w", args[0], err)
			}

			fmt.Println(tui.RenderWorkflowSummary(wf))
			return nil
		},
	}

	// 4. Demo command: runs an interactive real-world eCommerce DAG workflow
	demoCmd := &cobra.Command{
		Use:   "demo",
		Short: "Run an interactive eCommerce demo pipeline with retries and DAG branches",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := pocketflow.NewApp(dbPath)
			if err != nil {
				return err
			}
			defer app.Close()

			fmt.Println(tui.RenderBanner())
			fmt.Println("Executing 'order-fulfillment-pipeline' DAG with parallel branches...")
			fmt.Println()

			// Define demo workflow
			var flakyAttempts int
			err = app.Workflow("order-fulfillment-pipeline").
				Step("validate_cart", func(ctx *pocketflow.StepContext) (any, error) {
					time.Sleep(100 * time.Millisecond)
					return map[string]any{"order_id": "ORD-9912", "amount": 250.0}, nil
				}).
				Step("charge_credit_card", func(ctx *pocketflow.StepContext) (any, error) {
					flakyAttempts++
					time.Sleep(150 * time.Millisecond)
					if flakyAttempts == 1 {
						return nil, fmt.Errorf("gateway timeout (simulated)")
					}
					return map[string]string{"txn_id": "TX-448102", "card": "**** 4242"}, nil
				}).After("validate_cart").WithRetries(3).WithBackoff(100*time.Millisecond, 2.0, 1*time.Second).
				Step("reserve_warehouse_stock", func(ctx *pocketflow.StepContext) (any, error) {
					time.Sleep(120 * time.Millisecond)
					return map[string]string{"warehouse": "DE-BER-01", "reserved": "true"}, nil
				}).After("validate_cart").
				Step("generate_tax_invoice", func(ctx *pocketflow.StepContext) (any, error) {
					time.Sleep(80 * time.Millisecond)
					return map[string]string{"invoice_no": "INV-2026-00441"}, nil
				}).After("charge_credit_card").
				Step("notify_customer_sms", func(ctx *pocketflow.StepContext) (any, error) {
					time.Sleep(60 * time.Millisecond)
					return map[string]string{"sms_status": "delivered"}, nil
				}).After("generate_tax_invoice", "reserve_warehouse_stock").
				Done()

			if err != nil {
				return fmt.Errorf("workflow definition failed: %w", err)
			}

			start := time.Now()
			result, err := app.Run(context.Background(), "order-fulfillment-pipeline", nil)
			if err != nil {
				fmt.Printf("Execution failed: %v\n", err)
			}

			// Render summary
			summary, err := app.GetWorkflow(result.RunID)
			if err == nil {
				fmt.Println(tui.RenderWorkflowSummary(summary))
			}
			fmt.Printf("Total pipeline elapsed: %s\n", time.Since(start).Round(time.Millisecond))
			fmt.Printf("Database stored checkpoint: %s\n", dbPath)
			return nil
		},
	}

	rootCmd.AddCommand(versionCmd, listCmd, statusCmd, demoCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
