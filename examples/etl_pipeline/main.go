package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kemra/pocketflow/internal/tui"
	"github.com/kemra/pocketflow/pkg/pocketflow"
)

func main() {
	app, err := pocketflow.NewApp("etl.db")
	if err != nil {
		log.Fatalf("failed to init pocketflow: %v", err)
	}
	defer app.Close()

	err = app.Workflow("daily-analytics-etl").
		Step("extract_raw_logs", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Extracting raw clickstream logs from S3 buckets...")
			return map[string]int{"records_extracted": 50000}, nil
		}).
		Step("transform_users", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Sanitizing and aggregating user sessions...")
			return map[string]int{"users_processed": 14200}, nil
		}).After("extract_raw_logs").
		Step("transform_metrics", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Calculating conversion rates and revenue metrics...")
			return map[string]float64{"total_revenue": 18950.75}, nil
		}).After("extract_raw_logs").
		Step("load_data_warehouse", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Bulk-inserting transformed metrics into analytical warehouse...")
			return map[string]string{"destination": "clickhouse.prod.cluster"}, nil
		}).After("transform_users", "transform_metrics").
		Step("publish_slack_report", func(ctx *pocketflow.StepContext) (any, error) {
			ctx.Logf("Publishing summary notification to #analytics-alerts...")
			return map[string]string{"slack_status": "message_posted"}, nil
		}).After("load_data_warehouse").
		Done()

	if err != nil {
		log.Fatalf("workflow registration error: %v", err)
	}

	state, err := app.Run(context.Background(), "daily-analytics-etl", nil)
	if err != nil {
		log.Fatalf("ETL run failed: %v", err)
	}

	summary, _ := app.GetWorkflow(state.RunID)
	fmt.Println(tui.RenderWorkflowSummary(summary))
}
