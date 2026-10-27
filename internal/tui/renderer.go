package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/kemranbatcaev02-gif/pocketflow/internal/models"
)

var (
	brandStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1)

	headerBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#7D56F4")).
			Padding(0, 2).
			MarginBottom(1)

	completedBadge = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#04B575")).SetString("✔ COMPLETED")
	runningBadge   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00D7FF")).SetString("● RUNNING")
	failedBadge    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FF414D")).SetString("✖ FAILED")
	retryingBadge  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFAE00")).SetString("⟳ RETRYING")
	pendingBadge   = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262")).SetString("○ PENDING")
	skippedBadge   = lipgloss.NewStyle().Foreground(lipgloss.Color("#A0A0A0")).SetString("⊘ SKIPPED")

	dimText = lipgloss.NewStyle().Foreground(lipgloss.Color("#777777"))
	keyText = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#E0E0E0"))
	stepCol = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Width(28)
)

func RenderBanner() string {
	title := brandStyle.Render("⚡ PocketFlow")
	sub := dimText.Render("Durable Workflow Engine for Go")
	content := fmt.Sprintf("%s  %s", title, sub)
	return headerBox.Render(content)
}

func FormatStatus(s models.Status) string {
	switch s {
	case models.StatusCompleted:
		return completedBadge.String()
	case models.StatusRunning:
		return runningBadge.String()
	case models.StatusFailed:
		return failedBadge.String()
	case models.StatusRetrying:
		return retryingBadge.String()
	case models.StatusSkipped:
		return skippedBadge.String()
	case models.StatusPending:
		return pendingBadge.String()
	default:
		return string(s)
	}
}

func RenderWorkflowSummary(wf *models.WorkflowState) string {
	var b strings.Builder

	b.WriteString(RenderBanner())
	b.WriteString("\n")

	b.WriteString(fmt.Sprintf("%s: %s\n", keyText.Render("Workflow ID"), wf.WorkflowID))
	b.WriteString(fmt.Sprintf("%s: %s\n", keyText.Render("Run ID     "), wf.RunID))
	b.WriteString(fmt.Sprintf("%s: %s\n", keyText.Render("Status     "), FormatStatus(wf.Status)))

	duration := "in progress..."
	if !wf.CompletedAt.IsZero() && !wf.StartedAt.IsZero() {
		duration = wf.CompletedAt.Sub(wf.StartedAt).Round(time.Millisecond).String()
	}
	b.WriteString(fmt.Sprintf("%s: %s\n", keyText.Render("Duration   "), duration))

	if wf.Error != "" {
		b.WriteString(fmt.Sprintf("%s: %s\n", lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FF414D")).Render("Error      "), wf.Error))
	}

	b.WriteString("\n")
	b.WriteString(lipgloss.NewStyle().Bold(true).Underline(true).Render("Step Execution Breakdown:"))
	b.WriteString("\n\n")

	header := fmt.Sprintf("  %-24s %-16s %-10s %-10s %s", "STEP", "STATUS", "ATTEMPT", "TIME", "DETAILS")
	b.WriteString(dimText.Render(header) + "\n")
	b.WriteString(dimText.Render(strings.Repeat("─", 80)) + "\n")

	for _, step := range wf.StepStates {
		stepDuration := "-"
		if !step.CompletedAt.IsZero() && !step.StartedAt.IsZero() {
			stepDuration = step.CompletedAt.Sub(step.StartedAt).Round(time.Millisecond).String()
		}

		attemptStr := fmt.Sprintf("%d/%d", step.Attempt, step.MaxRetries)
		details := ""
		if step.Error != "" {
			details = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF6B6B")).Render("err: " + step.Error)
		} else if len(step.Output) > 0 {
			str := string(step.Output)
			if len(str) > 35 {
				str = str[:32] + "..."
			}
			details = dimText.Render(str)
		}

		line := fmt.Sprintf("  %-24s %-25s %-10s %-10s %s\n",
			stepCol.Render(step.StepName),
			FormatStatus(step.Status),
			attemptStr,
			stepDuration,
			details,
		)
		b.WriteString(line)
	}

	return b.String()
}
