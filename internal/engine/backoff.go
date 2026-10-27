package engine

import (
	"math"
	"math/rand"
	"time"

	"github.com/kemra/pocketflow/internal/models"
)

// ComputeBackoff calculates the wait duration before the next retry attempt
// using exponential backoff with full jitter to avoid thundering herd problem.
func ComputeBackoff(attempt int, policy models.RetryPolicy) time.Duration {
	if attempt <= 1 {
		return applyJitter(policy.InitialInterval)
	}

	mult := math.Pow(policy.Multiplier, float64(attempt-1))
	interval := time.Duration(float64(policy.InitialInterval) * mult)

	if interval > policy.MaxInterval {
		interval = policy.MaxInterval
	}

	return applyJitter(interval)
}

// applyJitter adds randomized jitter between 75% and 125% of the interval.
func applyJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	// random factor between 0.75 and 1.25
	factor := 0.75 + rand.Float64()*0.50
	jittered := time.Duration(float64(base) * factor)
	return jittered
}
