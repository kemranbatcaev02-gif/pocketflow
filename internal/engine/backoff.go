package engine

import (
	"math"
	"math/rand"
	"time"

	"github.com/kemranbatcaev02-gif/pocketflow/internal/models"
)

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

func applyJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	factor := 0.75 + rand.Float64()*0.50
	return time.Duration(float64(base) * factor)
}
