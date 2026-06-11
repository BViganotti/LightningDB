package lightning

import (
	"math"
	"math/rand"
	"time"
)

// computeBackoff calculates the delay before the next retry attempt.
func computeBackoff(attempt int, cfg RetryConfig) time.Duration {
	exp := math.Pow(2, float64(attempt))
	delay := float64(cfg.BaseDelay) * exp
	if delay > float64(cfg.MaxDelay) {
		delay = float64(cfg.MaxDelay)
	}
	jitter := (rand.Float64()*2 - 1) * cfg.JitterFactor * delay
	total := time.Duration(delay + jitter)
	if total < 0 {
		total = 0
	}
	return total
}
