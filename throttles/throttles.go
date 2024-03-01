package throttles

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v9"
	"github.com/goccha/logging/log"
	"github.com/pkg/errors"
)

func SetLimiter(rateLimiter *redis_rate.Limiter) {
	limiter = rateLimiter
}

func Setup(retryMax int, rateLimiter *redis_rate.Limiter) {
	if retryMax > 0 {
		defaultMax = retryMax
	}
	SetLimiter(rateLimiter)
}

type OverflowError struct {
}

func (e *OverflowError) Error() string {
	return "Limit has been exceeded."
}

func IsOverflow(err error) bool {
	overflow := &OverflowError{}
	return errors.As(err, &overflow)
}

var defaultMax = 5
var limiter *redis_rate.Limiter

func Limit(ctx context.Context, id string, rate int, f func() error, retryMax ...int) (err error) {
	return LimitPer(ctx, id, redis_rate.PerSecond(rate), f, retryMax...)
}

func try(ctx context.Context, id string, limit redis_rate.Limit) (retry bool, err error) {
	if result, err := limiter.Allow(ctx, id, limit); err != nil {
		return true, errors.WithStack(err)
	} else if result.Allowed <= 0 {
		log.Trace(ctx).Int("allowed", result.Allowed).
			Int("remaining", result.Remaining).
			Dur("retryAfter", result.RetryAfter).
			Dur("resetAfter", result.ResetAfter).Send()
		time.Sleep(result.RetryAfter)
		return true, nil
	}
	return false, nil
}

func LimitPer(ctx context.Context, id string, limit redis_rate.Limit, f func() error, retryMax ...int) (err error) {
	maxCnt := defaultMax
	if len(retryMax) > 0 {
		maxCnt = retryMax[0]
	}
	retry := true
	for i := 0; retry && i < maxCnt; i++ {
		if retry, err = try(ctx, id, limit); err != nil {
			return err
		}
	}
	if retry {
		return &OverflowError{}
	}
	err = f()
	return
}
