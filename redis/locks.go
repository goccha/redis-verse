package redis

import (
	"context"
	"github.com/goccha/logging/log"
	"github.com/goccha/problems"
	"github.com/pkg/errors"
	"time"
)

func WithLock(ctx context.Context, key string, f func() error, tryMax ...int) error {
	max := 10
	if len(tryMax) > 0 {
		max = tryMax[0]
	}
	if err := tryLock(ctx, key, max); err != nil {
		return err
	}
	defer func() {
		if cmd := Primary().Del(ctx, key); cmd.Err() != nil {
			log.Error(ctx).Err(cmd.Err()).Send()
		}
	}()
	return f()
}

var LockTime = 15 * time.Second

func tryLock(ctx context.Context, key string, tryMax int) error {
	if tryMax <= 0 {
		tryMax = 1
	}
	for i := 0; i < tryMax; i++ {
		if ok, err := Lock(ctx, key, LockTime); err != nil {
			return err
		} else if ok {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return problems.New("").Unavailable("Processing...").Wrap()
}

const Locked = "1"

func Lock(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	if cmd := Primary().SetNX(ctx, key, Locked, expiration); cmd.Err() != nil {
		return false, errors.WithStack(cmd.Err())
	} else {
		if ok, err := cmd.Result(); err != nil {
			return false, errors.WithStack(err)
		} else if ok {
			return true, nil
		}
		return false, nil
	}
}
