package locks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goccha/logging/log"
	"github.com/goccha/redis-verse/redis"
)

const (
	BlockPrefix = "blocks"
)

type Failure struct{}

func (err *Failure) Error() string {
	return "lock failure"
}

var ErrLock *Failure

func Failed(err error) bool {
	return errors.As(err, &ErrLock)
}

// Lock ロックを行うためのインターフェース
type Lock interface {
	Lock(ctx context.Context) (bool, error)
	UnLock(ctx context.Context)
	Locked(ctx context.Context) (bool, error)
}

func LockKey(k string) string {
	return fmt.Sprintf("%s://%s", BlockPrefix, k)
}

// RedisLock Redisを使ってロック
type RedisLock struct {
	Key      string        // ロックするキー
	Duration time.Duration // ロックする期間
}

func (l *RedisLock) String() string {
	return l.key()
}

func (l *RedisLock) key() string {
	return LockKey(l.Key)
}
func (l *RedisLock) Lock(ctx context.Context) (bool, error) {
	return redis.Lock(ctx, l.key(), l.Duration)
}
func (l *RedisLock) UnLock(ctx context.Context) {
	if cmd := redis.Primary().Del(ctx, l.key()); cmd.Err() != nil {
		log.Error(ctx).Err(cmd.Err()).Send()
	}
}
func (l *RedisLock) Locked(ctx context.Context) (bool, error) {
	cmd := redis.Primary().Get(ctx, l.key())
	if cmd.Err() != nil {
		if redis.IsNil(cmd.Err()) {
			return false, nil
		}
		return false, cmd.Err()
	}
	v := cmd.Val()
	if v == redis.Locked {
		return true, nil
	}
	return false, nil
}
