package guards

import (
	"context"
	"fmt"
	"time"

	"github.com/goccha/logging/log"
	"github.com/goccha/redis-verse/redis"
)

const (
	LimitCountPrefix = "limit-count"
)

func AccessLimitCounter(key string, max int, expiration time.Duration) *LimitCounter {
	return &LimitCounter{
		key:        key,
		TryMax:     max,
		Expiration: expiration,
	}
}

type ExceedLimitError struct {
	Limit    int
	Count    int64
	ExpireAt int64
}

func (e *ExceedLimitError) Error() string {
	return fmt.Sprintf("exceeded limit: limit=%d, count=%d", e.Limit, e.Count)
}

type LimitCounter struct {
	key        string        // ロック単位のキー文字列
	TryMax     int           // 最大回数
	Expiration time.Duration // 判定対象期間
	expireAt   int64
}

func (c *LimitCounter) Key() string {
	return fmt.Sprintf("%s://%s", LimitCountPrefix, c.key)
}

func (c *LimitCounter) Increment(ctx context.Context) (int64, error) {
	values := []interface{}{c.TryMax, int64(c.Expiration.Seconds())}
	v, err := incrementN.Run(ctx, redis.Primary(), []string{c.Key()}, values...).Result()
	if err != nil {
		return -1, err
	}
	values = v.([]interface{})
	log.Debug(ctx).Msgf("limit counter values: %v", values)
	ok := values[0].(int64)
	cnt := values[1].(int64)
	expireAt := values[2].(int64)
	if ok == 0 {
		return cnt, &ExceedLimitError{
			Limit:    c.TryMax,
			Count:    cnt,
			ExpireAt: expireAt,
		}
	}
	c.expireAt = expireAt
	return cnt, nil
}

func (c *LimitCounter) Clear(ctx context.Context) {
	cmd := redis.Primary().LRem(ctx, c.Key(), 1, c.expireAt)
	if cmd.Err() != nil {
		fmt.Printf("error: %v\n", cmd.Err())
		return
	}
}
