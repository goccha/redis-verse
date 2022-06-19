package guards

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/goccha/logging/log"
	"github.com/goccha/redis-verse/locks"
	"github.com/goccha/redis-verse/redis"
	"time"
)

const (
	LockCountPrefix = "lock-count"
)

type Trigger interface {
	Fire(ctx context.Context) error          // 発火
	Fired(ctx context.Context) (bool, error) // 発火済み
	Reset(ctx context.Context)
}

func TimedCounter(key string, max int, expiration time.Duration, trigger Trigger) *LockCounter {
	return &LockCounter{
		key:        key,
		TryMax:     max,
		Expiration: expiration,
		Trigger:    trigger,
	}
}

type LockCounter struct {
	key         string        // ロック単位のキー文字列
	TryMax      int           // 最大回数
	Expiration  time.Duration // 判定対象期間
	Trigger     Trigger
	expirations []int64
	locked      bool
}

func (c *LockCounter) Clear(ctx context.Context) {
	c.clear(ctx)
	c.Trigger.Reset(ctx)
	c.locked = false
}

func (c *LockCounter) Key() string {
	return fmt.Sprintf("%s://%s", LockCountPrefix, c.key)
}

func (c *LockCounter) clear(ctx context.Context) {
	cmd := redis.Primary().Del(ctx, c.Key())
	if cmd.Err() != nil {
		log.Error(ctx).Err(cmd.Err()).Send()
		return
	}
	c.expirations = []int64{}
}

func (c *LockCounter) Increment(ctx context.Context) (bool, error) {
	if c.Trigger == nil {
		c.Trigger = &LockTrigger{Lock: &locks.RedisLock{Key: c.key, Duration: c.Expiration}}
	}
	locked, err := c.Trigger.Fired(ctx)
	if err != nil {
		return false, err
	}
	if locked {
		c.locked = true
		return locked, nil
	}
	c.locked = false
	c.expirations, err = c.increment(ctx)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (c *LockCounter) increment(ctx context.Context) ([]int64, error) {
	now := time.Now()
	cmd := redis.Primary().Get(ctx, c.Key())
	if cmd.Err() != nil && !redis.IsNil(cmd.Err()) {
		return nil, cmd.Err()
	}
	expirations := make([]int64, 0, c.TryMax)
	if !redis.IsNil(cmd.Err()) {
		if b, err := cmd.Bytes(); err != nil {
			return nil, err
		} else {
			if err = json.Unmarshal(b, &expirations); err != nil {
				return nil, err
			}
		}
	}
	exp := now.Add(c.Expiration)
	expirations = append(expirations, exp.Unix())
	return expirations, nil
}

func (c *LockCounter) gc(now time.Time, expirations []int64) []int64 {
	if len(expirations) >= c.TryMax {
		result := make([]int64, 0, c.TryMax)
		for _, v := range expirations {
			if v > now.Unix() {
				result = append(result, v)
			}
		}
		expirations = result
	}
	return expirations
}

func (c *LockCounter) Fire(ctx context.Context, f func()) (bool, error) {
	if c.locked {
		return true, nil
	}
	keys := c.gc(time.Now(), c.expirations)
	if len(keys) >= c.TryMax { // 規定回数以上に達した場合
		if err := c.Trigger.Fire(ctx); err != nil { // 対象キーをロック
			log.Error(ctx).Err(err).Send()
		}
		c.locked = true
		if f != nil {
			f()
		}
		c.clear(ctx)
		return true, nil
	}
	if len(keys) > 0 {
		v, err := json.Marshal(keys)
		if err != nil {
			return false, err
		}
		if cmd := redis.Primary().SetEX(ctx, c.Key(), v, c.Expiration); cmd.Err() != nil {
			return false, cmd.Err()
		}
	}
	c.locked = false
	c.expirations = keys
	return false, nil
}
