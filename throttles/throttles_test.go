package throttles

import (
	"context"
	"errors"
	"github.com/go-redis/redis_rate/v9"
	"github.com/goccha/redis-verse/redis"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := redis.Setup(ctx, &redis.EnvBuilder{}); err != nil {
		panic(err)
	}
	if err := redis.WaitForActivation(ctx); err != nil {
		os.Exit(1)
	}
	SetLimiter(redis_rate.NewLimiter(redis.Primary()))
	code := m.Run()
	os.Exit(code)
}

func TestLimit(t *testing.T) {
	ctx := context.Background()
	id := "test1"
	rate := 3
	var cnt int32
	var pre time.Time
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				err := Limit(ctx, id, rate, func() error {
					if pre.IsZero() {
						pre = time.Now()
					} else if int(cnt)%rate == 0 {
						diff := time.Since(pre)
						pre = time.Now()
						if int(cnt) > rate {
							assert.True(t, diff >= 950*time.Millisecond, diff.Seconds())
						}
					}
					_ = atomic.AddInt32(&cnt, 1)
					return nil
				})
				if !IsOverflow(err) {
					assert.NoError(t, err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestLimitSingle(t *testing.T) {
	ctx := context.Background()
	id := "test2"
	rate := 10
	var cnt int
	var pre time.Time
	for j := 0; j < 30; j++ {
		err := Limit(ctx, id, rate, func() error {
			if pre.IsZero() {
				pre = time.Now()
			} else if cnt%rate == 0 {
				diff := time.Since(pre)
				pre = time.Now()
				if cnt > rate {
					assert.True(t, diff >= 950*time.Millisecond, diff.Seconds())
				}
			}
			cnt++
			return nil
		})
		if !IsOverflow(err) {
			assert.NoError(t, err)
		}
	}
}

func TestOverflowError_Error(t *testing.T) {
	var err error
	err = &OverflowError{}
	assert.Equal(t, IsOverflow(err), true)
	err = errors.New("test")
	assert.Equal(t, IsOverflow(err), false)
}
