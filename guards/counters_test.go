package guards

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/goccha/redis-verse/locks"
	"github.com/goccha/redis-verse/redis"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := redis.Setup(ctx, &redis.EnvBuilder{}); err != nil {
		panic(err)
	}
	if err := redis.WaitForActivation(ctx); err != nil {
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func Test_UserLock(t *testing.T) {
	ctx := context.Background()
	ip := "127.0.0.1"
	ipLock := &locks.RedisLock{
		Key:      ip,
		Duration: 5 * time.Minute,
	}
	userId := "test_user_000"
	userLock := &locks.RedisLock{
		Key:      userId,
		Duration: 3 * time.Minute,
	}

	for i := 1; i <= 5; i++ {
		fired := false
		ipCounter := TimedCounter(ip, 10, 10*time.Minute, &LockTrigger{Lock: ipLock})
		userCounter := TimedCounter(userId, 5, 5*time.Minute, &LockTrigger{Lock: userLock})
		locked, err := ipCounter.Increment(ctx)
		assert.Nil(t, err)
		assert.False(t, locked)

		locked, err = userCounter.Increment(ctx)
		assert.Nil(t, err)
		assert.False(t, locked)

		locked, err = userCounter.Fire(ctx, func() {
			fired = true
		})
		assert.Nil(t, err)
		if i == 5 {
			assert.True(t, locked)
			assert.True(t, fired)
			cmd := redis.Reader().Get(ctx, userLock.String())
			assert.Nil(t, cmd.Err())
			is, err := cmd.Bool()
			assert.Nil(t, err)
			assert.True(t, is)
			_ = redis.Primary().Del(ctx, userLock.String())
		} else {
			assert.False(t, locked)
			assert.False(t, fired)
		}
		fired = false
		locked, err = ipCounter.Fire(ctx, func() {
			fired = true
		})
		assert.Nil(t, err)
		assert.False(t, locked)
		assert.False(t, fired)
		if i == 5 {
			cmd := redis.Reader().Get(ctx, ipLock.String())
			assert.NotNil(t, cmd.Err())
			assert.True(t, redis.IsNil(cmd.Err()))
			_ = redis.Primary().Del(ctx, ipCounter.Key())
		}
	}
}

func Test_IpLock(t *testing.T) {
	ctx := context.Background()
	ip := "127.0.0.1"
	ipLock := &locks.RedisLock{
		Key:      ip,
		Duration: 5 * time.Minute,
	}
	users := []string{
		"test_user_001",
		"test_user_002",
		"test_user_003",
		"test_user_004",
		"test_user_005",
		"test_user_006",
		"test_user_007",
		"test_user_008",
		"test_user_009",
		"test_user_010",
		"test_user_011",
		"test_user_012",
	}

	for i := 1; i <= 12; i++ {
		userId := users[i-1]
		userLock := &locks.RedisLock{
			Key:      userId,
			Duration: 1 * time.Minute,
		}
		fired := false
		ipCounter := TimedCounter(ip, 10, 10*time.Minute, &LockTrigger{Lock: ipLock})
		userCounter := TimedCounter(userId, 5, 5*time.Minute, &LockTrigger{Lock: userLock})
		locked, err := ipCounter.Increment(ctx)
		assert.Nil(t, err)
		if i > 10 {
			assert.True(t, locked)
		} else {
			assert.False(t, locked)
		}
		locked, err = userCounter.Increment(ctx)
		assert.Nil(t, err)
		assert.False(t, locked)

		locked, err = userCounter.Fire(ctx, func() {
			fired = true
		})
		assert.Nil(t, err)
		assert.False(t, locked)
		assert.False(t, fired)

		fired = false
		locked, err = ipCounter.Fire(ctx, func() {
			fired = true
		})
		assert.Nil(t, err)
		if i == 10 {
			assert.True(t, locked)
			assert.True(t, fired)
			cmd := redis.Reader().Get(ctx, ipLock.String())
			assert.Nil(t, cmd.Err())
			is, err := cmd.Bool()
			assert.Nil(t, err)
			assert.True(t, is)
		} else if i > 10 {
			assert.True(t, locked)
			assert.False(t, fired)
		} else {
			assert.False(t, locked)
			assert.False(t, fired)
		}
	}
	_ = redis.Primary().Del(ctx, ipLock.String())
}
