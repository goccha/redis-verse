package guards

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIncrement(t *testing.T) {
	ctx := context.Background()
	counterKey := "test-increment-counter"
	maxCount := 3
	expiration := 3 * time.Second
	counter := &LimitCounter{
		key:        counterKey,
		TryMax:     maxCount,
		Expiration: expiration,
	}

	for i := 0; i <= 6; i++ {
		println(i)
		cnt, err := counter.Increment(ctx)
		if err != nil {
			println(i, cnt, err)
			if i >= maxCount {
				assert.Error(t, err)
			} else {
				t.Fatalf("Increment failed at iteration %d: %v", i, err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i <= 6; i++ {
		println(i)
		cnt, err := counter.Increment(ctx)
		if err != nil {
			println(i, cnt, err)
			if i >= maxCount {
				assert.Error(t, err)
			} else {
				t.Fatalf("Increment failed at iteration %d: %v", i, err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}
