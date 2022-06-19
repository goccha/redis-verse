package guards

import (
	"context"
	"github.com/goccha/redis-verse/locks"
)

type LockTrigger struct {
	Lock locks.Lock
}

func (t *LockTrigger) Fire(ctx context.Context) error {
	if ok, err := t.Lock.Lock(ctx); err != nil {
		return err
	} else if !ok {
		return &locks.Failure{}
	}
	return nil
}
func (t *LockTrigger) Fired(ctx context.Context) (bool, error) {
	return t.Lock.Locked(ctx)
}
func (t *LockTrigger) Reset(ctx context.Context) {
	t.Lock.UnLock(ctx)
}
