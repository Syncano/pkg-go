package jobs

import (
	"context"
	"fmt"
	"time"
)

type Runnable interface {
	fmt.Stringer
	Run(ctx context.Context) (isDone bool, err error)
	RunTimeout() time.Duration
}

type LockableRunnable interface {
	Runnable
	LockKey() string
}

type PeriodicRunnable interface {
	LockableRunnable
	RunPeriod() time.Duration
	RunResolution() time.Duration
}
