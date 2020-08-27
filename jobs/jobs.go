package jobs

import (
	"context"
	"fmt"
	"time"
)

type OneOffJob struct {
	Name    string
	Func    func(ctx context.Context) error
	Timeout time.Duration // Passed as context.WithTimeout to Func.
}

func (j *OneOffJob) Run(ctx context.Context) (bool, error) {
	return true, j.Func(ctx)
}

func (j *OneOffJob) RunTimeout() time.Duration {
	return j.Timeout
}

func (j *OneOffJob) String() string {
	return fmt.Sprintf("OneOffJob<Name=%s>", j.Name)
}

var _ Runnable = (*OneOffJob)(nil)

type PeriodicJob struct {
	Func       func(ctx context.Context) (isDone bool, err error)
	Timeout    time.Duration // Passed as context.WithTimeout to Func.
	Period     time.Duration
	Resolution time.Duration // Time between attempts to grab a lock, defaults to max(min(1/4 of Perion, 1 Hour), 5 Min)
	Name       string
}

var _ PeriodicRunnable = (*PeriodicJob)(nil)

func (j *PeriodicJob) Run(ctx context.Context) (bool, error) {
	return j.Func(ctx)
}

func (j *PeriodicJob) RunTimeout() time.Duration {
	return j.Timeout
}

const (
	minDefaultResolution = 5 * time.Minute
	maxDefaultResolution = 1 * time.Hour
)

func DefaultResolution(period time.Duration) time.Duration {
	res := period / 4

	if res < minDefaultResolution {
		return minDefaultResolution
	}

	if res > maxDefaultResolution {
		return maxDefaultResolution
	}

	return res
}

func (j *PeriodicJob) RunResolution() time.Duration {
	if j.Resolution == 0 {
		return DefaultResolution(j.Period)
	}

	return j.Resolution
}

func (j *PeriodicJob) RunPeriod() time.Duration {
	return j.Period
}

func (j *PeriodicJob) LockKey() string {
	return j.Name
}

func (j *PeriodicJob) String() string {
	return fmt.Sprintf("PeriodicJob<Name=%s>", j.Name)
}

func partition(t LockableRunnable) string {
	return "0"
}
