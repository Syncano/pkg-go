package jobs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"go.uber.org/zap"
)

type Runner struct {
	log  *zap.Logger
	wg   sync.WaitGroup
	rc   *redis.Client
	stop chan struct{}
	cfg  Config

	errorHandlerMu sync.RWMutex
	errorHandler   func(t Runnable, err error)
}

type Config struct {
	ServiceKey string
	Partition  func(t LockableRunnable) string
}

var DefaultConfig = Config{
	ServiceKey: "jobs",
	Partition:  partition,
}

type Option func(*Config)

func WithServiceKey(val string) Option {
	return func(config *Config) {
		config.ServiceKey = val
	}
}
func WithPartition(f func(LockableRunnable) string) Option {
	return func(config *Config) {
		config.Partition = f
	}
}

func New(log *zap.Logger, rc *redis.Client, opts ...Option) *Runner {
	cfg := DefaultConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Runner{
		log:  log,
		rc:   rc,
		stop: make(chan struct{}),
		cfg:  cfg,
	}
}

func (p *Runner) createLockKey(t LockableRunnable) string {
	return fmt.Sprintf("%s:%s:lock:%s", p.cfg.Partition(t), p.cfg.ServiceKey, t.LockKey())
}

func (p *Runner) runPeriodic(job PeriodicRunnable) {
	initialRun := true

	if initialRun {
		p.wg.Add(1)
	}

	lockKey := p.createLockKey(job)
	period := job.RunPeriod()

	go func() {
		ticker := time.NewTicker(job.RunResolution())

		for {
			if !initialRun {
				p.wg.Add(1)
			}

			initialRun = false

			ok, err := p.rc.SetNX(lockKey, 1, period).Result()
			if !ok || err != nil {
				if err != nil {
					p.log.With(zap.Error(err)).Error("Could not obtain redis lock for periodic job")
				}

				p.wg.Done()

				select {
				case <-ticker.C:
					continue
				case <-p.stop:
					return
				}
			}

			isDone, _ := p.processJob(job)

			if !isDone {
				continue
			}

			select {
			case <-ticker.C:
			case <-p.stop:
				return
			}
		}
	}()
}

func (p *Runner) processJob(job Runnable) (bool, error) {
	// Run job with optional timeout.
	timeout := job.RunTimeout()

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}

	p.log.With(zap.Stringer("job", job)).Info("Running job")

	isDone, err := job.Run(ctx)
	if err != nil {
		p.handleJobError(job, err)
	}

	cancel()

	return isDone, err
}

func (p *Runner) Run(job Runnable) {
	if periodic, ok := job.(PeriodicRunnable); ok {
		p.runPeriodic(periodic)

		return
	}

	p.wg.Add(1)

	_, _ = p.processJob(job)
	p.wg.Done()
}

func (p *Runner) Go(f func()) {
	p.wg.Add(1)

	go f()

	p.wg.Done()
}

func (p *Runner) handleJobError(job Runnable, err error) {
	p.errorHandlerMu.RLock()
	defer p.errorHandlerMu.RUnlock()

	if p.errorHandler != nil {
		p.errorHandler(job, err)

		return
	}

	p.log.Error("Job failed", zap.Error(err))
}

func (p *Runner) ErrorHandler(errorHandler func(t Runnable, err error)) {
	p.errorHandlerMu.Lock()
	p.errorHandler = errorHandler
	p.errorHandlerMu.Unlock()
}

func (p *Runner) GracefulStop() {
	close(p.stop)
	p.wg.Wait()
}
