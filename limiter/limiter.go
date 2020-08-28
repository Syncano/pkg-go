package limiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Syncano/pkg-go/v2/cache"
)

// Limiter allows semaphore rate limiting functionality.
type Limiter struct {
	mu       sync.Mutex
	channels *cache.LRUCache // string->channel

	cfg Config
}

// Config holds settable config for limiter.
type Config struct {
	Queue int
	TTL   time.Duration
}

type Option func(*Config)

// DefaultConfig holds default options values for limiter.
var DefaultConfig = Config{
	Queue: 100,
	TTL:   10 * time.Minute,
}

func WithQueue(size int) Option {
	return func(config *Config) {
		config.Queue = size
	}
}

func WithTTL(ttl time.Duration) Option {
	return func(config *Config) {
		config.TTL = ttl
	}
}

type lockData struct {
	ch    chan struct{}
	queue int32
}

const lockTemplate = "%s:%d"

var (
	// ErrMaxQueueSizeReached signals that queue has overflown.
	ErrMaxQueueSizeReached = errors.New("max queue size reached")
)

// New initializes new limiter.
func New(opts ...Option) *Limiter {
	cfg := DefaultConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	channels := cache.NewLRUCache(true, cache.WithTTL(cfg.TTL))
	l := &Limiter{
		cfg:      cfg,
		channels: channels,
	}

	return l
}

func (l *Limiter) createLock(key string, limit int) (lock *lockData) {
	l.mu.Lock()

	v := l.channels.Get(key)
	if v == nil {
		ch := make(chan struct{}, limit)
		lock = &lockData{ch: ch}

		l.channels.Set(key, lock)
	} else {
		lock = v.(*lockData)
	}

	l.mu.Unlock()

	return
}

// Lock tries to get a lock on a semaphore on key with limit.
func (l *Limiter) Lock(ctx context.Context, key string, limit int) (*LockInfo, error) {
	if limit <= 0 {
		return nil, ErrMaxQueueSizeReached
	}

	var lock *lockData

	key = fmt.Sprintf(lockTemplate, key, limit)

	v := l.channels.Get(key)
	if v == nil {
		lock = l.createLock(key, limit)
	} else {
		lock = v.(*lockData)
	}

	defer atomic.AddInt32(&lock.queue, -1)

	if int(atomic.AddInt32(&lock.queue, 1)) > l.cfg.Queue {
		return nil, ErrMaxQueueSizeReached
	}

	select {
	case lock.ch <- struct{}{}:
		return &LockInfo{Capacity: limit, Taken: len(lock.ch)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Limiter) Info(key string) *LockInfo {
	v := l.channels.Get(key)
	if v == nil {
		return nil
	}

	lock := v.(*lockData)

	return &LockInfo{
		Capacity: cap(lock.ch),
		Taken:    len(lock.ch),
		Queued:   int(atomic.LoadInt32(&lock.queue)),
	}
}

// Unlock returns lock to semaphore pool.
func (l *Limiter) Unlock(key string, limit int) {
	key = fmt.Sprintf(lockTemplate, key, limit)

	v := l.channels.Get(key)
	if v == nil {
		return
	}

	<-v.(*lockData).ch
}

// Shutdown stops everything.
func (l *Limiter) Shutdown() {
	// Stop cache janitor.
	l.channels.StopJanitor()
}
