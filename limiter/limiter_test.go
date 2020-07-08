package limiter

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLimiter(t *testing.T) {
	Convey("Given limiter", t, func() {
		l := New(&Options{Queue: 1})

		Convey("Lock returns error when limit is <= 0", func() {
			_, err := l.Lock(context.Background(), "key", 0)
			So(err, ShouldEqual, ErrMaxQueueSizeReached)
		})
		Convey("Lock is a semaphore-like lock", func() {
			_, err := l.Lock(context.Background(), "key", 2)
			So(err, ShouldBeNil)
			_, err = l.Lock(context.Background(), "key", 2)
			So(err, ShouldBeNil)
			l.Unlock("key", 2)
			l.Unlock("key", 2)
			_, err = l.Lock(context.Background(), "key", 2)
			So(err, ShouldBeNil)
		})
		Convey("Lock respects context", func() {
			ctx, cancel := context.WithCancel(context.Background())
			l.Lock(ctx, "key", 1)
			cancel()

			_, err := l.Lock(ctx, "key", 1)
			So(err, ShouldEqual, context.Canceled)
		})
		Convey("Lock can safely overflow", func() {
			_, err := l.Lock(context.Background(), "key", 1)
			So(err, ShouldBeNil)
			ch := make(chan time.Time, 1)

			go func() {
				l.Lock(context.Background(), "key", 1)
				ch <- time.Now()
			}()
			time.Sleep(50 * time.Millisecond)

			// Lock has overflown and should return error.
			_, err = l.Lock(context.Background(), "key", 1)
			So(err, ShouldEqual, ErrMaxQueueSizeReached)
			now := time.Now()

			// Now Lock that is waiting in goroutine should succeed.
			l.Unlock("key", 1)
			So((<-ch).After(now), ShouldBeTrue)

			// Try again to lock.
			l.Unlock("key", 1)
			_, err = l.Lock(context.Background(), "key", 1)
			So(err, ShouldBeNil)
		})
		Convey("Unlock silently quits for non existing keys", func() {
			l.Unlock("key", 2)
		})
		Convey("createLock returns existing channel if one was created in the mean time", func() {
			c1 := &lockData{ch: make(chan struct{}, 1)}
			l.channels.Set("key", c1)
			c2 := l.createLock("key", 2)
			So(c1, ShouldEqual, c2)
		})
		l.Shutdown()
	})
}
