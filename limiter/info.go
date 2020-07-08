package limiter

import "fmt"

type LockInfo struct {
	Capacity int
	Taken    int
	Queued   int
}

func (li *LockInfo) String() string {
	if li.Queued > 0 {
		return fmt.Sprintf("{Cap:%d, Taken:%d, Queued:%d}", li.Capacity, li.Taken, li.Queued)
	}

	return fmt.Sprintf("{Cap:%d, Taken:%d}", li.Capacity, li.Taken)
}
