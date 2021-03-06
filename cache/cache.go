package cache

import (
	"container/list"
	"sync"
	"time"
)

// Cache is an abstract raw struct used for LIFO-like LRU cache with ttl.
// As it provides no value access interface, it should be extended depending on what's needed.
// All exported methods should be thread-safe and use internal mutex.
// See LRUCache for example implementation.
type Cache struct {
	cfg Config

	// Maintain both valueMap and valuesList in sync.
	// valueMap is used as a storage for key->list
	// valuesList is used to keep it in TTL order so that we can expire them in same order.
	mu         sync.RWMutex
	valueMap   map[string]interface{}
	valuesList *list.List
	janitor    *janitor

	muHandler      sync.RWMutex
	onValueEvicted EvictionHandler

	deleteHandler DeleteHandler
}

// Config holds settable config for cache.
type Config struct {
	TTL             time.Duration
	CleanupInterval time.Duration
	Capacity        int
}

var DefaultConfig = Config{
	TTL:             30 * time.Second,
	CleanupInterval: 15 * time.Second,
}

type Option func(*Config)

func WithTTL(ttl time.Duration) Option {
	return func(config *Config) {
		config.TTL = ttl
	}
}

func WithCleanupInterval(val time.Duration) Option {
	return func(config *Config) {
		config.CleanupInterval = val
	}
}

func WithCapacity(c int) Option {
	return func(config *Config) {
		config.Capacity = c
	}
}

// Item is used as a single element in cache.
type Item struct {
	object            interface{}
	expiration        int64
	ttl               time.Duration
	valuesListElement *list.Element
}

type valuesItem struct {
	key  string
	item *Item
}

type keyValue struct {
	key   string
	value interface{}
}

// DeleteHandler is a function that should be provided in an implementation embedding Cache struct.
// It is especially useful when dealing with LIFO/FIFO stack-like objects.
// For standard KV storage there is a defaultDeleteHandler that should be enough.
type DeleteHandler func(*valuesItem) *keyValue

// EvictionHandler is a callback function called whenever eviction happens.
type EvictionHandler func(string, interface{})

// Init initializes cache struct fields and starts janitor process.
func (c *Cache) Init(deleteHandler DeleteHandler, opts ...Option) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.janitor != nil {
		panic("init on cache cannot be called twice")
	}

	cfg := DefaultConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	c.cfg = cfg

	if deleteHandler == nil {
		deleteHandler = c.defaultDeleteHandler
	}

	c.deleteHandler = deleteHandler
	c.valueMap = make(map[string]interface{})
	c.valuesList = list.New()
	c.janitor = &janitor{
		interval: cfg.CleanupInterval,
		stop:     make(chan struct{}),
	}

	go c.janitor.Run(c)
}

// Config returns a copy of config struct.
func (c *Cache) Config() Config {
	return c.cfg
}

// StopJanitor is meant to be called when cache is no longer needed to avoid leaking goroutine.
func (c *Cache) StopJanitor() {
	c.mu.Lock()
	if c.janitor != nil {
		close(c.janitor.stop)
		c.janitor = nil
	}
	c.mu.Unlock()
}

// Len returns cache length.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.valuesList.Len()
}

// OnValueEvicted sets an (optional) function that is called with the key and value when value is evicted from the cache.
// Set to nil to disable.
func (c *Cache) OnValueEvicted(f EvictionHandler) {
	c.muHandler.Lock()
	c.onValueEvicted = f
	c.muHandler.Unlock()
}

// DeleteOne deletes one element that is closest to expiring. Returns true if list was not empty.
// Calls onValueEvicted.
func (c *Cache) DeleteLRU() bool {
	c.mu.Lock()

	evicted := c.deleteLRU()

	c.mu.Unlock()

	if evicted != nil {
		c.handleEviction(evicted)

		return true
	}

	return false
}

func (c *Cache) deleteLRU() *keyValue {
	e := c.valuesList.Front()
	if e == nil {
		return nil
	}

	return c.delete(e)
}

// Flush deletes all elements in cache.
func (c *Cache) Flush() {
	c.DeleteByTime(0)
}

// DeleteExpired deletes items by checking their expiration against current time. Calls onValueEvicted.
func (c *Cache) DeleteExpired() {
	c.DeleteByTime(time.Now().UnixNano())
}

// DeleteByTime deletes items by checking their expiration against passed now value. If now == 0, deletes all items.
// Calls onValueEvicted.
func (c *Cache) DeleteByTime(now int64) {
	var evictedValues []*keyValue

	c.mu.Lock()

	// Iterate through valuesList (list of valuesListItem) and delete underlying cacheItem if it has expired.
	for e := c.valuesList.Front(); e != nil; {
		nextE := e.Next()

		valueEvicted := c.deleteValue(e, now)
		if valueEvicted == nil {
			break
		}

		e = nextE

		evictedValues = append(evictedValues, valueEvicted)
	}

	c.mu.Unlock()

	c.handleEviction(evictedValues...)
}

func (c *Cache) defaultDeleteHandler(item *valuesItem) *keyValue {
	delete(c.valueMap, item.key)
	return &keyValue{key: item.key, value: item.item.object}
}

func (c *Cache) delete(e *list.Element) *keyValue {
	return c.deleteValue(e, 0)
}

func (c *Cache) handleEviction(evictedValues ...*keyValue) {
	c.muHandler.RLock()

	if c.onValueEvicted != nil {
		for _, v := range evictedValues {
			c.onValueEvicted(v.key, v.value)
		}
	}

	c.muHandler.RUnlock()
}

// deleteValue deletes exactly one element from valuesList. If now != 0, it is checked against expiration time.
// Note: doesn't call onValueEvicted. Returns evicted keyValue.
func (c *Cache) deleteValue(e *list.Element, now int64) (valueEvicted *keyValue) {
	item := e.Value.(*valuesItem)

	if now != 0 && item.item.expiration > now {
		return
	}

	if _, ok := c.valueMap[item.key]; ok {
		valueEvicted = c.deleteHandler(item)
	}

	c.valuesList.Remove(e)

	return
}

func (c *Cache) checkLength() {
	// If we are over the capacity, delete one closest to expiring.
	if c.cfg.Capacity > 0 {
		for c.valuesList.Len() > c.cfg.Capacity {
			c.deleteLRU()
		}
	}
}

func (c *Cache) add(vi *valuesItem) *list.Element {
	exp := vi.item.expiration

	var at *list.Element

	for at = c.valuesList.Front(); at != nil; {
		if at.Value.(*valuesItem).item.expiration > exp {
			break
		}

		at = at.Next()
	}

	if at != nil {
		return c.valuesList.InsertBefore(vi, at)
	}

	return c.valuesList.PushBack(vi)
}

func (c *Cache) sortMove(ele *list.Element) {
	exp := ele.Value.(*valuesItem).item.expiration

	var at *list.Element

	for at = c.valuesList.Front(); at != nil; {
		if at.Value.(*valuesItem).item.expiration > exp {
			break
		}

		at = at.Next()
	}

	if at != nil {
		c.valuesList.MoveBefore(ele, at)
	}

	c.valuesList.MoveToBack(ele)
}

type janitor struct {
	interval time.Duration
	stop     chan struct{}
}

func (j *janitor) Run(cache *Cache) {
	ticker := time.NewTicker(j.interval)

	for {
		select {
		case <-ticker.C:
			cache.DeleteExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}
