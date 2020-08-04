package rediscache

import (
	"reflect"
	"time"

	"github.com/go-redis/cache/v7"
	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/Syncano/pkg-go/v2/database"
	"github.com/Syncano/pkg-go/v2/util"
)

const (
	versionGraceDuration = 5 * time.Minute
)

type Cache struct {
	codec *cache.Codec
	db    *database.DB
	cfg   Config
}

type Config struct {
	LocalCacheTimeout time.Duration
	CacheTimeout      time.Duration
	CacheVersion      int
	ServiceKey        string
}

var DefaultConfig = Config{
	CacheVersion:      1,
	CacheTimeout:      12 * time.Hour,
	LocalCacheTimeout: 1 * time.Hour,
	ServiceKey:        "cache",
}

func WithTimeout(local, global time.Duration) func(*Config) {
	return func(config *Config) {
		config.LocalCacheTimeout = local
		config.CacheTimeout = global
	}
}

func WithVersion(val int) func(*Config) {
	return func(config *Config) {
		config.CacheVersion = val
	}
}

func WithServiceKey(val string) func(*Config) {
	return func(config *Config) {
		config.ServiceKey = val
	}
}

// Init sets up a cache.
func New(r rediser, db *database.DB, opts ...func(*Config)) *Cache {
	cfg := DefaultConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	codec := &cache.Codec{
		Redis: r,

		Marshal:   msgpack.Marshal,
		Unmarshal: msgpack.Unmarshal,
	}
	codec.UseLocalCache(50000, cfg.LocalCacheTimeout)

	return &Cache{
		codec: codec,
		db:    db,
		cfg:   cfg,
	}
}

// Codec returns cache client.
func (c *Cache) Codec() *cache.Codec {
	return c.codec
}

// Stats returns cache statistics.
func (c *Cache) Stats() *cache.Stats {
	return c.codec.Stats()
}

type cacheItem struct {
	Object  interface{}
	Version string
}

func (ci *cacheItem) validate(version string, validate func(interface{}) bool) bool {
	return version == ci.Version && (validate == nil || validate(ci.Object))
}

func (c *Cache) VersionedCache(cacheKey, lookup string, val interface{},
	versionKeyFunc func() string, compute func() (interface{}, error), validate func(interface{}) bool, expiration time.Duration) error {
	item := &cacheItem{Object: val}

	var (
		version string
		err     error
	)

	// Get object and check version. First local and fallback to global cache.
	if c.codec.Get(cacheKey, item) == nil {
		if version == "" {
			version, err = c.codec.Redis.Get(versionKeyFunc()).Result()
			if err != nil && err != redis.Nil {
				return err
			}
		}

		if item.validate(version, validate) {
			return nil
		}
	}

	// Compute and save object.
	object, err := compute()
	if err != nil {
		return err
	}

	if version == "" {
		version, err = c.codec.Redis.Get(versionKeyFunc()).Result()
		if err != nil && err != redis.Nil {
			return err
		}
	}

	// Set object through reflect.
	vref := reflect.ValueOf(val)
	oref := reflect.ValueOf(object)

	if oref.Kind() == reflect.Ptr {
		oref = oref.Elem()
	}

	vref.Elem().Set(oref)

	item.Object = val
	item.Version = version

	// Set cache values.
	return c.codec.Set(&cache.Item{
		Key:        cacheKey,
		Object:     item,
		Expiration: expiration,
	})
}

func (c *Cache) InvalidateVersion(versionKey string, expiration time.Duration) error {
	return c.codec.Redis.Set(
		versionKey,
		util.GenerateRandomString(4),
		expiration+versionGraceDuration, // Add grace period to avoid race condition.
	).Err()
}
