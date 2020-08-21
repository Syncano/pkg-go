package rediscache

import (
	"errors"
	"reflect"
	"time"

	"github.com/go-pg/pg/v9/orm"
	"github.com/go-redis/cache/v7"
	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/Syncano/pkg-go/v2/database"
	"github.com/Syncano/pkg-go/v2/util"
)

const (
	versionGraceDuration = 5 * time.Minute
)

var (
	ErrNil = errors.New("compute returned nil object and storeNil is false")
)

type Cache struct {
	codec *cache.Codec
	db    *database.DB
	cfg   Config
}

type Config struct {
	ModelPartition    func(db orm.DB, tableName string) string
	FuncPartition     func(funcKey string) string
	LocalCacheTimeout time.Duration
	CacheTimeout      time.Duration
	CacheVersion      int
	ServiceKey        string
	StoreNil          bool
}

var DefaultConfig = Config{
	ModelPartition:    modelPartition,
	FuncPartition:     funcPartition,
	CacheVersion:      1,
	CacheTimeout:      12 * time.Hour,
	LocalCacheTimeout: 1 * time.Hour,
	ServiceKey:        "cache",
	StoreNil:          false,
}

type Option func(*Config)

func WithTimeout(local, global time.Duration) Option {
	return func(config *Config) {
		config.LocalCacheTimeout = local
		config.CacheTimeout = global
	}
}

func WithVersion(val int) Option {
	return func(config *Config) {
		config.CacheVersion = val
	}
}

func WithServiceKey(val string) Option {
	return func(config *Config) {
		config.ServiceKey = val
	}
}

func WithStoreNil(storeNil bool) Option {
	return func(config *Config) {
		config.StoreNil = storeNil
	}
}

func WithModelPartition(f func(db orm.DB, tableName string) string) Option {
	return func(config *Config) {
		config.ModelPartition = f
	}
}

func WithFuncPartition(f func(funcKey string) string) Option {
	return func(config *Config) {
		config.FuncPartition = f
	}
}

// Init sets up a cache.
func New(r rediser, db *database.DB, opts ...Option) *Cache {
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

	if object == nil && !c.cfg.StoreNil {
		return ErrNil
	}

	if version == "" {
		version, err = c.codec.Redis.Get(versionKeyFunc()).Result()
		if err != nil && err != redis.Nil {
			return err
		}
	}

	// Set object through reflect.
	oref := reflect.ValueOf(object)
	vref := reflect.ValueOf(val)

	if oref.IsValid() && oref.Kind() == reflect.Ptr && !oref.IsNil() {
		oref = oref.Elem()
	}

	if !oref.IsValid() || (oref.Kind() == reflect.Ptr && oref.IsNil()) {
		vref.Elem().Set(reflect.Zero(vref.Elem().Type()))
	} else {
		vref.Elem().Set(oref)
	}

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
