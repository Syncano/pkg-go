package rediscache

import (
	"fmt"

	"github.com/go-pg/pg/v9/orm"

	"github.com/Syncano/pkg-go/v2/util"
)

func (c *Cache) createFuncCacheKey(partition, funcKey, versionKey, lookup string) string {
	return fmt.Sprintf("%s:%s:f:%d:%s:%s:%x", partition, c.cfg.ServiceKey, c.cfg.CacheVersion, funcKey, versionKey, util.Hash(lookup))
}

func (c *Cache) createFuncVersionCacheKey(partition, funcKey, versionKey string) string {
	return fmt.Sprintf("%s:%s:f:%d:%s:%s:version", partition, c.cfg.ServiceKey, c.cfg.CacheVersion, funcKey, versionKey)
}

func funcPartition(funcKey string) string {
	return "0"
}

func (c *Cache) FuncCacheInvalidate(funcKey, versionKey string) error {
	partition := c.cfg.FuncPartition(funcKey)
	versionKey = c.createFuncVersionCacheKey(partition, funcKey, versionKey)

	return c.InvalidateVersion(versionKey, c.cfg.CacheTimeout)
}

func (c *Cache) FuncCacheCommitInvalidate(db orm.DB, funcKey, versionKey string) {
	c.db.AddDBCommitHook(db, func() error {
		return c.FuncCacheInvalidate(funcKey, versionKey)
	})
}

// FuncCache is a generic cache function that can be used with any value that has a compute function provided.
//
//   funcKey - unique function key e.g. `"Trigger.Match"`.
//   lookup - used as a part of CACHE KEY to separate different funcKeys from each other.
//   versionKey - used as a part of CACHE KEY and VERSION KEY. This way invalidating version, invalidates cache key as well. E.g. `"i=1;id=1"`.
//   val - pointer to be populated.
//   compute - function that computes the value when key is not found in cache.
//   validate - optional function that validates value from cache.
func (c *Cache) FuncCache(funcKey, lookup, versionKey string, val interface{},
	compute func() (interface{}, error), validate func(interface{}) bool, opts ...Option) error {
	partition := c.cfg.FuncPartition(funcKey)
	funcKey = c.createFuncCacheKey(partition, funcKey, versionKey, lookup)

	return c.VersionedCache(funcKey, lookup, val,
		func() string {
			return c.createFuncVersionCacheKey(partition, funcKey, versionKey)
		},
		compute, validate, c.cfg.CacheTimeout)
}

// SimpleFuncCache is a proxy for FuncCache with validate step omitted.
func (c *Cache) SimpleFuncCache(funcKey, lookup, versionKey string, val interface{},
	compute func() (interface{}, error), opts ...Option) error {
	return c.FuncCache(funcKey, versionKey, lookup, val, compute, nil)
}
