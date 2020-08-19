package rediscache

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-pg/pg/v9/orm"

	"github.com/Syncano/pkg-go/v2/database"
	"github.com/Syncano/pkg-go/v2/util"
)

func (c *Cache) createModelCacheKey(partition, model, lookup string) string {
	return fmt.Sprintf("%s:%s:m:%d:%s:%x", partition, c.cfg.ServiceKey, c.cfg.CacheVersion, model, util.Hash(lookup))
}

func (c *Cache) createModelVersionCacheKey(partition, model string, pk interface{}) string {
	return fmt.Sprintf("%s:%s:m:%d:%s:%v:version", partition, c.cfg.ServiceKey, c.cfg.CacheVersion, model, pk)
}

func modelPartition(db orm.DB, tableName string) string {
	schema, ok := db.Context().Value(database.KeySchema).(string)
	if !ok {
		schema = "0"
	} else {
		schema = strings.SplitN(schema, "_", 2)[0]
	}

	return schema
}

func (c *Cache) ModelCacheInvalidate(db orm.DB, m interface{}) {
	c.db.AddDBCommitHook(db, func() error {
		table := orm.GetTable(reflect.TypeOf(m).Elem())
		tableName := string(table.FullName)
		partition := c.cfg.ModelPartition(db, tableName)
		versionKey := c.createModelVersionCacheKey(partition, tableName, table.PKs[0].Value(reflect.ValueOf(m).Elem()).Interface())

		return c.InvalidateVersion(versionKey, c.cfg.CacheTimeout)
	})
}

func (c *Cache) ModelCache(db orm.DB, keyModel, val interface{}, lookup string,
	compute func() (interface{}, error), validate func(interface{}) bool) error {
	table := orm.GetTable(reflect.TypeOf(keyModel).Elem())
	n := strings.Split(string(table.FullName), ".")
	tableName := n[len(n)-1]
	partition := c.cfg.ModelPartition(db, tableName)
	modelKey := c.createModelCacheKey(partition, tableName, lookup)

	return c.VersionedCache(modelKey, lookup, val,
		func() string {
			return c.createModelVersionCacheKey(partition, tableName, table.PKs[0].Value(reflect.ValueOf(keyModel).Elem()))
		},
		compute, validate, c.cfg.CacheTimeout)
}

func (c *Cache) SimpleModelCache(db orm.DB, m interface{}, lookup string, compute func() (interface{}, error), opts ...Option) error {
	return c.ModelCache(db, m, m, lookup, compute, nil)
}
