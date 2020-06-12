package querymgr

import (
	"github.com/go-pg/pg/v9"

	"github.com/Syncano/pkg-go/database"
)

const ContextSchemaKey = "schema"

// DB returns base db for context.
func DB(db Databaser, c database.DBContext) *pg.DB {
	return db.DB()
}

// TenantDB returns base tenant db for context.
func TenantDB(db Databaser, c database.DBContext) *pg.DB {
	schema := c.Get(ContextSchemaKey).(string)
	return db.TenantDB(schema)
}
