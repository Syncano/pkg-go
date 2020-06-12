package querymgr

import (
	"github.com/go-pg/pg/v9"

	"github.com/Syncano/pkg-go/database"
)

//go:generate go run github.com/vektra/mockery/cmd/mockery -name Databaser
type Databaser interface {
	DB() *pg.DB
	TenantDB(schema string) *pg.DB
}

var _ Databaser = (*database.DB)(nil)
