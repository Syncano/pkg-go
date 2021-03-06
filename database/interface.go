package database

import (
	"context"

	"github.com/go-pg/pg/v9"
)

//go:generate go run github.com/vektra/mockery/cmd/mockery -name DBContext
type DBContext interface {
	Context() context.Context
	Unwrap() interface{}
	Schema() string
}

//go:generate go run github.com/vektra/mockery/cmd/mockery -name Databaser
type Databaser interface {
	DB() *pg.DB
	TenantDB(schema string) *pg.DB
}

var _ Databaser = (*DB)(nil)
