package fields

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Hstore struct {
	pgtype.Hstore
}

func NewHstore() Hstore {
	return Hstore{Hstore: pgtype.Hstore{Map: make(map[string]pgtype.Text), Status: pgtype.Present}}
}

// Value is used on value in go-pg, pass it to pointer version.
func (h Hstore) Value() (driver.Value, error) {
	return h.Hstore.Value()
}

// IsNull returns true if underlying value is null.
func (h *Hstore) IsNull() bool {
	return h.Status == pgtype.Null
}
