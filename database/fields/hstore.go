package fields

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Hstore struct {
	pgtype.Hstore
}

func NewHstore(val interface{}) Hstore {
	var h pgtype.Hstore

	err := h.Set(val)
	if err != nil {
		panic(err)
	}

	return Hstore{Hstore: h}
}

// Value is used on value in go-pg, pass it to pointer version.
func (h Hstore) Value() (driver.Value, error) {
	return h.Hstore.Value()
}

// IsNull returns true if underlying value is null.
func (h *Hstore) IsNull() bool {
	return h.Status == pgtype.Null
}
