package fields

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgtype"
)

type Date struct {
	pgtype.Date
}

// Value is used on value in go-pg, pass it to pointer version.
func (d Date) Value() (driver.Value, error) {
	return d.Date.Value()
}

// IsNull returns true if underlying value is null.
func (d *Date) IsNull() bool {
	return d.Status == pgtype.Null
}

func (d *Date) String() string {
	return d.Time.UTC().Format(DateTimeFormat)
}

func (d *Date) MarshalJSON() ([]byte, error) {
	if d.IsNull() {
		return jsonNull, nil
	}

	return []byte(fmt.Sprintf("\"%s\"", d.String())), nil
}

type Daterange struct {
	pgtype.Daterange
}

// IsNull returns true if underlying value is null.
func (r *Daterange) IsNull() bool {
	return r.Status == pgtype.Null
}
