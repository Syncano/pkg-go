package fields

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgtype"
)

var (
	jsonNull       = []byte("null")
	DateTimeFormat = "2006-01-02T15:04:05.000000Z"
)

type Time struct {
	pgtype.Timestamptz
}

func NewTime(val interface{}) Time {
	var t pgtype.Timestamptz

	err := t.Set(val)
	if err != nil {
		panic(err)
	}

	return Time{Timestamptz: t}
}

// Value is used on value in go-pg, pass it to pointer version.
func (t Time) Value() (driver.Value, error) {
	return t.Timestamptz.Value()
}

func (t *Time) String() string {
	return t.Time.UTC().Format(DateTimeFormat)
}

// IsNull returns true if underlying value is null.
func (t *Time) IsNull() bool {
	return t.Status == pgtype.Null
}

func (t *Time) MarshalJSON() ([]byte, error) {
	if t.IsNull() {
		return jsonNull, nil
	}

	return []byte(fmt.Sprintf("\"%s\"", t.String())), nil
}
