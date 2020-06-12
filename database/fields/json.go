package fields

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/jackc/pgtype"
)

type JSON struct {
	pgtype.JSON
	Data interface{}
}

// Value implements the database/sql/driver Valuer interface.
func (j JSON) Value() (driver.Value, error) {
	if j.Data != nil {
		b, e := json.Marshal(j.Data)
		return string(b), e
	}

	return j.JSON.Value()
}

func (j *JSON) Get() interface{} {
	if j.Data == nil && !j.IsNull() {
		j.Data = j.JSON.Get()
	}

	return j.Data
}

// Scan implements the database/sql Scanner interface.
func (j *JSON) Scan(src interface{}) error {
	err := j.JSON.Scan(src)
	j.Data = nil

	return err
}

// IsNull returns true if underlying value is null.
func (j *JSON) IsNull() bool {
	return j.JSON.Status == pgtype.Null
}

func (j *JSON) MarshalJSON() ([]byte, error) {
	if j.Data != nil {
		return json.Marshal(j.Data)
	}

	return j.Bytes, nil
}
