package fields //nolint: dupl

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/jackc/pgtype"
)

type JSONB struct {
	pgtype.JSONB
	Data interface{}
}

func NewJSONB(val interface{}) JSONB {
	var j pgtype.JSONB

	err := j.Set(val)
	if err != nil {
		panic(err)
	}

	return JSONB{JSONB: j}
}

// Value implements the database/sql/driver Valuer interface.
func (j JSONB) Value() (driver.Value, error) {
	if j.Data != nil {
		b, e := json.Marshal(j.Data)
		return string(b), e
	}

	v, e := j.JSONB.Value()
	if e != nil {
		return v, e
	}

	return string(v.([]byte)), e
}

func (j *JSONB) Get() interface{} {
	if j.Data == nil && !j.IsNull() {
		j.Data = j.JSONB.Get()
	}

	return j.Data
}

// Scan implements the database/sql Scanner interface.
func (j *JSONB) Scan(src interface{}) error {
	err := j.JSONB.Scan(src)
	j.Data = nil

	return err
}

// IsNull returns true if underlying value is null.
func (j *JSONB) IsNull() bool {
	return j.JSONB.Status == pgtype.Null
}

func (j *JSONB) MarshalJSON() ([]byte, error) {
	if j.Data != nil {
		return json.Marshal(j.Data)
	}

	return j.Bytes, nil
}
