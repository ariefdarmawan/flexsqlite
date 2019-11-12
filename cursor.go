package flexsqlite

import (
	"time"

	"git.eaciitapp.com/sebar/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) Serialize(dest interface{}) error {
	var err error
	m := toolkit.M{}
	toolkit.Serde(dest, &m, "")

	columnNames := c.ColumnNames()
	sqlTypes := c.ColumnTypes()
	//fmt.Println("\n[debug] values:", toolkit.JsonString(c.values))
	//fmt.Println("\n[debug] values Ptr:", toolkit.JsonString(c.valuesPtr))
	for idx, value := range c.Values() {
		name := columnNames[idx]
		ft := sqlTypes[idx]

		switch ft {
		case "int":
			m.Set(name, toolkit.ToInt(value, toolkit.RoundingAuto))

		case "float64":
			m.Set(name, toolkit.ToFloat64(value, 4, toolkit.RoundingAuto))

		case "time.Time":
			if dt, err := time.Parse(time.RFC3339, value.(string)); err == nil {
				m.Set(name, dt)
			} else {
				dt = toolkit.String2Date(value.(string), rdbms.TimeFormat())
				m.Set(name, dt)
			}

		default:
			m.Set(name, value)
		}
	}

	err = toolkit.Serde(m, dest, "")
	if err != nil {
		return toolkit.Error(err.Error() + toolkit.Sprintf(" object: %s", toolkit.JsonString(m)))
	}
	return nil
}
