package flexsqlite

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"git.eaciitapp.com/sebar/dbflex"

	"git.eaciitapp.com/sebar/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
	_ "github.com/mattn/go-sqlite3"
)

// Connection implementation of dbflex.IConnection
type Connection struct {
	rdbms.Connection
	db *sql.DB
}

func init() {
	// sqlite3://username:password@localhost?file=data.db
	dbflex.RegisterDriver("sqlite3", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.SetThis(c)
		c.ServerInfo = *si
		return c
	})
}

// Connect to database instance
func (c *Connection) Connect() error {
	dbpath := ""
	for k, v := range c.Config {
		if k == "file" {
			dbpath = v.(string)
		}
	}
	if dbpath == "" {
		return fmt.Errorf("file to dbpath is not defined")
	}
	db, err := sql.Open("sqlite3", dbpath)
	c.db = db
	return err
}

func (c *Connection) State() string {
	if c.db != nil {
		return dbflex.StateConnected
	}
	return dbflex.StateUnknown
}

// Close database connection
func (c *Connection) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

// NewQuery generates new query object to perform query action
func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.db = c.db
	return q
}

// DropTable - delete table
func (c *Connection) DropTable(name string) error {
	_, err := c.db.Exec("drop table if exists " + name)
	return err
}

// EnsureTable ensure existence and structures of the table
func (c *Connection) EnsureTable(name string, keys []string, obj interface{}) error {
	cmd := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' and name ='%s'", name)
	rs, err := c.db.Query(cmd)
	if err != nil {
		return fmt.Errorf("unable to check table existence. %s", err.Error())
	}
	defer rs.Close()

	tableExists := false
	for rs.Next() {
		tbname := ""
		rs.Scan(&tbname)
		tableExists = tbname == name
		break
	}

	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	if !tableExists {
		//-- create table
		cmd = "CREATE TABLE %s (\n%s\n)"

		fieldNum := t.NumField()
		fields := make([]string, fieldNum)
		idx := 0
		for idx < fieldNum {
			ft := t.Field(idx)
			dataType := "TEXT"
			ftName := strings.ToLower(ft.Type.Name())
			if strings.HasPrefix(ftName, "int") {
				dataType = "INT"
			} else if strings.HasPrefix(ftName, "float") {
				dataType = "REAL"
			} else if strings.HasPrefix(ftName, "time") {
				dataType = "DATETIME"
			}
			ftxt := fmt.Sprintf("%s %s", ft.Name, dataType)
			if toolkit.HasMember(keys, ft.Name) {
				ftxt = ftxt + " NOT NULL PRIMARY KEY"
			}
			fields[idx] = ftxt
			idx++
		}
		cmd = fmt.Sprintf(cmd, name, strings.Join(fields, ",\n"))
		//fmt.Println("command:\n", cmd)
		_, err = c.db.Exec(cmd)
		if err != nil {
			return fmt.Errorf("unable to created table %s. %s", name, err.Error())
		}
	} else {
		//fmt.Println("table", name, "is exist")
		return fmt.Errorf("table %s already exist. Please do manual DDL change", name)
	}
	return nil
}
