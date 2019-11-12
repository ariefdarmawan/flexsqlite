package flexsqlite

import (
	"database/sql"
	"fmt"

	"git.eaciitapp.com/sebar/dbflex"

	"git.eaciitapp.com/sebar/dbflex/drivers/rdbms"
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
