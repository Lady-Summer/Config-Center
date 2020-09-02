package resource

import (
	"database/sql"
	"log"
)

type PostgresDataBase struct {
	db *sql.DB
}

func prepare(db *sql.DB, query string) *sql.Stmt {
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println("Error exists when prepare Query. The Query is: ", query)
		return nil
	}
	return stmt
}

func (pq PostgresDataBase) Init(db *sql.DB) {
	pq.db = db
}

func (pq PostgresDataBase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	// TODO pre-check query
	stmt := prepare(pq.db, query)
	if stmt != nil {
		rows, err := stmt.Query(args)
		if err != nil {
			log.Println("Errors when query with statement: ", stmt, ". The error is: ", err)
			return new(sql.Rows), closeStmt(stmt)
		}
		return rows, closeStmt(stmt)
	}
	return new(sql.Rows), closeStmt(stmt)
}

func closeStmt(stmt *sql.Stmt) error {
	closeErr := stmt.Close()
	if closeErr != nil {
		log.Println("Error occurs when Closing Statement. The error is: ", closeErr)
	}
	return closeErr
}

func (pq PostgresDataBase) Insert(query string, args ...interface{}) error {
	// TODO pre-check insert query
	stmt := prepare(pq.db, query)
	return execute(stmt, args)
}

func execute(stmt *sql.Stmt, args ...interface{}) error {
	if stmt != nil {
		_, err := stmt.Exec(args)
		if err != nil {
			log.Println("Error occurs when execute")
			e := closeStmt(stmt)
			if e != nil {
				return e
			}
			return err
		}
	}
	return nil
}

func (pq PostgresDataBase) Delete(query string, args ...interface{}) error {
	// TODO pre-check delete query
	stmt := prepare(pq.db, query)
	return execute(stmt, args)
}

func (pq PostgresDataBase) Update(query string, args ...interface{}) error {
	// TODO pre-check update query
	stmt := prepare(pq.db, query)
	return execute(stmt, args)
}

func (pq PostgresDataBase) GetDriverName() string {
	return postgres
}

func UpdateIfConflict(db DataBase, tableName string, columnList []string, valueList [][]string, conflict string) error {
	if db.GetDriverName() == postgres {

	}
	return UnsupportedDataBaseDriver{driverName: db.GetDriverName()}
}