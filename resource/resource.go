package resource

import (
	"database/sql"
	"log"
	"reflect"
	"strings"
)

var (
	postgres = "postgres"
	dbSupportMap = map[string]DataBase{
		postgres: new(PostgresDataBase),
	}
)

type UnsupportedDataBaseDriver struct {
	driverName string
}


func GetDB(driverName string) DataBase {
	return dbSupportMap[driverName]
}

func (noDbSupported UnsupportedDataBaseDriver) Error() string {
	return "Unsupported database driver: " + noDbSupported.driverName
}

type DataBase interface {
	Init(db *sql.DB)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Insert(query string, args ...interface{}) error
	Delete(query string, args ...interface{}) error
	Update(query string, args ...interface{}) error
	GetDriverName() string
}

type DataBaseFactory struct {
	driverName string
	datasourceName map[string]string
}

func (dbFactory DataBaseFactory) NewInstance() DataBase {
	db, err := createDataBase(dbFactory.driverName, dbFactory.datasourceName)
	if err != nil {
		if reflect.TypeOf(err).String() == "UnsupportedDataBaseDriver" {
			return nil
		}
		log.Panic("Create DataBase of: ", dbFactory.driverName, " is failed.")
	}
	dbSupportMap[dbFactory.driverName].Init(db)
	return dbSupportMap[dbFactory.driverName]
}

func createDataBase(driverName string, datasourceConfig map[string]string) (*sql.DB, error) {
	if dbSupportMap[driverName] != nil {
		config := convertDBConfig(datasourceConfig)
		return sql.Open(driverName, config)
	}
	return nil, UnsupportedDataBaseDriver{driverName: driverName}
}

func convertDBConfig(config map[string]string) string {
	builder := new(strings.Builder)
	for key := range config {
		builder.WriteString(key + config[key])
		builder.WriteString(" ")
	}
	return builder.String()
}
