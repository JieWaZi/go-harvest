package service

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"time"
)

type MySQL struct {
	Host         string
	Port         int
	User         string
	Password     string
	DBName       string
	MaxIdleConns int
	MaxOpenConns int
	MaxLifetime  time.Duration
	db           *sql.DB
}

func (c MySQL) Init() error {
	dsn := getDSNWithDB(c)
	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		code, ok := getSQLErrCode(err)
		if !ok {
			return errors.WithStack(err)
		}
		if code == 1049 { // Database not exists
			if err := createDatabase(c); err != nil {
				return err
			}
		}
		db, err = gorm.Open("mysql", dsn)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if c.MaxIdleConns == 0 {
		db.DB().SetMaxIdleConns(3)
	}
	if c.MaxOpenConns == 0 {
		db.DB().SetMaxOpenConns(5)
	}
	if c.MaxLifetime == 0 {
		db.DB().SetConnMaxLifetime(time.Hour)
	}

	c.db = db.DB()
	return nil
}
func (c MySQL) Get() *sql.DB {
	if c.db == nil {
		c.Init()
	}
	return c.db
}
func getDSNWithDB(conf MySQL) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		conf.User, conf.Password, conf.Host, conf.Port, conf.DBName)
}
func getSQLErrCode(err error) (int, bool) {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return -1, false
	}

	return int(mysqlErr.Number), true
}
func createDatabase(conf MySQL) error {
	dsn := getBaseDSN(conf)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + conf.DBName)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
func getBaseDSN(conf MySQL) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		conf.User, conf.Password, conf.Host, conf.Port)
}
