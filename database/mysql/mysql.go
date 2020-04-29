package mysql

import (
	"context"
	"github.com/jinzhu/gorm"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"github.com/rex-ss/library/ecode"
	"time"
)

// Config mysql config.
type Config struct {
	DSN         string        // data source name.
	Active      int           // pool
	Idle        int           // pool
	IdleTimeout time.Duration // connect max life time.
}

type Dao struct {
	DB *gorm.DB
}

func init() {
	gorm.ErrRecordNotFound = encode.NothingFound
}

// NewMySQL new db and retry connection when has error.
func NewMySQL(c *Config) (db *gorm.DB) {
	db, err := gorm.Open("mysql", c.DSN)
	if err != nil {
		log.Error("db dsn(%s) error: %v", c.DSN, err)
		panic(err)
	}
	db.DB().SetMaxIdleConns(c.Idle)
	db.DB().SetMaxOpenConns(c.Active)
	db.DB().SetConnMaxLifetime(time.Duration(c.IdleTimeout) / time.Second)
	db.SetLogger(&logrus.Logger{})
	return
}

// New new a instance.
func New(c *Config) (d *Dao) {
	d = &Dao{
		DB: NewMySQL(c),
	}
	d.initORM()
	return
}

func (d *Dao) initORM() {
	d.DB.LogMode(true)
}

// Ping check connection of db , mc.
func (d *Dao) Ping(c context.Context) (err error) {
	if d.DB != nil {
		if err = d.DB.DB().PingContext(c); err != nil {
			log.Error("d.PingContext error (%v)", err)
		}
	}
	return
}

// Close close connection of db , mc.
func (d *Dao) Close() {
	if d.DB != nil {
		d.DB.Close()
	}
}
