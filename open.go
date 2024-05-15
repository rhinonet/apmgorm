// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package apmgorm // import "github.com/rhinonet/apmgorm/v2"

import (
	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"time"

	"go.elastic.co/apm/module/apmsql/v2"
)

// Open returns a *gorm.DB for the given dialect and arguments.
// The returned *gorm.DB will have callbacks registered with
// RegisterCallbacks, such that CRUD operations will be reported
// as spans.
//
// Open accepts the following signatures:
//   - a datasource name (i.e. the second argument to sql.Open)
//   - a driver name and a datasource name
//   - a *sql.DB, or some other type with the same interface
//
// If a driver and datasource name are supplied, and the appropriate
// apmgorm/dialects package has been imported (or the driver has
// otherwise been registered with apmsql), then the datasource name
// will be parsed for inclusion in the span context.
func Open(dialect string, args ...interface{}) (*gorm.DB, error) {
	var dialector gorm.Dialector
	var driverName, dsn string
	switch len(args) {
	case 1:
		switch arg0 := args[0].(type) {
		case string:
			driverName = dialect
			dsn = arg0
		}
	case 2:
		driverName, _ = args[0].(string)
		dsn, _ = args[1].(string)
	}

	switch driverName {
	case "postgres":
		dialector = postgres.Open(dsn)
	case "mysql":
		dialector = mysql.Open(dsn)
	}

	logLevel := logger.Info

	// 创建连接
	db, e := gorm.Open(dialector, &gorm.Config{
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             time.Second, // Slow SQL threshold
				LogLevel:                  logLevel,    // Log level
				IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
				Colorful:                  true,        // Disable color
			}),
	})
	if e != nil {
		return nil, errors.WithStack(e)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 最大连接数
	sqlDB.SetMaxOpenConns(100)

	// 最大闲置数
	sqlDB.SetMaxIdleConns(100)

	// 连接数据库闲置断线的问题
	sqlDB.SetConnMaxLifetime(time.Second * 60)

	// 激活链接
	if e = sqlDB.Ping(); e != nil {
		return nil, errors.WithStack(err)
	}

	registerCallbacks(db, apmsql.DriverDSNParser(driverName)(dsn))
	return db, nil
}
