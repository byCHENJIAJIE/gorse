// Copyright 2022 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/samber/lo"
	"github.com/zhenghaoz/gorse/base/log"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"

	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"
)

const (
	MySQLPrefix      = "mysql://"
	MongoPrefix      = "mongodb://"
	MongoSrvPrefix   = "mongodb+srv://"
	PostgresPrefix   = "postgres://"
	PostgreSQLPrefix = "postgresql://"
	SQLitePrefix     = "sqlite://"
	RedisPrefix      = "redis://"
	RedissPrefix     = "rediss://"
)

func AppendURLParams(rawURL string, params []lo.Tuple2[string, string]) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	q := parsed.Query()
	for _, tuple := range params {
		q.Add(tuple.A, tuple.B)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

func AppendMySQLParams(dsn string, params map[string]string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", errors.Trace(err)
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	for key, value := range params {
		if _, exist := cfg.Params[key]; !exist {
			cfg.Params[key] = value
		}
	}
	return cfg.FormatDSN(), nil
}

func ProbeMySQLIsolationVariableName(dsn string) (string, error) {
	connection, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer connection.Close()
	rows, err := connection.Query("SHOW VARIABLES LIKE '%isolation%'")
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()
	var name, value string
	if rows.Next() {
		if err = rows.Scan(&name, &value); err != nil {
			return "", errors.Trace(err)
		}
	}
	return name, nil
}

type TablePrefix string

func (tp TablePrefix) ValuesTable() string {
	return string(tp) + "values"
}

func (tp TablePrefix) SetsTable() string {
	return string(tp) + "sets"
}

func (tp TablePrefix) MessageTable() string {
	return string(tp) + "message"
}

func (tp TablePrefix) DocumentTable() string {
	return string(tp) + "documents"
}

func (tp TablePrefix) PointsTable() string {
	return string(tp) + "time_series_points"
}

func (tp TablePrefix) UsersTable() string {
	return string(tp) + "users"
}

func (tp TablePrefix) ItemsTable() string {
	return string(tp) + "items"
}

func (tp TablePrefix) FeedbackTable() string {
	return string(tp) + "feedback"
}

func (tp TablePrefix) Key(key string) string {
	return string(tp) + key
}

func NewGORMConfig(tablePrefix string) *gorm.Config {
	gormLogger := zapgorm2.New(log.Logger())
	if log.Logger().Level() == zapcore.DebugLevel {
		// Debug 模式：打印所有 SQL
		gormLogger.LogLevel = logger.Info
		gormLogger.SlowThreshold = 0
	} else {
		// 非 Debug 模式：只打印错误
		gormLogger.LogLevel = logger.Error          // 只打印错误日志
		gormLogger.SlowThreshold = time.Second      // 只打印超过1秒的慢查询
		gormLogger.IgnoreRecordNotFoundError = true // 忽略记录未找到的错误
	}

	return &gorm.Config{
		Logger:                 gormLogger,
		CreateBatchSize:        1000,
		SkipDefaultTransaction: true,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   tablePrefix,
			SingularTable: true,
			NameReplacer: strings.NewReplacer(
				"SQLValue", "Values",
				"SQLSet", "Sets",
				"SQLUser", "Users",
				"SQLItem", "Items",
				"SQLFeedback", "Feedback",
				"SQLDocument", "Documents",
				"PostgresDocument", "Documents",
				"TimeSeriesPoint", "time_series_points",
			),
		},
	}
}
