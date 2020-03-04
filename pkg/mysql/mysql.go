package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/dafanshu/sql-runner/pkg"
	"github.com/dafanshu/sql-runner/pkg/models"
	"github.com/dafanshu/sql-runner/pkg/sqleng"
	"github.com/go-sql-driver/mysql"
	"xorm.io/core"
)

func init() {
	pkg.RegisterTsdbQueryEndpoint("mysql", newMysqlQueryEndpoint)
}

func characterEscape(s string, escapeChar string) string {
	return strings.Replace(s, escapeChar, url.QueryEscape(escapeChar), -1)
}

func newMysqlQueryEndpoint(datasource *models.DataSource) (pkg.TsdbQueryEndpoint, error) {
	protocol := "tcp"
	if strings.HasPrefix(datasource.Url, "/") {
		protocol = "unix"
	}

	cnnstr := fmt.Sprintf("%s:%s@%s(%s)/%s?collation=utf8mb4_unicode_ci&parseTime=true&loc=UTC&allowNativePasswords=true",
		characterEscape(datasource.User, ":"),
		datasource.DecryptedPassword(),
		protocol,
		characterEscape(datasource.Url, ")"),
		characterEscape(datasource.Database, "?"),
	)

	config := sqleng.SqlQueryEndpointConfiguration{
		DriverName:        "mysql",
		ConnectionString:  cnnstr,
		Datasource:        datasource,
		TimeColumnNames:   []string{"time", "time_sec"},
		MetricColumnTypes: []string{"CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"},
	}

	rowTransformer := mysqlQueryResultTransformer{}

	return sqleng.NewSqlQueryEndpoint(&config, &rowTransformer)
}

type mysqlQueryResultTransformer struct {
}

func (t *mysqlQueryResultTransformer) TransformQueryResult(columnTypes []*sql.ColumnType, rows *core.Rows) (pkg.RowValues, error) {
	values := make([]interface{}, len(columnTypes))

	for i := range values {
		scanType := columnTypes[i].ScanType()
		values[i] = reflect.New(scanType).Interface()

		if columnTypes[i].DatabaseTypeName() == "BIT" {
			values[i] = new([]byte)
		}
	}

	if err := rows.Scan(values...); err != nil {
		return nil, err
	}

	for i := 0; i < len(columnTypes); i++ {
		typeName := reflect.ValueOf(values[i]).Type().String()

		switch typeName {
		case "*sql.RawBytes":
			values[i] = string(*values[i].(*sql.RawBytes))
		case "*mysql.NullTime":
			sqlTime := (*values[i].(*mysql.NullTime))
			if sqlTime.Valid {
				values[i] = sqlTime.Time
			} else {
				values[i] = nil
			}
		case "*sql.NullInt64":
			nullInt64 := (*values[i].(*sql.NullInt64))
			if nullInt64.Valid {
				values[i] = nullInt64.Int64
			} else {
				values[i] = nil
			}
		case "*sql.NullFloat64":
			nullFloat64 := (*values[i].(*sql.NullFloat64))
			if nullFloat64.Valid {
				values[i] = nullFloat64.Float64
			} else {
				values[i] = nil
			}
		}

		if columnTypes[i].DatabaseTypeName() == "DECIMAL" {
			f, err := strconv.ParseFloat(values[i].(string), 64)

			if err == nil {
				values[i] = f
			} else {
				values[i] = nil
			}
		}
	}

	return values, nil
}

func (t *mysqlQueryResultTransformer) TransformQueryError(err error) error {
	if driverErr, ok := err.(*mysql.MySQLError); ok {
		if driverErr.Number != ER_PARSE_ERROR && driverErr.Number != ER_BAD_FIELD_ERROR && driverErr.Number != ER_NO_SUCH_TABLE {
			return errQueryFailed
		}
	}

	return err
}

var errQueryFailed = errors.New("Query failed. Please inspect Grafana server log for details")
