package sqleng

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/dafanshu/sql-runner/pkg"
	"github.com/dafanshu/sql-runner/pkg/components/simplejson"
	"github.com/dafanshu/sql-runner/pkg/models"
	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"
)

// SqlQueryResultTransformer transforms a query result row to RowValues with proper types.
type SqlQueryResultTransformer interface {
	// TransformQueryResult transforms a query result row to RowValues with proper types.
	TransformQueryResult(columnTypes []*sql.ColumnType, rows *core.Rows) (pkg.RowValues, error)
	// TransformQueryError transforms a query error.
	TransformQueryError(err error) error
}

type engineCacheType struct {
	cache    map[int64]*xorm.Engine
	versions map[int64]int
	sync.Mutex
}

var engineCache = engineCacheType{
	cache:    make(map[int64]*xorm.Engine),
	versions: make(map[int64]int),
}

var sqlIntervalCalculator = pkg.NewIntervalCalculator(nil)

var NewXormEngine = func(driverName string, connectionString string) (*xorm.Engine, error) {
	return xorm.NewEngine(driverName, connectionString)
}

type sqlQueryEndpoint struct {
	queryResultTransformer SqlQueryResultTransformer
	engine                 *xorm.Engine
	timeColumnNames        []string
	metricColumnTypes      []string
}

type SqlQueryEndpointConfiguration struct {
	DriverName        string
	Datasource        *models.DataSource
	ConnectionString  string
	TimeColumnNames   []string
	MetricColumnTypes []string
}

var NewSqlQueryEndpoint = func(config *SqlQueryEndpointConfiguration, queryResultTransformer SqlQueryResultTransformer) (pkg.TsdbQueryEndpoint, error) {
	queryEndpoint := sqlQueryEndpoint{
		queryResultTransformer: queryResultTransformer,
		timeColumnNames:        []string{"time"},
	}

	if len(config.TimeColumnNames) > 0 {
		queryEndpoint.timeColumnNames = config.TimeColumnNames
	}

	if len(config.MetricColumnTypes) > 0 {
		queryEndpoint.metricColumnTypes = config.MetricColumnTypes
	}

	engineCache.Lock()
	defer engineCache.Unlock()

	if engine, present := engineCache.cache[config.Datasource.Id]; present {
		if version := engineCache.versions[config.Datasource.Id]; version == config.Datasource.Version {
			queryEndpoint.engine = engine
			return &queryEndpoint, nil
		}
	}

	engine, err := NewXormEngine(config.DriverName, config.ConnectionString)
	if err != nil {
		return nil, err
	}

	//maxOpenConns := config.Datasource.JsonData.Get("maxOpenConns").MustInt(0)
	engine.SetMaxOpenConns(0)
	//maxIdleConns := config.Datasource.JsonData.Get("maxIdleConns").MustInt(2)
	engine.SetMaxIdleConns(2)
	//connMaxLifetime := config.Datasource.JsonData.Get("connMaxLifetime").MustInt(14400)
	engine.SetConnMaxLifetime(time.Duration(14400) * time.Second)

	engineCache.versions[config.Datasource.Id] = config.Datasource.Version
	engineCache.cache[config.Datasource.Id] = engine
	queryEndpoint.engine = engine

	return &queryEndpoint, nil
}

const rowLimit = 1000000

// Query is the main function for the SqlQueryEndpoint
func (e *sqlQueryEndpoint) Query(ctx context.Context, dsInfo *models.DataSource, tsdbQuery *pkg.TsdbQuery) (*pkg.Response, error) {
	result := &pkg.Response{
		Results: make(map[string]*pkg.QueryResult),
	}

	var wg sync.WaitGroup

	for _, query := range tsdbQuery.Queries {
		rawSQL := query.Model.Get("rawSql").MustString()
		if rawSQL == "" {
			continue
		}

		queryResult := &pkg.QueryResult{Meta: simplejson.New(), RefId: query.RefId}
		result.Results[query.RefId] = queryResult

		queryResult.Meta.Set("sql", rawSQL)

		wg.Add(1)

		go func(rawSQL string, query *pkg.Query, queryResult *pkg.QueryResult) {
			defer wg.Done()
			session := e.engine.NewSession()
			defer session.Close()
			db := session.DB()

			var rows *core.Rows
			var err error
			params := query.Params
			if len(params) > 0 {
				rows, err = db.QueryMap(rawSQL, &params)
			} else {
				rows, err = db.Query(rawSQL)
			}

			if err != nil {
				fmt.Println(err)
				queryResult.Error = e.queryResultTransformer.TransformQueryError(err)
				return
			}

			defer rows.Close()

			err = e.transformToTable(query, rows, queryResult, tsdbQuery)
			if err != nil {
				queryResult.Error = err
				return
			}
		}(rawSQL, query, queryResult)
	}
	wg.Wait()

	return result, nil
}

func (e *sqlQueryEndpoint) transformToTable(query *pkg.Query, rows *core.Rows, result *pkg.QueryResult, tsdbQuery *pkg.TsdbQuery) error {
	columnNames, err := rows.Columns()
	columnCount := len(columnNames)

	if err != nil {
		return err
	}

	rowCount := 0
	timeIndex := -1

	table := &pkg.Table{
		Columns: make([]pkg.TableColumn, columnCount),
		Rows:    make([]pkg.RowValues, 0),
	}

	for i, name := range columnNames {
		table.Columns[i].Text = name

		for _, tc := range e.timeColumnNames {
			if name == tc {
				timeIndex = i
				break
			}
		}
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for ; rows.Next(); rowCount++ {
		if rowCount > rowLimit {
			return fmt.Errorf("query row limit exceeded, limit %d", rowLimit)
		}

		values, err := e.queryResultTransformer.TransformQueryResult(columnTypes, rows)
		if err != nil {
			return err
		}

		// converts column named time to unix timestamp in milliseconds
		// to make native mssql datetime types and epoch dates work in
		// annotation and table queries.
		ConvertSqlTimeColumnToEpochMs(values, timeIndex)
		table.Rows = append(table.Rows, values)
	}

	result.Tables = append(result.Tables, table)
	result.Meta.Set("rowCount", rowCount)
	return nil
}

// ConvertSqlTimeColumnToEpochMs converts column named time to unix timestamp in milliseconds
// to make native datetime types and epoch dates work in annotation and table queries.
func ConvertSqlTimeColumnToEpochMs(values pkg.RowValues, timeIndex int) {
	if timeIndex >= 0 {
		switch value := values[timeIndex].(type) {
		case time.Time:
			values[timeIndex] = float64(value.UnixNano()) / float64(time.Millisecond)
		case *time.Time:
			if value != nil {
				values[timeIndex] = float64((*value).UnixNano()) / float64(time.Millisecond)
			}
		case int64:
			values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(value)))
		case *int64:
			if value != nil {
				values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(*value)))
			}
		case uint64:
			values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(value)))
		case *uint64:
			if value != nil {
				values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(*value)))
			}
		case int32:
			values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(value)))
		case *int32:
			if value != nil {
				values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(*value)))
			}
		case uint32:
			values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(value)))
		case *uint32:
			if value != nil {
				values[timeIndex] = int64(pkg.EpochPrecisionToMs(float64(*value)))
			}
		case float64:
			values[timeIndex] = pkg.EpochPrecisionToMs(value)
		case *float64:
			if value != nil {
				values[timeIndex] = pkg.EpochPrecisionToMs(*value)
			}
		case float32:
			values[timeIndex] = pkg.EpochPrecisionToMs(float64(value))
		case *float32:
			if value != nil {
				values[timeIndex] = pkg.EpochPrecisionToMs(float64(*value))
			}
		}
	}
}

func SetupFillmode(query *pkg.Query, interval time.Duration, fillmode string) error {
	query.Model.Set("fill", true)
	query.Model.Set("fillInterval", interval.Seconds())
	switch fillmode {
	case "NULL":
		query.Model.Set("fillMode", "null")
	case "previous":
		query.Model.Set("fillMode", "previous")
	default:
		query.Model.Set("fillMode", "value")
		floatVal, err := strconv.ParseFloat(fillmode, 64)
		if err != nil {
			return fmt.Errorf("error parsing fill value %v", fillmode)
		}
		query.Model.Set("fillValue", floatVal)
	}

	return nil
}

type SqlMacroEngineBase struct{}

func NewSqlMacroEngineBase() *SqlMacroEngineBase {
	return &SqlMacroEngineBase{}
}

func (m *SqlMacroEngineBase) ReplaceAllStringSubmatchFunc(re *regexp.Regexp, str string, repl func([]string) string) string {
	result := ""
	lastIndex := 0

	for _, v := range re.FindAllSubmatchIndex([]byte(str), -1) {
		groups := []string{}
		for i := 0; i < len(v); i += 2 {
			groups = append(groups, str[v[i]:v[i+1]])
		}

		result += str[lastIndex:v[0]] + repl(groups)
		lastIndex = v[1]
	}

	return result + str[lastIndex:]
}
