package pkg

import (
	"context"
	"fmt"

	"sql-runner/pkg/models"
)

type MysqlExecutor struct {
	results   map[string]*QueryResult
	resultsFn map[string]ResultsFn
}

type ResultsFn func(context *TsdbQuery) *QueryResult

func NewMysqlExecutor(dsInfo *models.DataSource) (*MysqlExecutor, error) {
	return &MysqlExecutor{
		results:   make(map[string]*QueryResult),
		resultsFn: make(map[string]ResultsFn),
	}, nil
}

func (e *MysqlExecutor) Query(ctx context.Context, dsInfo *models.DataSource, context *TsdbQuery) (*Response, error) {
	result := &Response{Results: make(map[string]*QueryResult)}
	for _, query := range context.Queries {
		if results, has := e.results[query.RefId]; has {
			result.Results[query.RefId] = results
		}
		if testFunc, has := e.resultsFn[query.RefId]; has {
			result.Results[query.RefId] = testFunc(context)
		}
	}

	fmt.Println("Query >>> mysql_query")

	return result, nil
}

func (e *MysqlExecutor) Return(refId string, series TimeSeriesSlice) {
	e.results[refId] = &QueryResult{
		RefId: refId,
	}
}

func (e *MysqlExecutor) HandleQuery(refId string, fn ResultsFn) {
	e.resultsFn[refId] = fn
}
