package pkg

import (
	"context"

	"github.com/yur/sql-runner/pkg/models"
)

type HandleRequestFunc func(ctx context.Context, dsInfo *models.DataSource, req *TsdbQuery) (*Response, error)

func HandleRequest(ctx context.Context, dsInfo *models.DataSource, req *TsdbQuery) (*Response, error) {
	endpoint, err := getTsdbQueryEndpointFor(dsInfo)
	if err != nil {
		return nil, err
	}

	return endpoint.Query(ctx, dsInfo, req)
}
