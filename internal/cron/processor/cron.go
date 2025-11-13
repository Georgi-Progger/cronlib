package processor

import (
	"context"
	"time"
)

type (
	CursorHandler interface {
		BaseHandler
		GetBatch(ctx context.Context, fromTime, toTime time.Time) ([]interface{}, error)
		ProcessItem(ctx context.Context, item interface{}) error
	}

	BatchHandler interface {
		BaseHandler
		ProcessBatch(ctx context.Context) error
	}

	BaseHandler interface {
		GetName() string
		GetInterval() time.Duration
	}
)
