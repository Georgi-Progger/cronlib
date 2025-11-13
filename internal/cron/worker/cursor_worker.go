package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Georgi-Progger/cron/internal/cron/model"
	"github.com/Georgi-Progger/cron/internal/cron/processor"
	"github.com/Georgi-Progger/cron/internal/redis"
)

type CursorWorker struct {
	handler processor.CursorHandler
	storage *redis.Client
	config  *model.JobConfig
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewCursorWorker(handler processor.CursorHandler, storage *redis.Client, config *model.JobConfig) *CursorWorker {
	return &CursorWorker{
		handler: handler,
		storage: storage,
		config:  config,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

func (w *CursorWorker) Start(ctx context.Context) {
	go w.run(ctx)
}

func (w *CursorWorker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

func (w *CursorWorker) run(ctx context.Context) {
	defer close(w.doneCh)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ticker := time.NewTicker(w.handler.GetInterval())
	defer ticker.Stop()

	logger.Warn("Cursor worker started")

	for {
		select {
		case <-ticker.C:
			w.processBatch(ctx)
		case <-w.stopCh:
			logger.Warn("Cursor worker stopped")
			return
		case <-ctx.Done():
			logger.Warn("Cursor worker cancelled")
			return
		}
	}
}

func (w *CursorWorker) processBatch(ctx context.Context) {
	lockKey := fmt.Sprintf("lock:%s", w.config.Name)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	locked, err := w.storage.Lock(ctx, lockKey, 30*time.Second)
	if err != nil || !locked {
		logger.Error("Failed to lock for job")
		return
	}
	defer w.storage.Unlock(ctx, lockKey)

	jobInfo, err := w.storage.GetJobInfo(ctx, w.config.Name)
	if err != nil {
		logger.Error("Failed to get job info for")
		return
	}

	fromTime := time.Time{}
	if jobInfo.LastCursor != nil {
		fromTime = *jobInfo.LastCursor
	} else if w.config.StartFrom != nil {
		fromTime = *w.config.StartFrom
	}

	toTime := time.Now()

	items, err := w.handler.GetBatch(ctx, fromTime, toTime)
	if err != nil {
		logger.Error("Failed to get batch for job")
		return
	}

	processed := 0
	for _, item := range items {
		select {
		case <-w.stopCh:
			return
		default:
			if err := w.handler.ProcessItem(ctx, item); err != nil {
				logger.Error("Failed to process item in job")
				continue
			}
			processed++
		}
	}

	now := time.Now()
	jobInfo.LastRun = &now
	nextRun := now.Add(w.handler.GetInterval())
	jobInfo.NextRun = &nextRun
	jobInfo.LastCursor = &toTime
	jobInfo.Processed += processed

	if err := w.storage.SaveJobInfo(ctx, w.config.Name, jobInfo); err != nil {
		logger.Error("Failed to save job info for")
	}
}
