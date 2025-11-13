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

type BatchWorker struct {
	handler processor.BatchHandler
	storage *redis.Client
	config  *model.JobConfig
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewBatchWorker(handler processor.BatchHandler, storage *redis.Client, config *model.JobConfig) *BatchWorker {
	return &BatchWorker{
		handler: handler,
		storage: storage,
		config:  config,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

func (w *BatchWorker) Start(ctx context.Context) {
	go w.run(ctx)
}

func (w *BatchWorker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

func (w *BatchWorker) run(ctx context.Context) {
	defer close(w.doneCh)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ticker := time.NewTicker(w.handler.GetInterval())
	defer ticker.Stop()

	logger.Warn("Batch worker started")

	for {
		select {
		case <-ticker.C:
			w.processBatch(ctx)
		case <-w.stopCh:
			logger.Warn("Batch worker stopped")
			return
		case <-ctx.Done():
			logger.Warn("Batch worker cancelled")
			return
		}
	}
}

func (w *BatchWorker) processBatch(ctx context.Context) {
	lockKey := fmt.Sprintf("lock:%s", w.config.Name)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	locked, err := w.storage.Lock(ctx, lockKey, 30*time.Second)
	if err != nil || !locked {
		logger.Error("Failed to lock for job")
		return
	}
	defer w.storage.Unlock(ctx, lockKey)

	if err := w.handler.ProcessBatch(ctx); err != nil {
		logger.Error("Failed to process batch")
		return
	}

	jobInfo, err := w.storage.GetJobInfo(ctx, w.config.Name)
	if err != nil {
		jobInfo = &model.JobInfo{
			Name:   w.config.Name,
			Type:   model.JobTypeBatch,
			Status: model.JobStatusRunning,
		}
	}

	now := time.Now()
	jobInfo.LastRun = &now
	nextRun := now.Add(w.handler.GetInterval())
	jobInfo.NextRun = &nextRun
	jobInfo.Processed++

	if err := w.storage.SaveJobInfo(ctx, w.config.Name, jobInfo); err != nil {
		logger.Error("Failed to save job info for")
	}
}
