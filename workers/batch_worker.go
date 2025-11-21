package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Georgi-Progger/cronlib/logger"
	"github.com/Georgi-Progger/cronlib/model"
	"github.com/Georgi-Progger/cronlib/processor"
	"github.com/Georgi-Progger/cronlib/storage"
)

type BatchWorker struct {
	handler processor.BatchHandler
	storage storage.Storage
	logger  logger.Logger
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewBatchWorker(handler processor.BatchHandler, storage storage.Storage, logger logger.Logger) *BatchWorker {
	return &BatchWorker{
		handler: handler,
		storage: storage,
		logger:  logger,
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

	w.logger.Info("batch worker started", "job", w.handler.GetName())

	ticker := time.NewTicker(w.handler.GetInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.processBatch(ctx)
		case <-w.stopCh:
			w.logger.Info("batch worker stopped", "job", w.handler.GetName())
			return
		case <-ctx.Done():
			w.logger.Info("batch worker cancelled", "job", w.handler.GetName())
			return
		}
	}
}

func (w *BatchWorker) processBatch(ctx context.Context) {
	lockKey := fmt.Sprintf("lock:%s", w.handler.GetName())

	locked, err := w.storage.Lock(ctx, lockKey, 30*time.Second)
	if err != nil || !locked {
		w.logger.Error("failed to acquire lock", "job", w.handler.GetName(), "error", err)
		return
	}
	defer w.storage.ReleaseLock(ctx, lockKey)

	job, err := w.storage.GetJob(ctx, w.handler.GetName())
	if err != nil && !errors.Is(err, storage.ErrJobNotFound) {
		w.logger.Error("failed to get job", "job", w.handler.GetName(), "error", err)
		return
	}

	if errors.Is(err, storage.ErrJobNotFound) {
		job = model.Job{
			Name:     w.handler.GetName(),
			Type:     model.BatchJob,
			Status:   model.RunningStatus,
			Interval: w.handler.GetInterval(),
		}
	}

	if err := w.handler.ProcessBatch(ctx); err != nil {
		w.logger.Error("failed to process batch", "job", w.handler.GetName(), "error", err)
		job.Error = err.Error()
		job.Status = model.ErrorStatus
	} else {
		job.LastRun = time.Now()
		job.Status = model.RunningStatus
		job.Error = ""
	}

	if err := w.storage.SaveJob(ctx, job); err != nil {
		w.logger.Error("failed to save job", "job", w.handler.GetName(), "error", err)
	}
}
