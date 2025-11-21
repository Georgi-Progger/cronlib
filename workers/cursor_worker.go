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

type CursorWorker struct {
	handler processor.CursorHandler
	storage storage.Storage
	logger  logger.Logger
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewCursorWorker(handler processor.CursorHandler, storage storage.Storage, logger logger.Logger) *CursorWorker {
	return &CursorWorker{
		handler: handler,
		storage: storage,
		logger:  logger,
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

	ticker := time.NewTicker(w.handler.GetInterval())
	defer ticker.Stop()

	w.logger.Info("cursor worker started", "job", w.handler.GetName())

	for {
		select {
		case <-ticker.C:
			w.processWithCursor(ctx)
		case <-w.stopCh:
			w.logger.Info("cursor worker stopped", "job", w.handler.GetName())
			return
		case <-ctx.Done():
			w.logger.Info("cursor worker cancelled", "job", w.handler.GetName())
			return
		}
	}
}

func (w *CursorWorker) processWithCursor(ctx context.Context) {
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
			Type:     model.CursorJob,
			Status:   model.RunningStatus,
			Interval: w.handler.GetInterval(),
		}
	}

	fromTime := time.Time{}
	if !job.LastRun.IsZero() {
		fromTime = job.LastRun
	}
	toTime := time.Now()

	items, err := w.handler.GetBatch(ctx, fromTime, toTime)
	if err != nil {
		w.logger.Error("failed to get batch", "job", w.handler.GetName(), "error", err)
		return
	}

	processed := 0
	for _, item := range items {
		select {
		case <-w.stopCh:
			return
		default:
			if err := w.handler.ProcessItem(ctx, item); err != nil {
				w.logger.Error("failed to process item", "job", w.handler.GetName(), "error", err)
				continue
			}
			processed++
		}
	}

	job.LastRun = toTime
	job.Status = model.RunningStatus

	if err := w.storage.SaveJob(ctx, job); err != nil {
		w.logger.Error("failed to save job", "job", w.handler.GetName(), "error", err)
	}
}
