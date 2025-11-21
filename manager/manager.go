package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Georgi-Progger/cronlib/logger"
	"github.com/Georgi-Progger/cronlib/model"
	"github.com/Georgi-Progger/cronlib/processor"
	"github.com/Georgi-Progger/cronlib/storage"
	"github.com/Georgi-Progger/cronlib/workers"
)

type JobManager interface {
	RegisterCursorJob(handler processor.CursorHandler) error
	RegisterBatchJob(handler processor.BatchHandler) error

	StartJob(ctx context.Context, name string, until *time.Time) error
	StopJob(ctx context.Context, name string) error
	GetJob(ctx context.Context, name string) (model.Job, error)
	ListJobs(ctx context.Context) ([]model.Job, error)

	Shutdown(ctx context.Context) error
}

type Manager struct {
	storage storage.Storage
	mu      sync.RWMutex
	logger  logger.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	jobs    map[string]*jobWrapper
}

type jobWrapper struct {
	jobType model.JobType
	handler processor.BaseHandler
	worker  workerInterface
}

type workerInterface interface {
	Start(ctx context.Context)
	Stop()
}

func NewJobManager(storage storage.Storage, logger logger.Logger) Manager {
	ctx, cancel := context.WithCancel(context.Background())

	return Manager{
		storage: storage,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		jobs:    make(map[string]*jobWrapper),
	}
}

func (m *Manager) RegisterCursorJob(handler processor.CursorHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if handler.GetInterval() <= 0 {
		return fmt.Errorf("interval must be positive")
	}

	name := handler.GetName()
	if _, exists := m.jobs[name]; exists {
		return fmt.Errorf("job %q already registered", name)
	}

	m.jobs[name] = &jobWrapper{
		jobType: model.CursorJob,
		handler: handler,
	}

	m.logger.Info("cursor job registered", "name", name)
	return nil
}

func (m *Manager) RegisterBatchJob(handler processor.BatchHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := handler.GetName()
	if _, exists := m.jobs[name]; exists {
		return fmt.Errorf("job %q already registered", name)
	}

	m.jobs[name] = &jobWrapper{
		jobType: model.BatchJob,
		handler: handler,
	}

	m.logger.Info("batch job registered", "name", name)
	return nil
}

func (m *Manager) StartJob(ctx context.Context, name string, until *time.Time) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wrapper, exists := m.jobs[name]
	if !exists {
		return fmt.Errorf("job %q not found", name)
	}

	if wrapper.worker != nil {
		return fmt.Errorf("job %q is already running", name)
	}

	switch wrapper.jobType {
	case model.CursorJob:
		cursorHandler := wrapper.handler.(processor.CursorHandler)
		wrapper.worker = workers.NewCursorWorker(cursorHandler, m.storage, m.logger)
	case model.BatchJob:
		batchHandler := wrapper.handler.(processor.BatchHandler)
		wrapper.worker = workers.NewBatchWorker(batchHandler, m.storage, m.logger)
	default:
		return fmt.Errorf("unknown job type for %q", name)
	}

	wrapper.worker.Start(m.ctx)

	m.logger.Info("job started", "name", name)
	return nil
}

func (m *Manager) StopJob(ctx context.Context, name string) error {
	m.mu.Lock()

	wrapper, exists := m.jobs[name]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("job %q not found", name)
	}
	if wrapper.worker == nil {
		m.mu.Unlock()
		return fmt.Errorf("job %q is not running", name)
	}

	wrapper.worker.Stop()
	wrapper.worker = nil
	m.mu.Unlock()

	m.logger.Info("job stopped", "name", name)
	return nil
}

func (m *Manager) GetJob(ctx context.Context, name string) (model.Job, error) {
	job, err := m.storage.GetJob(ctx, name)
	if err != nil {
		return model.Job{}, fmt.Errorf("get job from storage: %w", err)
	}
	return job, nil
}

func (m *Manager) ListJobs(ctx context.Context) ([]model.Job, error) {
	jobs, err := m.storage.ListJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list jobs from storage: %w", err)
	}
	return jobs, nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.logger.Info("shutting down job manager")

	m.cancel()

	var errors []error

	m.mu.Lock()
	for name, wrapper := range m.jobs {
		if wrapper.worker != nil {
			wrapper.worker.Stop()
			wrapper.worker = nil
			m.logger.Info("worker stopped during shutdown", "name", name)
		}
	}
	m.mu.Unlock()

	if len(errors) > 0 {
		return fmt.Errorf("shutdown completed with errors: %v", errors)
	}

	m.logger.Info("job manager shutdown completed")
	return nil
}
