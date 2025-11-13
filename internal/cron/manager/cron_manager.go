package manager

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Georgi-Progger/cron/internal/cron/model"
	"github.com/Georgi-Progger/cron/internal/cron/processor"
	"github.com/Georgi-Progger/cron/internal/cron/worker"
	"github.com/Georgi-Progger/cron/internal/redis"
)

type JobManager struct {
	storage *redis.Client
	mu      sync.RWMutex
	jobs    map[string]*jobWrapper
	ctx     context.Context
	cancel  context.CancelFunc
}

type jobWrapper struct {
	jobType model.JobType
	handler processor.BaseHandler
	worker  workerInterface
	config  *model.JobConfig
}

type workerInterface interface {
	Start(ctx context.Context)
	Stop()
}

func NewJobManager(storage *redis.Client) *JobManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &JobManager{
		storage: storage,
		jobs:    make(map[string]*jobWrapper),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (jm *JobManager) RegisterCursorJob(handler processor.CursorHandler) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	name := handler.GetName()
	if _, exists := jm.jobs[name]; exists {
		return errors.New("job already registered")
	}

	jm.jobs[name] = &jobWrapper{
		jobType: model.JobTypeCursor,
		handler: handler,
		config: &model.JobConfig{
			Name:     name,
			Type:     model.JobTypeCursor,
			Interval: handler.GetInterval(),
		},
	}

	return nil
}

func (jm *JobManager) RegisterBatchJob(handler processor.BatchHandler) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	name := handler.GetName()
	if _, exists := jm.jobs[name]; exists {
		return errors.New("job is already register")
	}

	jm.jobs[name] = &jobWrapper{
		jobType: model.JobTypeBatch,
		handler: handler,
		config: &model.JobConfig{
			Name:     name,
			Type:     model.JobTypeBatch,
			Interval: handler.GetInterval(),
		},
	}

	return nil
}

func (jm *JobManager) StartJob(ctx context.Context, name string, interval time.Duration, startFrom *time.Time) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	wrapper, exists := jm.jobs[name]
	if !exists {
		return errors.New("job not found")
	}

	wrapper.config.Interval = interval
	wrapper.config.StartFrom = startFrom

	switch wrapper.jobType {
	case model.JobTypeCursor:
		cursorHandler := wrapper.handler
		wrapper.worker = worker.NewCursorWorker(cursorHandler.(processor.CursorHandler), jm.storage, wrapper.config)

	case model.JobTypeBatch:
		batchHandler := wrapper.handler
		wrapper.worker = worker.NewBatchWorker(batchHandler.(processor.BatchHandler), jm.storage, wrapper.config)

	default:
		return errors.New("unknown job type for")
	}

	jobInfo := &model.JobInfo{
		Name:   name,
		Type:   wrapper.jobType,
		Status: model.JobStatusRunning,
	}
	if err := jm.storage.SaveJobInfo(ctx, name, jobInfo); err != nil {
		return errors.New("failed to save job info")
	}

	wrapper.worker.Start(jm.ctx)
	return nil
}

func (jm *JobManager) StopJob(name string) (*model.JobInfo, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	wrapper, exists := jm.jobs[name]
	if !exists {
		return nil, errors.New("job not found")
	}

	if wrapper.worker != nil {
		wrapper.worker.Stop()
	}

	jobInfo, err := jm.storage.GetJobInfo(context.Background(), name)
	if err != nil {
		return nil, errors.New("failed to get job info")
	}

	jobInfo.Status = model.JobStatusStopped
	if err := jm.storage.SaveJobInfo(context.Background(), name, jobInfo); err != nil {
		return nil, errors.New("failed to save job info")
	}

	return jobInfo, nil
}

func (jm *JobManager) ListJobs() ([]model.JobInfo, error) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	jobs, err := jm.storage.ListJobs(context.Background())
	if err != nil {
		return nil, errors.New("failed to list jobs")
	}

	return jobs, nil
}

func (jm *JobManager) GetJobStatus(name string) (model.JobStatus, error) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	jobInfo, err := jm.storage.GetJobInfo(context.Background(), name)
	if err != nil {
		return model.JobStatusStopped, errors.New("failed to get job status")
	}

	return jobInfo.Status, nil
}

func (jm *JobManager) Shutdown() {
	log.Println("Stop")
	jm.cancel()

	jm.mu.Lock()
	defer jm.mu.Unlock()

	for _, wrapper := range jm.jobs {
		if wrapper.worker != nil {
			wrapper.worker.Stop()
		}
	}

	log.Println("manager shutdown")
}
