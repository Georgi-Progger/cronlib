package server

import (
	"context"
	"time"

	"github.com/Georgi-Progger/cron/internal/cron/manager"
	"github.com/Georgi-Progger/cron/internal/cron/model"
	"github.com/Georgi-Progger/cron/pkg/cron_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type cronServer struct {
	cron_v1.UnimplementedCronV1Server
	manager *manager.JobManager
}

func NewCronServer(jobManager *manager.JobManager) *cronServer {
	return &cronServer{
		manager: jobManager,
	}
}

func (s *cronServer) Start(ctx context.Context, req *cron_v1.StartJobRequest) (*cron_v1.StartJobResponse, error) {
	var startFrom *time.Time
	if req.StartFrom != nil {
		time := req.StartFrom.AsTime()
		startFrom = &time
	}

	err := s.manager.StartJob(ctx, req.Name, req.Interval.AsDuration(), startFrom)
	if err != nil {
		return &cron_v1.StartJobResponse{
			Name:         req.Name,
			Started:      false,
			ErrorMessage: err.Error(),
		}, status.Error(codes.Internal, err.Error())
	}

	return &cron_v1.StartJobResponse{
		Name:    req.Name,
		Started: true,
	}, nil
}

func (s *cronServer) Stop(ctx context.Context, req *cron_v1.StopJobRequest) (*cron_v1.StopJobResponse, error) {
	jobInfo, err := s.manager.StopJob(req.Name)
	if err != nil {
		return &cron_v1.StopJobResponse{
			Name:    req.Name,
			Stopped: false,
		}, status.Error(codes.Internal, err.Error())
	}

	var lastProcessed *timestamppb.Timestamp
	if jobInfo.LastRun != nil {
		lastProcessed = timestamppb.New(*jobInfo.LastRun)
	}

	return &cron_v1.StopJobResponse{
		Name:              req.Name,
		Stopped:           true,
		ProcessedCount:    int32(jobInfo.Processed),
		LastProcessedTime: lastProcessed,
	}, nil
}

func (s *cronServer) ListJobs(ctx context.Context, req *cron_v1.ListJobsRequest) (*cron_v1.ListJobsResponse, error) {
	jobs, err := s.manager.ListJobs()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := &cron_v1.ListJobsResponse{
		Jobs: make([]*cron_v1.JobInfo, 0, len(jobs)),
	}

	for _, job := range jobs {
		jobInfo := &cron_v1.JobInfo{
			Name:   job.Name,
			Status: s.mapJobStatus(job.Status),
		}

		if job.LastRun != nil {
			jobInfo.LastRun = timestamppb.New(*job.LastRun)
		}
		if job.NextRun != nil {
			jobInfo.NextRun = timestamppb.New(*job.NextRun)
		}

		response.Jobs = append(response.Jobs, jobInfo)
	}

	return response, nil
}

func (s *cronServer) mapJobStatus(status model.JobStatus) cron_v1.JobStatus {
	switch status {
	case model.JobStatusRunning:
		return cron_v1.JobStatus_JOB_STATUS_RUNNING
	case model.JobStatusStopped:
		return cron_v1.JobStatus_JOB_STATUS_STOPPED
	default:
		return cron_v1.JobStatus_JOB_STATUS_UNKNOWN
	}
}
