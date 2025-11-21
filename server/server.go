package grpc

import (
	"context"
	"time"

	"github.com/Georgi-Progger/cronlib/manager"
	"github.com/Georgi-Progger/cronlib/model"
	"github.com/Georgi-Progger/cronlib/pb/cron_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	cron_v1.UnimplementedCronV1Server
	manager manager.JobManager
}

func NewServer(manager manager.JobManager) *Server {
	return &Server{
		manager: manager,
	}
}

func (s *Server) Start(ctx context.Context, req *cron_v1.StartJobRequest) (*cron_v1.StartJobResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}

	var until *time.Time
	if req.Until != nil {
		untilTime := req.Until.AsTime()
		until = &untilTime
	}

	err := s.manager.StartJob(ctx, req.Name, until)
	if err != nil {
		return &cron_v1.StartJobResponse{
			Name:         req.Name,
			Status:       cron_v1.Status_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &cron_v1.StartJobResponse{
		Name:   req.Name,
		Status: cron_v1.Status_RUNNING,
	}, nil
}

func (s *Server) Stop(ctx context.Context, req *cron_v1.StopJobRequest) (*cron_v1.StopJobResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}

	err := s.manager.StopJob(ctx, req.Name)
	if err != nil {
		return &cron_v1.StopJobResponse{
			Status:       cron_v1.Status_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &cron_v1.StopJobResponse{
		Status: cron_v1.Status_STOPPED,
	}, nil
}

func (s *Server) GetJob(ctx context.Context, req *cron_v1.GetJobRequest) (*cron_v1.GetJobResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}

	job, err := s.manager.GetJob(ctx, req.Name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	jobInfo := &cron_v1.JobInfo{
		Name:   job.Name,
		Status: s.mapJobStatus(job.Status),
		Error:  job.Error,
	}

	if !job.LastRun.IsZero() {
		jobInfo.LastRun = timestamppb.New(job.LastRun)
	}

	return &cron_v1.GetJobResponse{
		Job: jobInfo,
	}, nil
}

func (s *Server) ListJobs(ctx context.Context) (*cron_v1.ListJobsResponse, error) {
	jobs, err := s.manager.ListJobs(ctx)
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
			Error:  job.Error,
		}

		if !job.LastRun.IsZero() {
			jobInfo.LastRun = timestamppb.New(job.LastRun)
		}

		response.Jobs = append(response.Jobs, jobInfo)
	}

	return response, nil
}

func (s *Server) mapJobStatus(status model.JobStatus) cron_v1.Status {
	switch status {
	case model.RunningStatus:
		return cron_v1.Status_RUNNING
	case model.StoppedStatus:
		return cron_v1.Status_STOPPED
	case model.ErrorStatus:
		return cron_v1.Status_ERROR
	default:
		return cron_v1.Status_STOPPED
	}
}
