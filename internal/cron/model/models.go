package model

import "time"

type JobType int

const (
	JobTypeCursor JobType = iota
	JobTypeBatch
)

type JobStatus string

const (
	JobStatusRunning JobStatus = "running"
	JobStatusStopped JobStatus = "stopped"
)

type JobInfo struct {
	Name       string     `json:"name"`
	Type       JobType    `json:"type"`
	Status     JobStatus  `json:"status"`
	LastRun    *time.Time `json:"last_run,omitempty"`
	NextRun    *time.Time `json:"next_run,omitempty"`
	LastCursor *time.Time `json:"last_cursor,omitempty"`
	Processed  int        `json:"processed"`
}

type JobConfig struct {
	Name      string
	Type      JobType
	Interval  time.Duration
	StartFrom *time.Time
}
