package model

import "time"

type Job struct {
	Name     string
	Error    string
	Type     JobType
	Status   JobStatus
	LastRun  time.Time
	Interval time.Duration
}

type JobType int8

const (
	CursorJob JobType = iota
	BatchJob
)

type JobStatus int8

const (
	RunningStatus JobStatus = iota
	StoppedStatus
	ErrorStatus
)
