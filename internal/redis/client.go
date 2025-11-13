package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Georgi-Progger/cron/internal/cron/model"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	client *redis.Client
}

func NewClient(addr, password, user string, db int) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		Username: user,
		DB:       db,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		fmt.Printf("failed to connect to redis server: %s\n", err.Error())
		return nil, err
	}

	return &Client{
		client: client,
	}, nil
}

func (c *Client) SaveJobInfo(ctx context.Context, key string, info *model.JobInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, 0).Err()
}

func (s *Client) GetJobInfo(ctx context.Context, key string) (*model.JobInfo, error) {
	val, err := s.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var info model.JobInfo
	if err := json.Unmarshal([]byte(val), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (s *Client) ListJobs(ctx context.Context) ([]model.JobInfo, error) {
	keys, err := s.client.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}

	var jobs []model.JobInfo
	for _, key := range keys {
		val, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var job model.JobInfo
		if err := json.Unmarshal([]byte(val), &job); err != nil {
			continue
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (c *Client) Lock(ctx context.Context, key string, timeout time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, "lock", timeout).Result()
}

func (c *Client) Unlock(ctx context.Context, key string) error {
	_, err := c.client.Del(ctx, key).Result()
	return err
}
