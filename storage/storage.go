package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Georgi-Progger/cronlib/model"
	"github.com/redis/go-redis/v9"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type Storage interface {
	SaveJob(ctx context.Context, job model.Job) error
	GetJob(ctx context.Context, name string) (model.Job, error)
	ListJobs(ctx context.Context) ([]model.Job, error)

	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
}

type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(client *redis.Client) *RedisStorage {
	return &RedisStorage{
		client: client,
	}
}

func (s *RedisStorage) key(name string) string {
	return fmt.Sprintf("job:%s", name)
}

func (s *RedisStorage) lockKey(key string) string {
	return fmt.Sprintf("lock:%s", key)
}

func (s *RedisStorage) SaveJob(ctx context.Context, job model.Job) error {
	if len(job.Name) == 0 {
		return errors.New("job name not be empty")
	}

	value, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("narshal job error: %w", err)
	}

	return s.client.Set(ctx, s.key(job.Name), value, 0).Err()
}

func (s *RedisStorage) GetJob(ctx context.Context, name string) (model.Job, error) {
	if len(name) == 0 {
		return model.Job{}, errors.New("job name cannot be empty")
	}

	key := s.key(name)
	data, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return model.Job{}, ErrJobNotFound
		}
		return model.Job{}, fmt.Errorf("get job from redis: %w", err)
	}

	var job model.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return model.Job{}, fmt.Errorf("unmarshal job: %w", err)
	}

	return job, nil
}

func (s *RedisStorage) ListJobs(ctx context.Context) ([]model.Job, error) {
	pattern := s.key("*")
	keys, err := s.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("get job keys: %w", err)
	}

	jobs := make([]model.Job, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var job model.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			continue
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (s *RedisStorage) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("key cannot be empty")
	}
	if ttl <= 0 {
		return false, errors.New("ttl must be positive")
	}

	lockKey := s.lockKey(key)
	result, err := s.client.SetNX(ctx, lockKey, "lock", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquire lock: %w", err)
	}
	return result, nil
}

func (s *RedisStorage) ReleaseLock(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("lock key cannot be empty")
	}
	lockKey := s.lockKey(key)
	_, err := s.client.Del(ctx, lockKey).Result()
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	return nil
}
