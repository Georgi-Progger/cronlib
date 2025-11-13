package config

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/subosito/gotenv"
)

type Config struct {
	GRPCConfig  grpcConfig
	RedisConfig redisConfig
}

type grpcConfig struct {
	Port string
}

type redisConfig struct {
	Addr        string
	Password    string
	User        string
	DB          int
	MaxRetries  int
	DialTimeout time.Duration
	Timeout     time.Duration
}

func NewConfig() (Config, error) {
	if err := gotenv.Load(); err != nil {
		return Config{}, errors.New("error loading env")
	}

	redisAddr := os.Getenv("REDIS_ADDRESS")

	redisPswd := os.Getenv("REDIS_PASSWORD")
	redisUser := os.Getenv("REDIS_USER")

	var redisDb int
	dbStr := os.Getenv("REDIS_DB")
	redisDb, err := strconv.Atoi(dbStr)
	if err != nil {
		return Config{}, errors.New("error geting redisDb")
	}

	grpcPort := os.Getenv("GRPC_PORT")

	cfg := Config{
		GRPCConfig: grpcConfig{
			Port: grpcPort,
		},
		RedisConfig: redisConfig{
			Addr:     redisAddr,
			Password: redisPswd,
			User:     redisUser,
			DB:       redisDb,
		},
	}

	return cfg, nil
}
