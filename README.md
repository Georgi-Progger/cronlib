Библиотека для запуска распределенных cron-джоб.

```bash
go get github.com/Georgi-Progger/cronlib
```

```go
import (
    "github.com/Georgi-Progger/cronlib/manager"
    "github.com/Georgi-Progger/cronlib/storage"
    "github.com/redis/go-redis/v9"
)

func main() {
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    
    storage := storage.NewRedisStorage(redisClient)
    
    logger := &Logger{}
    
    jobManager := manager.NewJobManager(storage, logger)
    
    if err := jobManager.RegisterCursorJob(&UserSyncJob{}); err != nil {
        log.Fatal(err)
    }
    
    startGRPCServer(jobManager)
}
```
