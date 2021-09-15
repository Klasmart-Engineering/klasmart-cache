package cache

import (
	"context"
	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/constant"
	"gitlab.badanamu.com.cn/calmisland/ro"
	"strings"
	"sync"
	"time"
)

const (
	defaultRefreshSize     = 10
	defaultRefreshInterval = time.Second * 30
)

type CacheRefresher struct {
	engine *CacheEngine

	refreshSize     int64
	refreshInterval time.Duration

	start bool
}

func (c *CacheRefresher) SetRefreshSize(ctx context.Context, refreshSize int64) {
	c.refreshSize = refreshSize
}
func (c *CacheRefresher) SetRefreshInterval(ctx context.Context, refreshInterval time.Duration) {
	c.refreshInterval = refreshInterval
}
func (c *CacheRefresher) BatchGet(ctx context.Context, querierName string, ids []string, result *[]Object, refresh bool) error {
	err := c.engine.BatchGet(ctx, querierName, ids, result, InfiniteExpire)
	if err != nil {
		log.Error(ctx, "QueryByIDs failed",
			log.Err(err),
			log.String("querierName", querierName),
			log.Strings("ids", ids))
		return err
	}

	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return err
	}

	//if need refresh, enqueue it
	if refresh {
		c.enqueueData(ctx, client, querierName, ids)
	}
	return nil
}

func (c *CacheRefresher) Stop() {
	c.start = false
}

func (c *CacheRefresher) Start() {
	ctx := context.Background()
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return
	}
	c.start = true
	go func() {
		//sleep 30 seconds
		for c.start {
			time.Sleep(c.refreshInterval)
			c.doRefresh(ctx, client)
		}
	}()
}
func (c *CacheRefresher) doRefresh(ctx context.Context, client *redis.Client) {
	querierMap, err := c.dequeueData(ctx, client)
	if err != nil {
		log.Error(ctx, "dequeueData failed",
			log.Err(err))
		return
	}

	//enqueue
	for querierName, ids := range querierMap {
		querier, exists := c.engine.querierMap[querierName]
		if !exists {
			log.Error(ctx, "GetRedis failed",
				log.String("querierName", querierName),
				log.Any("querierMap", c.engine.querierMap))
			continue
		}
		objs, err := querier.QueryByIDs(ctx, ids)
		if err != nil {
			log.Error(ctx, "Query for refresh failed",
				log.Err(err),
				log.String("querier", querierName),
				log.Strings("ids", ids))
			continue
		}
		//update cache
		c.engine.saveCache(ctx, querier, client, objs, 0)

		//redo enqueue for next refresh
		c.enqueueData(ctx, client, querierName, ids)
	}
}

func (c *CacheRefresher) enqueueData(ctx context.Context, client *redis.Client, querierName string, ids []string) {
	values := make([]interface{}, len(ids))
	for i := range ids {
		values[i] = querierName + constant.KlcIDSeparator + ids[i]
	}
	client.SAdd(constant.KlcRefreshPrefix, values...)
}

func (c *CacheRefresher) dequeueData(ctx context.Context, client *redis.Client) (map[string][]string, error) {
	data, err := client.SPopN(constant.KlcRefreshPrefix, c.refreshSize).Result()
	if err != nil {
		log.Error(ctx, "pop redis set failed",
			log.Err(err))
		return nil, err
	}
	result := make(map[string][]string)
	for i := range data {
		keyPairs := strings.Split(data[i], constant.KlcIDSeparator)
		if len(keyPairs) != 2 {
			log.Error(ctx, "pop redis set failed",
				log.Err(err),
				log.String("data", data[i]),
				log.Strings("all data", data))
			continue
		}
		querierName := keyPairs[0]
		id := keyPairs[1]

		querierData := result[querierName]
		if querierData == nil {
			querierData = make([]string, 0)
		}
		querierData = append(querierData, id)
		result[querierName] = querierData
	}
	return result, nil
}

var (
	_cacheRefresherEngine     *CacheRefresher
	_cacheRefresherEngineOnce sync.Once
)

func GetCacheRefresher() *CacheRefresher {
	_cacheRefresherEngineOnce.Do(func() {
		_cacheRefresherEngine = &CacheRefresher{
			engine:          GetCacheEngine(),
			refreshSize:     defaultRefreshSize,
			refreshInterval: defaultRefreshInterval,
		}
	})
	return _cacheRefresherEngine
}
