package cache

import (
	"context"
	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/ro"
	"strings"
	"time"
)

func (c *CacheEngine) BatchGet(ctx context.Context, querierName string, ids []string, result *[]Object, refresh bool) error {
	querier, exists := c.querierMap[querierName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return ErrUnknownQuerier
	}
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return err
	}

	//allocate space
	*result = make([]Object, 0, len(ids))
	//query from cache
	missingIDs := ids
	if len(ids) > 0 {
		missingIDs, err = c.queryForCache(ctx, querier, client, ids, result)
		if err != nil {
			log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
			return err
		}
	}
	//all in cache
	if len(missingIDs) < 1 {
		log.Info(ctx, "All in cache")
		return nil
	} else if len(missingIDs) == len(ids) {
		log.Info(ctx, "All missing cache")
	} else {
		log.Info(ctx, "Parts in cache")
	}
	//query from database
	missingObjs, err := c.batchGetFromDB(ctx, querier, missingIDs)
	if err != nil {
		log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
		return err
	}

	//save cache
	go c.saveCache(ctx, querier, client, missingObjs)
	*result = append(*result, missingObjs...)

	*result = c.resort(ctx, ids, *result)

	//if need refresh, enqueue it
	if refresh {
		c.enqueueData(ctx, client, querierName, ids)
	}
	return nil
}
func (c *CacheEngine) Stop() {
	c.start = false
}

func (c *CacheEngine) Start() {
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
func (c *CacheEngine) doRefresh(ctx context.Context, client *redis.Client) {
	querierMap, err := c.dequeueData(ctx, client)
	if err != nil {
		log.Error(ctx, "dequeueData failed",
			log.Err(err))
		return
	}

	//enqueue
	for querierName, ids := range querierMap {
		querier, exists := c.querierMap[querierName]
		if !exists {
			log.Error(ctx, "GetRedis failed",
				log.String("querierName", querierName),
				log.Any("querierMap", c.querierMap))
			continue
		}
		objs, err := querier.BatchGet(ctx, ids)
		if err != nil {
			log.Error(ctx, "Query for refresh failed",
				log.Err(err),
				log.String("querier", querierName),
				log.Strings("ids", ids))
			continue
		}
		//update cache
		c.saveCache(ctx, querier, client, objs)

		//redo enqueue for next refresh
		c.enqueueData(ctx, client, querierName, ids)
	}
}

func (c *CacheEngine) enqueueData(ctx context.Context, client *redis.Client, querierName string, ids []string) {
	values := make([]interface{}, len(ids))
	for i := range ids {
		values[i] = querierName + klcIDSeparator + ids[i]
	}
	client.SAdd(klcRefreshPrefix, values...)
}

func (c *CacheEngine) dequeueData(ctx context.Context, client *redis.Client) (map[string][]string, error) {
	data, err := client.SPopN(klcRefreshPrefix, c.refreshSize).Result()
	if err != nil {
		log.Error(ctx, "pop redis set failed",
			log.Err(err))
		return nil, err
	}
	result := make(map[string][]string)
	for i := range data {
		keyPairs := strings.Split(data[i], klcIDSeparator)
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
