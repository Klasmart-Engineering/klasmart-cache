package cache

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/expirecalculator"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/utils"
	"gitlab.badanamu.com.cn/calmisland/ro"
	"sync"
	"time"
)

const (
	klcGlobalFeedbackPrefix = "klc:cache:expirecalculator:global"
	klcGroupFeedbackPrefix  = "klc:cache:expirecalculator:group:"
	klcIDFeedbackPrefix     = "klc:cache:expirecalculator:id:"
	klcIDExpirePrefix       = "klc:cache:expire:id:"

	defaultUpdateMaxFrequency = time.Minute * 30
	defaultUpdateMinFrequency = time.Second * 15
)

type idCache struct {
	id       string
	hitCache bool
}
type CacheExpire struct {
	ID         string
	ExpireTime int64
}

type IPassiveRefresher interface {
	BatchGet(ctx context.Context, querierName string, ids []string, result *[]Object) error
	SetUpdateFrequency(maxFrequency, minFrequency time.Duration)
}

type PassiveRefresher struct {
	engine             *CacheEngine
	maxUpdateFrequency time.Duration
	minUpdateFrequency time.Duration
}

func (c *PassiveRefresher) SetUpdateFrequency(maxFrequency, minFrequency time.Duration) {
	if maxFrequency < minFrequency {
		temp := maxFrequency
		maxFrequency = minFrequency
		minFrequency = temp
	}
	c.maxUpdateFrequency = maxFrequency
	c.minUpdateFrequency = minFrequency
}

func (c *PassiveRefresher) BatchGet(ctx context.Context,
	querierName string,
	ids []string,
	result *[]Object) error {
	querier, exists := c.engine.querierMap[querierName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("querierName", querierName),
			log.Any("querierMap", c.engine.querierMap))
		return ErrUnknownQuerier
	}
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return err
	}

	missingObjs, err := c.engine.fetchData(ctx, querierName, ids, result)
	if err != nil {
		log.Error(ctx, "fetchData failed", log.Err(err),
			log.Strings("ids", ids),
			log.String("querierName", querierName))
		return err
	}

	if len(missingObjs) > 0 {
		//save cache
		go c.saveCache(ctx, querier, client, ids, missingObjs)
	}

	return nil
}
func (c *PassiveRefresher) saveCache(ctx context.Context,
	querier IQuerier,
	client *redis.Client,
	ids []string,
	missingObjs []Object) {

	// maybe needs mutex
	idCaches, objMap := c.fetchObjects(ctx, ids, missingObjs)
	feedbackEntities, err := c.fetchFeedback(ctx, querier.ID(), idCaches)
	if err != nil {
		log.Error(ctx, "failed to fetch expirecalculator",
			log.Err(err),
			log.String("querierName", querier.ID()),
			log.Any("caches", idCaches))
		return
	}

	//calculate expire time
	feedbackRecord := make([]*entity.FeedbackRecordEntry, 0)
	for i := range feedbackEntities {
		expireTime := expirecalculator.GetExpireCalculator().Calculate(ctx, feedbackEntities[i])

		//limit time
		expireTime = c.expireLimit(expireTime)

		feedbackRecord[i] = &entity.FeedbackRecordEntry{
			ID:              feedbackEntities[i].ID,
			QuerierName:     feedbackEntities[i].QuerierName,
			CurrentFeedback: feedbackEntities[i].CurrentFeedback,
			ExpireTime:      expireTime,
		}

		c.engine.saveCache(ctx, querier, client, []Object{objMap[feedbackEntities[i].ID]}, expireTime)
	}

	//save expirecalculator info
	c.saveFeedback(ctx, querier.ID(), feedbackRecord)
}

func (c *PassiveRefresher) fetchObjects(ctx context.Context,
	ids []string,
	missingObjs []Object) ([]*idCache, map[string]Object) {
	idCaches := make([]*idCache, len(ids))
	objMap := make(map[string]Object)
	for i := range missingObjs {
		objMap[missingObjs[i].StringID()] = missingObjs[i]
	}
	for i := range ids {
		idCaches[i] = &idCache{
			id:       ids[i],
			hitCache: false,
		}
		obj := objMap[ids[i]]
		if obj != nil {
			idCaches[i].hitCache = true
		}
	}
	return idCaches, objMap
}

func (c *PassiveRefresher) saveFeedback(ctx context.Context, querierName string, newFeedback []*entity.FeedbackRecordEntry) error {
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return err
	}
	//save global data & group data
	globalData := make([]interface{}, len(newFeedback))
	groupData := make([]interface{}, len(newFeedback))
	for i := range newFeedback {
		globalData[i] = newFeedback[i].CurrentFeedback
		groupData[i] = newFeedback[i].CurrentFeedback
	}

	//save global data
	client.LPush(klcGlobalFeedbackPrefix, globalData...)
	//save group data
	client.LPush(klcGroupFeedbackPrefix+querierName, groupData...)

	//pending clean key list
	cleanKeyList := []string{
		klcGlobalFeedbackPrefix,
		klcGroupFeedbackPrefix + querierName,
	}

	//save id data
	for i := range newFeedback {
		key := idFeedbackPrefix(querierName, newFeedback[i].ID)
		client.LPush(key, newFeedback[i].CurrentFeedback)
		cleanKeyList = append(cleanKeyList, key)
	}

	//clean redis list
	c.cleanRedisList(ctx, client, cleanKeyList)

	//save expire
	c.saveExpireTime(ctx, client, newFeedback)
	return nil
}

func (c *PassiveRefresher) cleanRedisList(ctx context.Context, client *redis.Client, keys []string) {
	go func() {
		for i := range keys {
			size, err := client.LLen(keys[i]).Result()
			if err != nil {
				log.Error(ctx, "LLen failed", log.Err(err))
				return
			}
			cleanCount := int(size - entity.FeedbackRecordSize)
			for i := 0; i < cleanCount; i++ {
				client.LPop(keys[i])
			}
		}
	}()
}

func (c *PassiveRefresher) fetchFeedback(ctx context.Context,
	querierName string,
	idCaches []*idCache) ([]*entity.FeedbackEntry, error) {
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return nil, err
	}
	ids := make([]string, len(idCaches))
	for i := range idCaches {
		ids[i] = idCaches[i].id
	}

	globalData, groupData, err := c.fetchGlobalGroupFeedback(ctx, client, querierName)
	if err != nil {
		log.Error(ctx, "fetchGlobalGroupFeedback failed",
			log.String("querierName", querierName),
			log.Err(err))
		return nil, err
	}

	idDataMap, err := c.fetchIDFeedback(ctx, client, querierName, ids)
	if err != nil {
		log.Error(ctx, "fetchIDFeedback failed",
			log.String("querierName", querierName),
			log.Strings("ids", ids),
			log.Err(err))
		return nil, err
	}

	expireDataMap, err := c.fetchExpireTime(ctx, client, querierName, ids)
	if err != nil {
		log.Error(ctx, "fetchExpireTime failed",
			log.String("querierName", querierName),
			log.Strings("ids", ids),
			log.Err(err))
		return nil, err
	}

	result := make([]*entity.FeedbackEntry, len(idCaches))
	for i := range idCaches {
		currentFeedback := 0
		if idCaches[i].hitCache {
			currentFeedback = 1
		}

		result[i] = &entity.FeedbackEntry{
			ID:              idCaches[i].id,
			QuerierName:     querierName,
			CurrentFeedback: currentFeedback,
			RecentFeedback:  idDataMap[idCaches[i].id],
			GlobalFeedback:  globalData,
			GroupFeedback:   groupData,
			ExpireTime:      time.Duration(expireDataMap[idCaches[i].id]),
		}
	}
	return result, nil
}

func (c *PassiveRefresher) fetchIDFeedback(ctx context.Context,
	client *redis.Client,
	querierName string,
	ids []string) (map[string][]int, error) {

	idDataMap := make(map[string][]int)
	for i := range ids {
		idRaw, err := client.LRange(idFeedbackPrefix(querierName, ids[i]), 0, entity.FeedbackRecordSize).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Error(ctx, "Redis LRange id failed",
				log.String("querierName", querierName),
				log.String("id", ids[i]),
				log.Err(err))
			return nil, err
		}
		idDataMap[ids[i]] = utils.StringsToInts(ctx, idRaw)
	}

	return idDataMap, nil
}

func (c *PassiveRefresher) saveExpireTime(ctx context.Context,
	client *redis.Client,
	newFeedbacks []*entity.FeedbackRecordEntry) {

	cachePairs := make([]interface{}, len(newFeedbacks)*2)
	for i := range newFeedbacks {
		expireData := &CacheExpire{
			ID:         newFeedbacks[i].ID,
			ExpireTime: int64(newFeedbacks[i].ExpireTime),
		}
		jsonData, err := json.Marshal(expireData)
		if err != nil {
			log.Error(ctx, "marshal expireData failed",
				log.Err(err))
			continue
		}
		key := idExpirePrefix(newFeedbacks[i].QuerierName, newFeedbacks[i].ID)
		value := jsonData
		cachePairs = append(cachePairs, key)
		cachePairs = append(cachePairs, value)
	}
	client.MSet(cachePairs...)
}

func (c *PassiveRefresher) fetchExpireTime(ctx context.Context,
	client *redis.Client,
	querierName string,
	ids []string) (map[string]int64, error) {
	//expireTime
	expireData, err := client.MGet(c.engine.keyList(querierName, ids, idExpirePrefix)...).Result()
	//handle nil
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return nil, err
	}
	expireDataMap := make(map[string]int64)
	for i := range expireData {
		data, ok := expireData[i].(string)
		if !ok {
			continue
		}
		expireData := new(CacheExpire)
		err := json.Unmarshal([]byte(data), expireData)
		if err != nil {
			log.Error(ctx, "UnmarshalObject failed",
				log.Err(err),
				log.String("data", data))
			return nil, err
		}
		expireDataMap[expireData.ID] = expireData.ExpireTime
	}
	return expireDataMap, nil
}

func (c *PassiveRefresher) fetchGlobalGroupFeedback(ctx context.Context,
	client *redis.Client,
	querierName string) ([]int, []int, error) {
	var globalData []int
	var groupData []int

	globalRaw, err := client.LRange(klcGlobalFeedbackPrefix, 0, entity.FeedbackRecordSize).Result()
	if err != redis.Nil {
		if err != nil {
			log.Error(ctx, "Redis LRange global failed",
				log.Err(err))
			return nil, nil, err
		}
		globalData = utils.StringsToInts(ctx, globalRaw)
	}

	groupRaw, err := client.LRange(klcGroupFeedbackPrefix+querierName, 0, entity.FeedbackRecordSize).Result()
	if err != redis.Nil {
		if err != nil {
			log.Error(ctx, "Redis LRange group failed",
				log.String("querierName", querierName),
				log.Err(err))
			return nil, nil, err
		}
		groupData = utils.StringsToInts(ctx, groupRaw)
	}
	return globalData, groupData, nil
}

func (c *PassiveRefresher) expireLimit(expire time.Duration) time.Duration {
	if expire > c.maxUpdateFrequency {
		return c.maxUpdateFrequency
	}
	if expire < c.minUpdateFrequency {
		return c.minUpdateFrequency
	}
	return expire
}

func idFeedbackPrefix(querierName string, id string) string {
	return klcIDFeedbackPrefix + querierName + ":" + id
}
func idExpirePrefix(querierName string, id string) string {
	return klcIDExpirePrefix + querierName + ":" + id
}

var (
	_cachePassiveRefresherEngine     *PassiveRefresher
	_cachePassiveRefresherEngineOnce sync.Once
)

func GetPassiveCacheRefresher() *PassiveRefresher {
	_cachePassiveRefresherEngineOnce.Do(func() {
		_cachePassiveRefresherEngine = &PassiveRefresher{
			engine:             GetCacheEngine(),
			maxUpdateFrequency: defaultUpdateMaxFrequency,
			minUpdateFrequency: defaultUpdateMinFrequency,
		}
	})
	return _cachePassiveRefresherEngine
}
