package cache

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/constant"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/expirecalculator"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/statistics"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/utils"
	"gitlab.badanamu.com.cn/calmisland/ro"
)

const (
	defaultUpdateMaxFrequency = time.Minute * 30
	defaultUpdateMinFrequency = time.Second * 15
)

type idCache struct {
	id       string
	hitCache bool
}
type CacheExpire struct {
	ID             string
	ExpireAt       time.Time
	ExpireDuration time.Duration
}

type expiredObject struct {
	object      Object
	expiredInfo *CacheExpire
}

type fetchObjectDataResponse struct {
	dbObjects      map[string]Object
	expiredObjects map[string]*expiredObject
}

type IPassiveRefresher interface {
	BatchGet(ctx context.Context, dataSourceName string, ids []string, result interface{}, options ...interface{}) error
	SetUpdateFrequency(maxFrequency, minFrequency time.Duration)
	SetExpireCalculator(expireCalculator expirecalculator.IExpireCalculator)
}

type PassiveRefresher struct {
	engine             *CacheEngine
	maxUpdateFrequency time.Duration
	minUpdateFrequency time.Duration
	expiredCalculator  expirecalculator.IExpireCalculator
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

func (c *PassiveRefresher) SetExpireCalculator(expireCalculator expirecalculator.IExpireCalculator) {
	c.expiredCalculator = expireCalculator
}

func (c *PassiveRefresher) BatchGet(ctx context.Context,
	dataSourceName string,
	ids []string,
	res interface{},
	options ...interface{}) error {
	querier, exists := c.engine.querierMap[dataSourceName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("dataSourceName", dataSourceName),
			log.Any("querierMap", c.engine.querierMap))
		return ErrUnknownQuerier
	}
	result, err := NewReflectObjectSlice(res)
	if err != nil {
		log.Error(ctx, "NewReflectObjectSlice failed", log.Err(err), log.Any("res", res))
		return err
	}
	//close cache
	if !c.engine.open {
		return c.engine.doBatchGetFromDB(ctx, dataSourceName, ids, result, options...)
	}

	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return err
	}

	objs, err := c.fetchData(ctx, querier, client, ids, result, options...)
	if err != nil {
		log.Error(ctx, "fetchData failed", log.Err(err),
			log.Strings("ids", ids),
			log.String("dataSourceName", dataSourceName))
		return err
	}

	if len(objs.dbObjects) > 0 {
		//save cache
		ctx2 := context.Background()
		badaCtx, ok := helper.GetBadaCtx(ctx)
		if ok {
			badaCtx.EmbedIntoContext(ctx2)
		}
		go c.saveCache(ctx2, querier, client, objs)
	}

	return nil
}

func (c *PassiveRefresher) fetchExpiredData(ctx context.Context,
	client *redis.Client,
	querierName string,
	hitIDs []string,
	result *ReflectObjectSlice) (map[string]*expiredObject, error) {
	expiredInfo, err := c.fetchExpireTime(ctx, client, querierName, hitIDs)
	if err != nil {
		log.Error(ctx, "fetchExpireTime failed",
			log.Err(err),
			log.String("querierName", querierName),
			log.Strings("hitIDs", hitIDs))
		return nil, err
	}
	//build objects map
	objMap := make(map[string]Object)
	result.Iterator(func(o Object) {
		objMap[o.StringID()] = o
	})

	expiredObjects := make(map[string]*expiredObject)
	now := time.Now()
	for i := range hitIDs {
		exp, exists := expiredInfo[hitIDs[i]]
		if (!exists) || now.After(exp.ExpireAt) {
			//add expire
			expiredObjects[hitIDs[i]] = &expiredObject{
				object:      objMap[hitIDs[i]],
				expiredInfo: exp,
			}
		}
	}

	return expiredObjects, nil
}

func (c *PassiveRefresher) fetchData(ctx context.Context,
	querier IDataSource,
	client *redis.Client,
	ids []string,
	result *ReflectObjectSlice,
	options ...interface{}) (*fetchObjectDataResponse, error) {

	//query from cache
	missingIDs := ids
	hitIDs := make([]string, 0, len(ids))
	var err error
	if len(ids) > 0 {
		hitIDs, missingIDs, err = c.engine.queryForCache(ctx, querier, client, ids, result)
		if err != nil {
			log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
			return nil, err
		}
	}
	//check hitIDs and add expiredIDs into missingIDs
	expiredObjects, err := c.fetchExpiredData(ctx, client, querier.Name(), hitIDs, result)
	if err != nil {
		log.Error(ctx, "fetchExpiredData failed",
			log.Err(err),
			log.String("querier name", querier.Name()),
			log.Strings("hitIDs", hitIDs))
		return nil, err
	}
	for i := range expiredObjects {
		if expiredObjects[i].object != nil {
			missingIDs = append(missingIDs, expiredObjects[i].object.StringID())
		}
	}
	log.Debug(ctx, "Expired ids",
		log.Any("expired objs", expiredObjects))

	missingIDsCount := len(missingIDs)
	allIDsCount := len(ids)

	ctx2 := context.Background()
	badaCtx, ok := helper.GetBadaCtx(ctx)
	if ok {
		badaCtx.EmbedIntoContext(ctx2)
	}

	go statistics.GetHitRatioRecorder().AddHitRatio(ctx2, allIDsCount-missingIDsCount, missingIDsCount)

	//all in cache
	if missingIDsCount < 1 {
		log.Info(ctx, "All in cache", log.Any("result", result.slice.Interface()))
		return &fetchObjectDataResponse{
			dbObjects:      nil,
			expiredObjects: expiredObjects,
		}, nil
	} else if missingIDsCount == allIDsCount {
		log.Info(ctx, "All missing cache",
			log.Strings("all ids", ids))
	} else {
		log.Info(ctx, "Parts in cache",
			log.Strings("missing IDs", missingIDs),
			log.Strings("all ids", ids), log.Any("result", result.slice.Interface()))
	}

	//query from database
	missingObjs, err := c.engine.batchGetFromDB(ctx, querier, missingIDs, options...)
	if err != nil {
		log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
		return nil, err
	}
	result.Append(missingObjs...)

	c.engine.resort(ctx, ids, result)

	dbObjects := make(map[string]Object)
	for i := range missingObjs {
		dbObjects[missingObjs[i].StringID()] = missingObjs[i]
	}
	return &fetchObjectDataResponse{
		dbObjects:      dbObjects,
		expiredObjects: expiredObjects,
	}, nil
}
func (c *PassiveRefresher) saveCache(ctx context.Context,
	querier IDataSource,
	client *redis.Client,
	objs *fetchObjectDataResponse) {

	// maybe needs mutex
	//idCaches, objMap := c.fetchObjects(ctx, ids, missingObjs)
	feedbackEntities, err := c.fetchFeedback(ctx, querier.Name(), objs)
	if err != nil {
		log.Error(ctx, "failed to fetch expirecalculator",
			log.Err(err),
			log.String("querierName", querier.Name()),
			log.Any("objs", objs))
		return
	}

	//calculate expire time
	feedbackRecord := make([]*entity.FeedbackRecordEntry, len(feedbackEntities))
	for i := range feedbackEntities {
		expireTime := c.expiredCalculator.Calculate(ctx, feedbackEntities[i])
		//limit time
		expireTime = c.expireLimit(expireTime)
		feedbackRecord[i] = &entity.FeedbackRecordEntry{
			ID:              feedbackEntities[i].ID,
			DataSourceName:  feedbackEntities[i].DataSourceName,
			CurrentFeedback: feedbackEntities[i].CurrentFeedback,
			ExpireTime:      expireTime,
		}

		if objs.dbObjects[feedbackEntities[i].ID] != nil {
			c.engine.saveCache(ctx, querier, client, []Object{objs.dbObjects[feedbackEntities[i].ID]}, MaxExpireTime)
		}
	}

	//save expirecalculator info
	c.saveFeedback(ctx, client, querier.Name(), feedbackRecord)
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

func (c *PassiveRefresher) saveFeedback(ctx context.Context,
	client *redis.Client,
	querierName string,
	newFeedback []*entity.FeedbackRecordEntry) {
	//save global data & group data
	globalData := make([]interface{}, len(newFeedback))
	groupData := make([]interface{}, len(newFeedback))
	for i := range newFeedback {
		globalData[i] = newFeedback[i].CurrentFeedback
		groupData[i] = newFeedback[i].CurrentFeedback
	}

	//save global data
	client.LPush(ctx, constant.KlcGlobalFeedbackPrefix, globalData...)
	//save group data
	client.LPush(ctx, constant.KlcGroupFeedbackPrefix+querierName, groupData...)

	//pending clean key list
	cleanKeyList := []string{
		constant.KlcGlobalFeedbackPrefix,
		constant.KlcGroupFeedbackPrefix + querierName,
	}

	//save id data
	for i := range newFeedback {
		key := idFeedbackPrefix(querierName, newFeedback[i].ID)
		client.LPush(ctx, key, newFeedback[i].CurrentFeedback)
		cleanKeyList = append(cleanKeyList, key)
	}

	//save expire
	c.saveExpireTime(ctx, client, newFeedback)

	//clean redis list
	c.cleanRedisList(ctx, client, cleanKeyList)
}

func (c *PassiveRefresher) cleanRedisList(ctx context.Context, client *redis.Client, keys []string) {
	//TODO: needs to lock
	for i := range keys {
		size, err := client.LLen(ctx, keys[i]).Result()
		if err != nil {
			log.Error(ctx, "LLen failed", log.Err(err))
			return
		}
		cleanCount := int(size - entity.FeedbackRecordSize)
		if cleanCount > entity.FeedbackRecordSize*10 {
			for j := 0; j < cleanCount; j++ {
				client.RPop(ctx, keys[i])
			}
		}
	}
}

func (c *PassiveRefresher) fetchFeedback(ctx context.Context,
	querierName string,
	objs *fetchObjectDataResponse) ([]*entity.FeedbackEntry, error) {
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return nil, err
	}

	globalData, groupData, err := c.fetchGlobalGroupFeedback(ctx, client, querierName)
	if err != nil {
		log.Error(ctx, "fetchGlobalGroupFeedback failed",
			log.String("querierName", querierName),
			log.Err(err))
		return nil, err
	}

	ids := make([]string, 0, len(objs.expiredObjects))
	for id := range objs.expiredObjects {
		ids = append(ids, id)
	}

	//expired data fetch feedback data
	idDataMap, err := c.fetchIDFeedback(ctx, client, querierName, ids)
	if err != nil {
		log.Error(ctx, "fetchIDFeedback failed",
			log.String("querierName", querierName),
			log.Strings("ids", ids),
			log.Err(err))
		return nil, err
	}

	now := time.Now()
	result := make([]*entity.FeedbackEntry, 0, len(objs.dbObjects))
	for _, obj := range objs.dbObjects {
		expiredObj, exists := objs.expiredObjects[obj.StringID()]
		if exists {
			if expiredObj.expiredInfo == nil {
				expiredObj.expiredInfo = &CacheExpire{
					ID:             obj.StringID(),
					ExpireAt:       now.Add(DefaultExpire),
					ExpireDuration: DefaultExpire,
				}
			}
			fbe := &entity.FeedbackEntry{
				ID:              obj.StringID(),
				DataSourceName:  querierName,
				CurrentFeedback: entity.FeedbackChanged,
				RecentFeedback:  idDataMap[obj.StringID()],
				GlobalFeedback:  globalData,
				GroupFeedback:   groupData,
				ExpireTime:      expiredObj.expiredInfo.ExpireDuration,
			}
			//unchanged
			if reflect.DeepEqual(expiredObj.object, obj) {
				fbe.CurrentFeedback = entity.FeedbackUnchanged
				uncheckedDuration := now.Sub(expiredObj.expiredInfo.ExpireAt)
				//if unchanged, expire time can update to expireDuration + uncheckedDuration
				fbe.ExpireTime = expiredObj.expiredInfo.ExpireDuration + uncheckedDuration
			}
			result = append(result, fbe)
			continue
		}

		result = append(result, &entity.FeedbackEntry{
			ID:              obj.StringID(),
			DataSourceName:  querierName,
			CurrentFeedback: entity.FeedbackChanged,
			RecentFeedback:  nil,
			GlobalFeedback:  globalData,
			GroupFeedback:   groupData,
			ExpireTime:      0,
		})
	}
	return result, nil
}

func (c *PassiveRefresher) fetchIDFeedback(ctx context.Context,
	client *redis.Client,
	querierName string,
	ids []string) (map[string][]int, error) {

	idDataMap := make(map[string][]int)
	for i := range ids {
		idRaw, err := client.LRange(ctx, idFeedbackPrefix(querierName, ids[i]), 0, entity.FeedbackRecordSize).Result()
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

	cachePairs := make([]interface{}, 0, len(newFeedbacks)*2)
	keys := make([]string, len(newFeedbacks))
	now := time.Now()
	for i := range newFeedbacks {
		expireData := &CacheExpire{
			ID:             newFeedbacks[i].ID,
			ExpireAt:       now.Add(newFeedbacks[i].ExpireTime),
			ExpireDuration: newFeedbacks[i].ExpireTime,
		}
		jsonData, err := json.Marshal(expireData)
		if err != nil {
			log.Error(ctx, "marshal expireData failed",
				log.Err(err))
			continue
		}
		key := idExpirePrefix(newFeedbacks[i].DataSourceName, newFeedbacks[i].ID)
		value := jsonData
		cachePairs = append(cachePairs, key)
		cachePairs = append(cachePairs, value)
		keys[i] = key
	}
	client.MSet(ctx, cachePairs...)
	for i := range keys {
		client.Expire(ctx, keys[i], MaxExpireTime)
	}
}

func (c *PassiveRefresher) fetchExpireTime(ctx context.Context,
	client *redis.Client,
	querierName string,
	ids []string) (map[string]*CacheExpire, error) {
	//expireTime
	if len(ids) < 1 {
		return nil, nil
	}
	keys := c.engine.keyList(querierName, ids, idExpirePrefix)
	expireData, err := client.MGet(ctx, keys...).Result()
	//handle nil
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err), log.Strings("keys", keys))
		return nil, err
	}
	expireDataMap := make(map[string]*CacheExpire)
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
		expireDataMap[expireData.ID] = expireData
	}
	return expireDataMap, nil
}

func (c *PassiveRefresher) fetchGlobalGroupFeedback(ctx context.Context,
	client *redis.Client,
	querierName string) ([]int, []int, error) {
	var globalData []int
	var groupData []int

	globalRaw, err := client.LRange(ctx, constant.KlcGlobalFeedbackPrefix, 0, entity.FeedbackRecordSize).Result()
	if err != redis.Nil {
		if err != nil {
			log.Error(ctx, "Redis LRange global failed",
				log.Err(err))
			return nil, nil, err
		}
		globalData = utils.StringsToInts(ctx, globalRaw)
	}

	groupRaw, err := client.LRange(ctx, constant.KlcGroupFeedbackPrefix+querierName, 0, entity.FeedbackRecordSize).Result()
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
	return constant.KlcIDFeedbackPrefix + querierName + ":" + id
}
func idExpirePrefix(querierName string, id string) string {
	return constant.KlcIDExpirePrefix + querierName + ":" + id
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
			expiredCalculator:  expirecalculator.GetExpireCalculator(),
		}
	})
	return _cachePassiveRefresherEngine
}
