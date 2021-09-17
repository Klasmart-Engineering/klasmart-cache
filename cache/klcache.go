package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/constant"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/statistics"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/utils"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/dbo"
	"gitlab.badanamu.com.cn/calmisland/ro"
)

var (
	ErrUnknownQuerier            = errors.New("unknown querier")
	ErrQuerierUnsupportCondition = errors.New("querier doesn't support condition search")
	ErrInvalidObjectSlice        = errors.New("invalid object slice")
)

const (
	DefaultExpire  = time.Minute * 10
	InfiniteExpire = -1
)

type RelatedEntity struct {
	QuerierName string
	RelatedIDs  []string
}

type IConditionalDataSource interface {
	IDataSource
	ConditionQueryForIDs(ctx context.Context, condition dbo.Conditions, options ...interface{}) ([]string, error)
}

type IDataSource interface {
	QueryByIDs(ctx context.Context, ids []string, options ...interface{}) ([]Object, error)
	Name() string
}

type Object interface {
	StringID() string
	RelatedIDs() []*RelatedEntity
}

type ICacheEngine interface {
	Query(ctx context.Context, dataSourceName string, condition dbo.Conditions, result interface{}, expireTime time.Duration, options ...interface{}) error
	Clean(ctx context.Context, dataSourceName string, ids []string)
	BatchGet(ctx context.Context, dataSourceName string, ids []string, result interface{}, expireTime time.Duration, options ...interface{}) error

	SetExpire(ctx context.Context, duration time.Duration)

	AddQuerier(ctx context.Context, querier IDataSource)
}
type CacheEngine struct {
	querierMap map[string]IDataSource
	expireTime time.Duration
}

func (c *CacheEngine) SetExpire(ctx context.Context, duration time.Duration) {
	c.expireTime = duration
}

func (c *CacheEngine) AddQuerier(ctx context.Context, querier IDataSource) {
	c.querierMap[querier.Name()] = querier
}

func (c *CacheEngine) BatchGet(ctx context.Context, querierName string, ids []string, result interface{}, expireTime time.Duration, options ...interface{}) error {
	s, err := NewReflectObjectSlice(result)
	if err != nil {
		log.Error(ctx, "fail to create object slice", log.Err(err), log.Any("result", result))
		return err
	}
	return c.doBatchGet(ctx, querierName, ids, s, expireTime, options...)
}

func (c *CacheEngine) Clean(ctx context.Context, querierName string, ids []string) {
	c.doubleDelete(ctx, func() {
		err := c.doClean(ctx, querierName, ids)
		if err != nil {
			log.Error(ctx, "doClean failed",
				log.Err(err),
				log.String("querierName", querierName),
				log.String("err", err.Error()),
				log.Strings("ids", ids))
		}

	})
}

func (c *CacheEngine) Query(ctx context.Context, querierName string, condition dbo.Conditions, result interface{}, expireTime time.Duration, options ...interface{}) error {
	querier, exists := c.querierMap[querierName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return ErrUnknownQuerier
	}
	conditionQuerier, ok := querier.(IConditionalDataSource)
	if !ok {
		log.Error(ctx, "Querier doesn't support condition search",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return ErrQuerierUnsupportCondition
	}
	//query by condition for ids
	ids, err := conditionQuerier.ConditionQueryForIDs(ctx, condition, options...)
	if err != nil {
		log.Error(ctx, "GetRedis failed",
			log.Err(err),
			log.Any("condition", condition),
			log.Any("options", options))
		return err
	}

	return c.BatchGet(ctx, querierName, ids, result, expireTime)
}

func (c *CacheEngine) fetchData(ctx context.Context,
	querierName string,
	ids []string,
	result *ReflectObjectSlice, options ...interface{}) ([]Object, error) {
	querier, exists := c.querierMap[querierName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return nil, ErrUnknownQuerier
	}
	client, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "GetRedis failed", log.Err(err))
		return nil, err
	}

	//query from cache
	missingIDs := ids
	if len(ids) > 0 {
		_, missingIDs, err = c.queryForCache(ctx, querier, client, ids, result)
		if err != nil {
			log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
			return nil, err
		}
	}

	missingIDsCount := len(missingIDs)
	allIDsCount := len(ids)

	go statistics.GetHitRatioRecorder().AddHitRatio(ctx, allIDsCount-missingIDsCount, missingIDsCount)
	//all in cache
	if missingIDsCount < 1 {
		log.Info(ctx, "All in cache")
		return nil, nil
	} else if missingIDsCount == allIDsCount {
		log.Info(ctx, "All missing cache",
			log.Strings("all ids", ids))
	} else {
		log.Info(ctx, "Parts in cache",
			log.Strings("missing IDs", missingIDs),
			log.Strings("all ids", ids))
	}

	//query from database
	missingObjs, err := c.batchGetFromDB(ctx, querier, missingIDs, options...)
	if err != nil {
		log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
		return nil, err
	}
	result.Append(missingObjs...)

	c.resort(ctx, ids, result)
	return missingObjs, nil
}

func (c *CacheEngine) doBatchGet(ctx context.Context,
	querierName string,
	ids []string,
	result *ReflectObjectSlice,
	expireTime time.Duration,
	options ...interface{}) error {
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

	missingObjs, err := c.fetchData(ctx, querierName, ids, result, options...)

	//save cache
	go c.saveCache(ctx, querier, client, missingObjs, expireTime)
	return nil
}

func (c *CacheEngine) resort(ctx context.Context, ids []string, result *ReflectObjectSlice) {
	resultMap := make(map[string]Object)
	result.Iterator(func(o Object) {
		resultMap[o.StringID()] = o
	})
	newResult := make([]Object, 0, len(ids))
	for i := range ids {
		obj, exists := resultMap[ids[i]]
		if !exists {
			continue
		}
		newResult = append(newResult, obj)
	}
	result.SetSlice(newResult)
}

func (c *CacheEngine) doClean(ctx context.Context, querierName string, ids []string) error {
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
	if len(ids) > 0 {
		err = client.Del(c.keyList(querier.Name(), ids, c.IDKey)...).Err()
		if err != nil {
			log.Error(ctx, "Del ids failed", log.Err(err), log.Strings("ids", ids))
			return err
		}
	}

	//clean related ids
	err = c.cleanRelatedIDs(ctx, client, querier, ids)
	if err != nil {
		log.Error(ctx, "cleanRelatedIDs failed", log.Err(err), log.Strings("ids", ids))
		return err
	}
	return nil
}

func (c *CacheEngine) keyList(prefix string, ids []string, idMap func(prefix string, id string) string) []string {
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = idMap(prefix, ids[i])
	}
	return keys
}
func (c *CacheEngine) IDKey(querierName string, id string) string {
	return constant.KlcEntryPrefix + querierName + ":" + id
}
func (c *CacheEngine) RelatedIDKey(querierName string, id string) string {
	return constant.KlcRelatedPrefix + querierName + ":" + id
}
func (c *CacheEngine) doubleDelete(ctx context.Context, deleteFunc func()) {
	go func() {
		deleteFunc()
		time.Sleep(time.Second * 5)
	}()
}
func (c *CacheEngine) cleanRelatedIDs(ctx context.Context, client *redis.Client, querier IDataSource, ids []string) error {
	//Query related cache
	keyList := c.keyList(querier.Name(), ids, c.RelatedIDKey)

	cacheRelatedRes := make([]string, 0)
	for i := range keyList {
		tempRes, err := client.SMembers(keyList[i]).Result()
		if err != nil {
			log.Error(ctx, "QueryByIDs failed",
				log.Err(err),
				log.Strings("ids", ids))
			return err
		}
		cacheRelatedRes = append(cacheRelatedRes, tempRes...)
	}

	//Remove related cache
	for i := range cacheRelatedRes {
		res := cacheRelatedRes[i]
		relatedEntity := new(RelatedEntity)
		err := json.Unmarshal([]byte(res), &relatedEntity)
		if err != nil {
			log.Error(ctx, "Unmarshal failed",
				log.Err(err),
				log.String("json", res),
				log.Strings("ids", ids))
			return err
		}
		err = c.doClean(ctx, relatedEntity.QuerierName, relatedEntity.RelatedIDs)
		if err != nil {
			log.Error(ctx, "Clean failed",
				log.Err(err),
				log.String("querierName", relatedEntity.QuerierName),
				log.Strings("relatedIDs", relatedEntity.RelatedIDs))
			return err
		}
	}

	//delete related
	if len(keyList) > 0 {
		err := client.Del(keyList...).Err()
		if err != nil {
			log.Error(ctx, "Del ids failed", log.Err(err), log.Strings("ids", ids))
			return err
		}
	}

	return nil
}
func (c *CacheEngine) batchGetFromDB(ctx context.Context,
	querier IDataSource,
	missingIDs []string, options ...interface{}) ([]Object, error) {
	//query from database segmented
	missingObjs := make([]Object, 0, len(missingIDs))
	utils.SegmentLoop(context.Background(), len(missingIDs), 800, func(start, end int) error {
		segmentObjs, err := querier.QueryByIDs(ctx, missingIDs[start:end], options...)
		if err != nil {
			log.Error(ctx, "QueryByIDs failed",
				log.Err(err),
				log.Strings("missingIDs", missingIDs),
				log.Any("options", options))
			return err
		}
		missingObjs = append(missingObjs, segmentObjs...)
		return nil
	})

	return missingObjs, nil
}

func (c *CacheEngine) queryForCache(ctx context.Context,
	querier IDataSource,
	client *redis.Client,
	ids []string,
	result *ReflectObjectSlice) ([]string, []string, error) {
	missingIDs := make([]string, 0, len(ids))
	hitIDs := make([]string, 0, len(ids))
	cacheRes, err := client.MGet(c.keyList(querier.Name(), ids, c.IDKey)...).Result()
	if err == redis.Nil {
		//handle nil
		fmt.Println("Nil")
		missingIDs = ids
	} else {
		if err != nil {
			log.Error(ctx, "GetRedis failed", log.Err(err))
			return nil, nil, err
		}
		for i := range cacheRes {
			res, ok := cacheRes[i].(string)
			if !ok {
				continue
			}
			obj := result.NewElement()
			err = json.Unmarshal([]byte(res), &obj)
			if err != nil {
				log.Error(ctx, "UnmarshalObject failed",
					log.Err(err),
					log.String("res", res))
				return nil, nil, err
			}
			result.Append(obj)
		}

		//get missing ids
		for i := range ids {
			if !c.containsInObjects(ctx, *result, ids[i]) {
				missingIDs = append(missingIDs, ids[i])
			} else {
				hitIDs = append(hitIDs, ids[i])
			}
		}
	}
	return hitIDs, missingIDs, nil
}

type ObjectRelatedIDs struct {
	ID         string
	RelatedIDs []*RelatedEntity
}
type PrepareSavingRelatedIDs struct {
	querierName     string
	querierObjectID string
	relatedIDs      []string
}

func (c *CacheEngine) saveRelatedIDs(ctx context.Context,
	client *redis.Client,
	relatedRecords []*ObjectRelatedIDs,
	expireAt time.Time,
	infinite bool) {
	//rebuild structure
	//relatedIDMap is map[querierName][objectID][relatedIDs]
	relatedIDMap := make(map[string]map[string][]interface{})
	for i := range relatedRecords {
		for j := range relatedRecords[i].RelatedIDs {
			relatedIDMap = c.handleRelatedEntity(ctx, relatedRecords[i].ID, relatedRecords[i].RelatedIDs[j], relatedIDMap)
		}
	}

	for querierName, objectMap := range relatedIDMap {
		for objectID, relatedIDs := range objectMap {
			key := c.RelatedIDKey(querierName, objectID)
			client.SAdd(key, relatedIDs...)
			if !infinite {
				client.ExpireAt(key, expireAt)
			}
		}
	}
}
func (c *CacheEngine) handleRelatedEntity(ctx context.Context,
	objectID string,
	relatedEntity *RelatedEntity,
	relatedIDMap map[string]map[string][]interface{}) map[string]map[string][]interface{} {
	for i := range relatedEntity.RelatedIDs {
		querierNameMap, exist := relatedIDMap[relatedEntity.QuerierName]
		if !exist {
			querierNameMap = make(map[string][]interface{})
		}
		querierIDMap, exist := querierNameMap[relatedEntity.RelatedIDs[i]]
		if !exist {
			querierIDMap = make([]interface{}, 0)
		}
		querierIDMap = append(querierIDMap, objectID)

		querierNameMap[relatedEntity.RelatedIDs[i]] = querierIDMap
		relatedIDMap[relatedEntity.QuerierName] = querierNameMap
	}
	return relatedIDMap
}

func (c *CacheEngine) saveCache(ctx context.Context,
	querier IDataSource,
	client *redis.Client,
	missingObjs []Object,
	expireTime time.Duration) {
	//save cache
	cachePairs := make([]interface{}, len(missingObjs)*2)
	relatedRecords := make([]*ObjectRelatedIDs, 0)

	//get Expire
	now := time.Now()
	expireAt := now.Add(c.expireTime)
	infinite := false
	if expireTime > 0 {
		expireAt = now.Add(expireTime)
	}
	if expireTime == -1 {
		infinite = true
	}

	keys := make([]string, len(missingObjs))
	for i := range missingObjs {
		jsonData, err := json.Marshal(missingObjs[i])
		if err != nil {
			log.Error(ctx, "Marshal data failed", log.Err(err), log.Any("missingObj", missingObjs[i]))
			continue
		}
		//record := c.collectObjectRelatedIDs(ctx, missingObjs[i])
		relatedRecords = append(relatedRecords, &ObjectRelatedIDs{
			ID:         missingObjs[i].StringID(),
			RelatedIDs: missingObjs[i].RelatedIDs(),
		})
		key := c.IDKey(querier.Name(), missingObjs[i].StringID())
		cachePairs[i*2] = key
		cachePairs[i*2+1] = jsonData

		keys[i] = key
	}
	client.MSet(cachePairs...)
	if !infinite {
		for i := range keys {
			client.ExpireAt(keys[i], expireAt)
		}
	}

	//save related ids
	c.saveRelatedIDs(ctx, client, relatedRecords, expireAt, infinite)
}
func (c *CacheEngine) containsInObjects(Octx context.Context, objs ReflectObjectSlice, id string) bool {
	flag := false
	objs.Iterator(func(o Object) {
		if o.StringID() == id {
			flag = true
		}
	})
	return flag
}

var (
	_cacheEngine     *CacheEngine
	_cacheEngineOnce sync.Once
)

func GetCacheEngine() *CacheEngine {
	_cacheEngineOnce.Do(func() {
		_cacheEngine = &CacheEngine{
			querierMap: make(map[string]IDataSource),
			expireTime: DefaultExpire,
		}
	})
	return _cacheEngine
}
