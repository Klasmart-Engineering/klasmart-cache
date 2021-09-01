package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

const (
	defaultExpire = time.Minute * 10

	klcEntryPrefix   = "klc:cache:entry:"
	klcRelatedPrefix = "klc:cache:related:"
)

type RelatedEntity struct {
	QuerierName string
	RelatedIDs  []string
}

type IConditionQuerier interface {
	IQuerier
	QueryForIDs(ctx context.Context, condition dbo.Conditions) ([]string, error)
}

type IQuerier interface {
	BatchGet(ctx context.Context, ids []string) ([]Object, error)
	UnmarshalObject(ctx context.Context, jsonData string) (Object, error)

	ID() string
}

type Object interface {
	StringID() string
	RelatedIDs() []*RelatedEntity
	Equal(o Object) bool
}

type ICacheEngine interface {
	Query(ctx context.Context, querierName string, condition dbo.Conditions, result *[]Object) error
	Clean(ctx context.Context, querierName string, ids []string)
	BatchGet(ctx context.Context, querierName string, ids []string, result *[]Object) error

	SetExpire(ctx context.Context, duration time.Duration)

	AddQuerier(ctx context.Context, querier IQuerier)
}
type CacheEngine struct {
	querierMap map[string]IQuerier
	expireTime time.Duration
}

func (c *CacheEngine) SetExpire(ctx context.Context, duration time.Duration) {
	c.expireTime = duration
}

func (c *CacheEngine) AddQuerier(ctx context.Context, querier IQuerier) {
	c.querierMap[querier.ID()] = querier
}

func (c *CacheEngine) BatchGet(ctx context.Context, querierName string, ids []string, result *[]Object) error {
	return c.doBatchGet(ctx, querierName, ids, result)
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

func (c *CacheEngine) Query(ctx context.Context, querierName string, condition dbo.Conditions, result *[]Object) error {
	querier, exists := c.querierMap[querierName]
	if !exists {
		log.Error(ctx, "GetRedis failed",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return ErrUnknownQuerier
	}
	conditionQuerier, ok := querier.(IConditionQuerier)
	if !ok {
		log.Error(ctx, "Querier doesn't support condition search",
			log.String("querierName", querierName),
			log.Any("querierMap", c.querierMap))
		return ErrQuerierUnsupportCondition
	}
	//query by condition for ids
	ids, err := conditionQuerier.QueryForIDs(ctx, condition)
	if err != nil {
		log.Error(ctx, "GetRedis failed",
			log.Err(err),
			log.Any("condition", condition))
		return err
	}

	return c.BatchGet(ctx, querierName, ids, result)
}

func (c *CacheEngine) fetchData(ctx context.Context,
	querierName string,
	ids []string,
	result *[]Object) ([]Object, error) {
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

	//allocate space
	*result = make([]Object, 0, len(ids))
	//query from cache
	missingIDs := ids
	if len(ids) > 0 {
		_, missingIDs, err = c.queryForCache(ctx, querier, client, ids, result)
		if err != nil {
			log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
			return nil, err
		}
	}
	//all in cache
	if len(missingIDs) < 1 {
		log.Info(ctx, "All in cache")
		return nil, nil
	} else if len(missingIDs) == len(ids) {
		log.Info(ctx, "All missing cache",
			log.Strings("all ids", ids))
	} else {
		log.Info(ctx, "Parts in cache",
			log.Strings("missing IDs", missingIDs),
			log.Strings("all ids", ids))
	}
	//query from database
	missingObjs, err := c.batchGetFromDB(ctx, querier, missingIDs)
	if err != nil {
		log.Error(ctx, "queryForCache failed", log.Err(err), log.Strings("ids", ids))
		return nil, err
	}
	*result = append(*result, missingObjs...)

	*result = c.resort(ctx, ids, *result)

	return missingObjs, nil
}

func (c *CacheEngine) doBatchGet(ctx context.Context, querierName string, ids []string, result *[]Object) error {
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

	missingObjs, err := c.fetchData(ctx, querierName, ids, result)

	//save cache
	go c.saveCache(ctx, querier, client, missingObjs, 0)
	return nil
}

func (c *CacheEngine) resort(ctx context.Context, ids []string, result []Object) []Object {
	resultMap := make(map[string]Object)
	for i := range result {
		resultMap[result[i].StringID()] = result[i]
	}
	newResult := make([]Object, 0, len(result))
	for i := range ids {
		obj, exists := resultMap[ids[i]]
		if !exists {
			continue
		}
		newResult = append(newResult, obj)
	}

	return newResult
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
		err = client.Del(c.keyList(querier.ID(), ids, c.IDKey)...).Err()
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
	return klcEntryPrefix + querierName + ":" + id
}
func (c *CacheEngine) RelatedIDKey(querierName string, id string) string {
	return klcRelatedPrefix + querierName + ":" + id
}
func (c *CacheEngine) doubleDelete(ctx context.Context, deleteFunc func()) {
	go func() {
		deleteFunc()
		time.Sleep(time.Second * 5)
	}()
}
func (c *CacheEngine) cleanRelatedIDs(ctx context.Context, client *redis.Client, querier IQuerier, ids []string) error {
	//Query related cache
	keyList := c.keyList(querier.ID(), ids, c.RelatedIDKey)

	cacheRelatedRes := make([]string, 0)
	for i := range keyList {
		tempRes, err := client.SMembers(keyList[i]).Result()
		if err != nil {
			log.Error(ctx, "BatchGet failed",
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
	querier IQuerier,
	missingIDs []string) ([]Object, error) {
	//query from database segmented
	missingObjs := make([]Object, 0, len(missingIDs))
	SegmentLoop(context.Background(), len(missingIDs), 800, func(start, end int) error {
		segmentObjs, err := querier.BatchGet(ctx, missingIDs[start:end])
		if err != nil {
			log.Error(ctx, "BatchGet failed", log.Err(err), log.Strings("missingIDs", missingIDs))
			return err
		}
		missingObjs = append(missingObjs, segmentObjs...)
		return nil
	})

	return missingObjs, nil
}

func (c *CacheEngine) queryForCache(ctx context.Context,
	querier IQuerier,
	client *redis.Client,
	ids []string,
	result *[]Object) ([]string, []string, error) {
	missingIDs := make([]string, 0, len(ids))
	hitIDs := make([]string, 0, len(ids))
	cacheRes, err := client.MGet(c.keyList(querier.ID(), ids, c.IDKey)...).Result()
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
			obj, err := querier.UnmarshalObject(ctx, res)
			if err != nil {
				log.Error(ctx, "UnmarshalObject failed",
					log.Err(err),
					log.String("res", res))
				return nil, nil, err
			}
			*result = append(*result, obj)
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
	querier IQuerier,
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
		key := c.IDKey(querier.ID(), missingObjs[i].StringID())
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
func (c *CacheEngine) containsInObjects(Octx context.Context, objs []Object, id string) bool {
	for i := range objs {
		if objs[i].StringID() == id {
			return true
		}
	}
	return false
}

var (
	_cacheEngine     *CacheEngine
	_cacheEngineOnce sync.Once
)

func GetCacheEngine() *CacheEngine {
	_cacheEngineOnce.Do(func() {
		_cacheEngine = &CacheEngine{
			querierMap: make(map[string]IQuerier),
			expireTime: defaultExpire,
		}
	})
	return _cacheEngine
}
