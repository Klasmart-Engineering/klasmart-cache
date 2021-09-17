package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/cache"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/constant"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/global"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/model"
	"gitlab.badanamu.com.cn/calmisland/ro"
)

const connStr = "root:Badanamu123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"

var (
	nameLikes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randChar() string {
	index := rand.Int() % len(nameLikes)
	return fmt.Sprintf("%v", string(nameLikes[index]))
}
func queryAByDB(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		ids, err := model.GetAQuerier().ConditionQueryForIDs(ctx, &model.RecordACondition{
			NameLike: prefix,
		})
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
		_, err = model.GetAQuerier().QueryByIDs(ctx, ids)
		if err != nil {
			fmt.Println("query data failed, err:", err)
			continue
		}
	}
}

func queryBByDB(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		ids, err := model.GetBQuerier().ConditionQueryForIDs(ctx, &model.RecordBCondition{
			NameLike: prefix,
		})
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
		res, err := model.GetBQuerier().QueryByIDs(ctx, ids)
		if err != nil {
			fmt.Println("query data failed, err:", err)
			continue
		}
		fmt.Println("res:", res)
	}
}

func queryCByDB(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		ids, err := model.GetCQuerier().ConditionQueryForIDs(ctx, &model.RecordCCondition{
			NameLike: prefix,
		})
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
		res, err := model.GetCQuerier().QueryByIDs(ctx, ids)
		if err != nil {
			fmt.Println("query data failed, err:", err)
			continue
		}
		fmt.Println("res:", res)
	}
}

func queryAByCache(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		res := make([]model.RecordAEntity, 0)
		err := cache.GetCacheEngine().Query(ctx, constant.QuerierA, &model.RecordACondition{
			NameLike: prefix,
		}, &res, time.Second*20)
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
	}
}

func queryBByCache(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		res := make([]cache.Object, 0)
		err := cache.GetCacheEngine().Query(ctx, constant.QuerierB, &model.RecordBCondition{
			NameLike: prefix,
		}, &res, cache.DefaultExpire)
		fmt.Println("res:", res)
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
	}
}

func queryCByCache(ctx context.Context, prefix string) {
	for i := 0; i < 1; i++ {
		res := make([]cache.Object, 0)
		err := cache.GetCacheEngine().Query(ctx, constant.QuerierC, &model.RecordCCondition{
			NameLike: prefix,
		}, &res, cache.DefaultExpire)
		fmt.Println("res:", res)
		if err != nil {
			fmt.Println("query failed, err:", err)
			continue
		}
	}
}

func calculateTime(ctx context.Context, title string, prefix string, function func(ctx context.Context, prefix string)) int64 {
	startAt := time.Now()
	function(ctx, prefix)
	endAt := time.Now()
	duration := endAt.Sub(startAt)
	return int64(duration)
}

func initQuerier(ctx context.Context) {
	cache.GetCacheEngine().AddQuerier(ctx, model.GetAQuerier())
	cache.GetCacheEngine().AddQuerier(ctx, model.GetBQuerier())
	cache.GetCacheEngine().AddQuerier(ctx, model.GetCQuerier())
	cache.GetCacheEngine().AddQuerier(ctx, model.GetDQuerier())
	cache.GetCacheEngine().AddQuerier(ctx, model.GetEQuerier())
}

func clearCache(ctx context.Context) {
	client, err := ro.GetRedis(ctx)
	if err != nil {
		panic(err)
	}
	res, _ := client.Keys("klc:cache:*").Result()
	client.Del(res...)
}

func calculateAvgTime(ctx context.Context, queryByCache, queryByDB func(ctx context.Context, prefix string)) {
	noCacheTime := int64(0)
	partCacheTime := int64(0)
	allCacheTime := int64(0)
	dbTime := int64(0)
	for i := 0; i < 10; i++ {
		noCacheTime = noCacheTime + calculateTime(ctx, "query no cache B", "B", queryByCache)
		time.Sleep(time.Second)
		clearCache(ctx)

		//query Ba to cache parts
		//save Ba as cache
		queryByCache(ctx, "Ba")
		time.Sleep(time.Second)

		partCacheTime = partCacheTime + calculateTime(ctx, "query parts cache B", "B", queryByCache)
		time.Sleep(time.Second)

		allCacheTime = allCacheTime + calculateTime(ctx, "query cache B", "B", queryByCache)
		dbTime = dbTime + calculateTime(ctx, "query db B", "B", queryByDB)

		clearCache(ctx)
	}

	fmt.Println("No cache time:", time.Duration(noCacheTime))
	fmt.Println("Parts cache time:", time.Duration(partCacheTime))
	fmt.Println("All cache time:", time.Duration(allCacheTime))
	fmt.Println("DB time:", time.Duration(dbTime))
}

func test1(ctx context.Context) {
	calculateAvgTime(ctx, queryAByCache, queryAByDB)
}
func test2(ctx context.Context) {
	calculateAvgTime(ctx, queryBByCache, queryBByDB)
}
func test3(ctx context.Context) {
	calculateAvgTime(ctx, queryCByCache, queryCByDB)
}
func test4(ctx context.Context) {
	ids, err := model.GetAQuerier().ConditionQueryForIDs(ctx, &model.RecordACondition{
		NameLike: "e",
	})
	if err != nil {
		panic(err)
	}
	mid := len(ids) / 2
	dbIDs := ids[:mid]
	cacheIDs := ids[mid:]
	fmt.Println("DB len:", len(dbIDs))
	fmt.Println("Cache len:", len(cacheIDs))

	startAt := time.Now()
	err = model.GetAQuerier().DeleteByID(ctx, dbIDs)
	if err != nil {
		fmt.Println("Remove failed")
		panic(err)
	}
	endAt := time.Now()
	fmt.Println("DB remove spend ", endAt.Sub(startAt))

	startAt = time.Now()
	err = model.GetAQuerier().DeleteByID(ctx, cacheIDs)
	if err != nil {
		fmt.Println("Remove failed")
		panic(err)
	}
	cache.GetCacheEngine().Clean(ctx, constant.QuerierA, cacheIDs)
	endAt = time.Now()
	fmt.Println("Cache remove spend ", endAt.Sub(startAt))

	time.Sleep(time.Second * 2)
}

func main() {
	ro.SetConfig(&redis.Options{Addr: "127.0.0.1:6379"})
	db, err := gorm.Open("mysql", connStr)
	if err != nil {
		panic(err)
	}
	//db.LogMode(true)

	ctx := context.Background()
	global.DBClient = db
	initQuerier(ctx)

	test1(ctx)
}
