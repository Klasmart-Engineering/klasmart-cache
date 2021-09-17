package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/cache"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/statistics"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/global"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/model"
	"gitlab.badanamu.com.cn/calmisland/ro"
	"time"
)

const connStr = "root:Badanamu123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"

var ids = []string{
	"aCkDOE139998772582481b",
	"acnayq3dd143fb0d058727",
	"acqQfE2e383b83a869942b",
	"aCVoPV5d241e8d5c7ade0e",
	"adCCayd040201f7c5ea57",
	"AdEgcG457142577e1b9d49",
	"AdjLQg57e3b2b0316475d6",
	"aDPZVm72ef2acee8a50623",
	"adRVYyda4f2b71a8bff9d",
	"aDTAya636f291178727438",
	"AdUdgx11c6563227e20368",
	"AdUfCU3652d84d16ab9f07",
	"adwjbu4faf46198ea1f39c",
	"adwTBt3404b27b7bf19bbc",
	"AEDQQc41f3fa3a1c940263",
	"AeeZJj6fde0ef8ca8a14f6",
	"aEnciK5f9a631b6f1bd5dc",
	"aeOUsG2dc8f91eeae5df5e",
	"AeMRlu6ae354371a585922",
	"AESNKQ6f87ee53bc50635",
	"AESOag6253ba26d56e0778",
	"aeToKf7dbb8cb8afa34bde",
	"aEunZY44082e35a628756",
}

func initQuerier(ctx context.Context) {
	cache.GetCacheEngine().AddDataSource(ctx, model.GetAQuerier())
	cache.GetCacheEngine().AddDataSource(ctx, model.GetBQuerier())
	cache.GetCacheEngine().AddDataSource(ctx, model.GetCQuerier())
	cache.GetCacheEngine().AddDataSource(ctx, model.GetDQuerier())
	cache.GetCacheEngine().AddDataSource(ctx, model.GetEQuerier())
}

func test(ctx context.Context) {
	entities := make([]*model.RecordAEntity, 0)
	for i := 0; i < 1; i++ {
		err := cache.GetPassiveCacheRefresher().BatchGet(ctx, model.GetAQuerier().Name(), ids, &entities)
		if err != nil {
			panic(err)
		}
		err = cache.GetPassiveCacheRefresher().BatchGet(ctx, model.GetAQuerier().Name(), ids, &entities)
		if err != nil {
			panic(err)
		}
	}
	for i := range entities {
		fmt.Printf("%#v \n", entities[i])
	}

}

func main() {
	ro.SetConfig(&redis.Options{Addr: "127.0.0.1:6379"})
	db, err := gorm.Open("mysql", connStr)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	global.DBClient = db
	initQuerier(ctx)

	test(ctx)
	time.Sleep(time.Second * 4)

	//cache.GetCacheEngine().Clean(ctx, constant.QuerierA, ids)
	//time.Sleep(time.Second * 2)
	fmt.Printf("%#v\n", statistics.GetHitRatioRecorder().GetCurrentHitRatio(ctx))

}
