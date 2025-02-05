package statistics

import (
	"context"
	"time"

	"github.com/KL-Engineering/common-log/log"
	"github.com/KL-Engineering/kidsloop-cache/constant"
	"github.com/KL-Engineering/ro"
)

type HitRatioResponse struct {
	HitCount  int `json:"hit_count"`
	MissCount int `json:"miss_count"`

	Ratio float64 `json:"ratio"`
}

type HitRatioRecorder struct {
}

func (h *HitRatioRecorder) GetCurrentHitRatio(ctx context.Context) *HitRatioResponse {
	hitKey := h.getRedisKey(ctx, constant.KlcHitCachePrefix)
	missKey := h.getRedisKey(ctx, constant.KlcMissCachePrefix)

	redis, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "Can't connect to redis", log.Err(err))
		return nil
	}
	hit, err := redis.Get(ctx, hitKey).Int()
	if err != nil {
		hit = 0
		log.Warn(ctx, "Get hit count failed", log.Err(err))
	}

	miss, err := redis.Get(ctx, missKey).Int()
	if err != nil {
		miss = 0
		log.Warn(ctx, "Get miss count failed", log.Err(err))
	}
	return h.calculateRatio(ctx, hit, miss)
}

func (h *HitRatioRecorder) AddHitRatio(ctx context.Context, hitCount, missingCount int) {
	redis, err := ro.GetRedis(ctx)
	if err != nil {
		log.Error(ctx, "Can't connect to redis", log.Err(err))
		return
	}

	//init key/value
	hitKey := h.getRedisKey(ctx, constant.KlcHitCachePrefix)
	missKey := h.getRedisKey(ctx, constant.KlcMissCachePrefix)
	log.Debug(ctx, "add hit ratio",
		log.Int("hitCount", hitCount),
		log.Int("missingCount", missingCount),
		log.String("hitKey", hitKey),
		log.String("missKey", missKey))
	err = redis.SetNX(ctx, hitKey, "0", 0).Err()
	if err != nil {
		log.Warn(ctx, "Set redis hit key failed", log.Err(err))
	}
	err = redis.SetNX(ctx, missKey, "0", 0).Err()
	if err != nil {
		log.Warn(ctx, "Set redis miss key failed", log.Err(err))
	}

	err = redis.IncrBy(ctx, hitKey, int64(hitCount)).Err()
	if err != nil {
		log.Error(ctx, "Add redis hit count failed", log.Err(err))
		return
	}
	err = redis.IncrBy(ctx, missKey, int64(missingCount)).Err()
	if err != nil {
		log.Error(ctx, "Add redis miss count failed", log.Err(err))
		return
	}
}

func (h *HitRatioRecorder) calculateRatio(ctx context.Context, hit int, miss int) *HitRatioResponse {
	total := hit + miss
	if total == 0 {
		return &HitRatioResponse{
			HitCount:  0,
			MissCount: 0,
			Ratio:     0,
		}
	}
	return &HitRatioResponse{
		HitCount:  hit,
		MissCount: miss,
		Ratio:     float64(hit) / float64(total),
	}
}

func (h *HitRatioRecorder) getRedisKey(ctx context.Context, prefix string) string {
	return h.getRedisKeyByTime(ctx, prefix, time.Now())
}
func (h *HitRatioRecorder) getRedisKeyByTime(ctx context.Context, prefix string, t time.Time) string {
	return prefix + t.Format("200601")
}

func GetHitRatioRecorder() *HitRatioRecorder {
	return new(HitRatioRecorder)
}
