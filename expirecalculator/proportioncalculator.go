package expirecalculator

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"time"
)

type ProportionTimeExpireCalculator struct {
}

func (f *ProportionTimeExpireCalculator) Calculate(ctx context.Context, feedback *entity.FeedbackEntry) time.Duration {
	if feedback.Empty() {
		return defaultFirstExpireTime
	}
	expireInt := int64(feedback.ExpireTime)
	newExpire := int64(3-2*feedback.CurrentFeedback) * expireInt / 2
	return time.Duration(newExpire)
}
