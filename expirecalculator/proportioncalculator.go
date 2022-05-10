package expirecalculator

import (
	"context"
	"time"

	"github.com/KL-Engineering/kidsloop-cache/entity"
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
