package expirecalculator

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"math"
	"time"
)

type IntegrateDerivativeTimeExpireCalculator struct {
}

func (f *IntegrateDerivativeTimeExpireCalculator) Calculate(ctx context.Context, feedback *entity.FeedbackEntry) time.Duration {
	if feedback.Empty() {
		return defaultFirstExpireTime
	}
	dn := -feedback.Dn()
	expire := feedback.ExpireTime.Seconds()
	if dn > 2 {
		dn = 2
	} else if dn < -2 {
		dn = -2
	}
	newExpire := expire * math.Pow(2, float64(dn))

	//if we need to decrease frequency
	diff := newExpire - expire
	if diff != 0 {
		_, inte1 := feedback.In()
		if inte1 == 0 {
			inte1 = 1
		}
		diff = diff / float64(inte1)
		newExpire = expire + diff
	}

	adjust := 15
	if feedback.CurrentFeedback == 0 {
		newExpire = newExpire + float64(adjust)
	} else {
		newExpire = newExpire - float64(adjust)
	}

	if newExpire < 10 {
		newExpire = 10
	} else if newExpire > 3600 {
		newExpire = 600
	}
	return time.Duration(newExpire) * time.Second
}
