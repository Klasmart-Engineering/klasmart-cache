package expirecalculator

import (
	"context"
	"math"
	"time"

	"github.com/KL-Engineering/kidsloop-cache/entity"
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
	inte0, inte1 := feedback.In()
	amend := inte0
	if diff > 0 {
		amend = inte1
	}
	if diff != 0 {
		if amend == 0 {
			amend = 1
		}
		diff = diff / float64(amend)
		newExpire = expire + diff
	}

	adjust := 15
	if feedback.CurrentFeedback == 0 {
		newExpire = newExpire + float64(adjust)
	} else {
		newExpire = newExpire - float64(adjust)
	}

	return time.Duration(newExpire) * time.Second
}
