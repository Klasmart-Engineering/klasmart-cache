package expirecalculator

import (
	"context"
	"math"
	"time"

	"github.com/KL-Engineering/kidsloop-cache/entity"
)

type DerivativeTimeExpireCalculator struct {
}

func (f *DerivativeTimeExpireCalculator) Calculate(ctx context.Context, feedback *entity.FeedbackEntry) time.Duration {
	if feedback.Empty() {
		return defaultFirstExpireTime
	}
	dn := feedback.Dn()
	expire := feedback.ExpireTime.Seconds()
	if dn > 2 {
		dn = 2
	} else if dn < -2 {
		dn = -2
	}
	expire = expire * math.Pow(2, float64(dn))

	adjust := 15
	if feedback.CurrentFeedback == 0 {
		expire = expire + float64(adjust)
	} else {
		expire = expire - float64(adjust)
	}

	if expire < 10 {
		expire = 10
	} else if expire > 3600 {
		expire = 600
	}
	return time.Duration(expire) * time.Second
}
