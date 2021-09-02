package expirecalculator

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"time"
)

type SimpleExpireTimeCalculator struct {
}

func (f *SimpleExpireTimeCalculator) Calculate(ctx context.Context, feedback *entity.FeedbackEntry) time.Duration {
	if feedback.Empty() {
		return defaultFirstExpireTime
	}
	output := time.Duration(0)
	adjust := time.Second * 30
	if feedback.CurrentFeedback == 0 {
		output = feedback.ExpireTime + adjust
	} else {
		output = feedback.ExpireTime - adjust
	}
	if output < 1 {
		output = time.Second * 5
	}
	return output

}
