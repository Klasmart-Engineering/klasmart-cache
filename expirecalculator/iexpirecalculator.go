package expirecalculator

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"sync"
	"time"
)

const (
	defaultFirstExpireTime = time.Second * 10
)

type IExpireCalculator interface {
	Calculate(ctx context.Context, feedback *entity.FeedbackEntry) time.Duration
}

var (
	_calculator     IExpireCalculator
	_calculatorOnce sync.Once
)

func GetExpireCalculator() IExpireCalculator {
	_calculatorOnce.Do(func() {
		_calculator = new(IntegrateDerivativeTimeExpireCalculator)
	})
	return _calculator
}
