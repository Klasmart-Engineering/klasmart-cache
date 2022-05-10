package expirecalculator

import (
	"context"
	"sync"
	"time"

	"github.com/KL-Engineering/kidsloop-cache/entity"
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
