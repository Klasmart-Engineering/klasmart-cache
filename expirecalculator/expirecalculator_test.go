package expirecalculator

import (
	"context"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/entity"
	"math"
	"math/rand"
	"testing"
	"time"
)

var (
	curTime   = time.Duration(0)
	lastIndex = 0
	times     = 1000
)

func fixedValues(val time.Duration, size int) []time.Duration {
	res := make([]time.Duration, size)
	for i := 0; i < size; i++ {
		res[i] = val
	}
	return res
}
func randVal(val []time.Duration, maxSec int) []time.Duration {
	for i := range val {
		nextRand := rand.Int63n(int64(maxSec))
		val[i] = time.Duration(nextRand) * time.Second
	}
	return val
}

func randOffset(val []time.Duration) []time.Duration {
	for i := range val {
		data := int64(val[i])
		d := rand.Float64() * float64(data)
		val[i] = val[i] + time.Duration(d)
	}

	return val
}

func sinWave(val []time.Duration) []time.Duration {
	for i := range val {
		data := int64(val[i])
		d := math.Sin(float64(i)) * float64(data)
		val[i] = val[i] + time.Duration(d)
	}

	return val
}

func increase(val []time.Duration, val0 time.Duration) []time.Duration {
	for i := range val {
		val[i] = val[i] + val0*time.Duration(i)
	}
	return val
}
func decrease(val []time.Duration, val0 time.Duration) []time.Duration {
	for i := range val {
		val[i] = val[i] - val0*time.Duration(i)
		if val[i] < time.Second {
			val[i] = time.Second
		}

	}
	return val
}

func squareWave(val []time.Duration, period int, lowStart int) []time.Duration {
	for i := range val {
		if i%period > lowStart {
			val[i] = val[i] * 10
		}
	}
	return val
}

func fakeUpdateCache(dur time.Duration, sequence []time.Duration) int {
	curTime = curTime + dur
	i := 0
	flag := false
	for ; i < len(sequence[lastIndex:]); i++ {
		if curTime > sequence[lastIndex+i] {
			lastIndex = lastIndex + 1
			flag = true
		} else {
			break
		}
	}
	if flag {
		return 1
	}

	return 0
}
func testCalculator(t *testing.T, calculator IExpireCalculator, frequance []time.Duration) {
	ctx := context.Background()

	fb := &entity.FeedbackEntry{
		ID:              "test",
		CurrentFeedback: 0,
	}

	freqTime := make([]int, len(frequance)+1)
	freqTime[0] = 0
	for i := range frequance {
		freqTime[i+1] = freqTime[i] + int(frequance[i].Seconds())
	}

	actualIndex := 0
	rangeEnd := int((time.Hour * 4).Seconds())
	cacheTimes := time.Duration(0)
	isUpdated := 0
	for i := 0; i <= rangeEnd; i += 5 {
		cacheUpdate := 0
		actualUpdate := 0
		//缓存过期
		if int(cacheTimes.Seconds()) < i {
			//重新计算缓存
			cacheFreq := calculator.Calculate(ctx, fb)
			cacheTimes = cacheTimes + cacheFreq

			fb.RecentFeedback = append(fb.RecentFeedback, fb.CurrentFeedback)
			fb.CurrentFeedback = isUpdated
			fb.ExpireTime = cacheFreq

			//更新完成
			cacheUpdate = 1
			isUpdated = 0

		}
		//数据库更新
		if freqTime[actualIndex] < i {
			actualIndex++
			isUpdated = 1
			actualUpdate = 1
		}

		//isUpdated = fakeUpdateCache(cacheFreq, frequance)
		fmt.Printf("%v,  %v,  %v \n", i, cacheUpdate, actualUpdate)
	}
}
func testCalculatorFreq(t *testing.T, calculator IExpireCalculator, frequance []time.Duration) {
	ctx := context.Background()

	fb := &entity.FeedbackEntry{
		ID:              "test",
		CurrentFeedback: 0,
	}
	freqTime := make([]time.Duration, len(frequance)+1)
	freqTime[0] = 0
	for i := range frequance {
		freqTime[i+1] = freqTime[i] + frequance[i]
	}

	isUpdated := 0
	for i := range frequance {
		//重新计算缓存
		cacheFreq := calculator.Calculate(ctx, fb)
		fb.RecentFeedback = append([]int{fb.CurrentFeedback}, fb.RecentFeedback...)
		fb.CurrentFeedback = isUpdated
		fb.ExpireTime = cacheFreq

		//更新完成
		isUpdated = fakeUpdateCache(cacheFreq, freqTime)

		fmt.Printf("%v, %v, %v \n", i, frequance[i].Seconds(), cacheFreq.Seconds())
	}

}

func TestSimpleTimeExpireCalculator(t *testing.T) {
	calculator := new(IntegrateDerivativeTimeExpireCalculator)
	frequance := fixedValues(1*time.Minute, times)
	frequance = squareWave(frequance, 100, 30)
	testCalculator(t, calculator, frequance)
}
