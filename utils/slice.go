package utils

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"strconv"
)

func StringsToInts(ctx context.Context, str []string) []int {
	res := make([]int, 0, len(str))
	for i := range str {
		d, err := strconv.Atoi(str[i])
		if err != nil {
			log.Warn(ctx, "parse id failed",
				log.Err(err),
				log.String("str", str[i]))
			continue
		}
		res = append(res, d)
	}
	return res
}
