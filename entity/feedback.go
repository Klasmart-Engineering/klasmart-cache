package entity

import (
	"time"
)

const (
	FeedbackRecordSize = 20
)

type FeedbackEntry struct {
	ID              string
	QuerierName     string
	CurrentFeedback int
	RecentFeedback  []int

	GlobalFeedback []int
	GroupFeedback  []int
	ExpireTime     time.Duration
}

func (f FeedbackEntry) Empty() bool {
	if len(f.RecentFeedback) == 0 {
		return true
	}
	return false
}

func (f FeedbackEntry) In() (int, int) {
	res0 := 0
	res1 := 0
	for i := range f.RecentFeedback {
		if f.RecentFeedback[i] == 0 {
			res0++
		} else {
			res1++
		}
	}
	return res0, res1
}
func (f FeedbackEntry) Dn() int {
	if len(f.RecentFeedback) <= 1 {
		return 0
	}
	res := 0
	for i := range f.RecentFeedback {
		if f.CurrentFeedback == f.RecentFeedback[i] {
			res = res + 1
		} else {
			break
		}
	}
	if f.CurrentFeedback == 0 {
		res = -res
	}

	return res
}
func (f FeedbackEntry) Gn() int {
	res := 0
	for i := range f.GlobalFeedback {
		res = res + f.GlobalFeedback[i]
	}
	return res
}

func (f FeedbackEntry) Cn() int {
	res := 0
	for i := range f.GroupFeedback {
		res = res + f.GroupFeedback[i]
	}
	return res
}

type FeedbackRecordEntry struct {
	ID              string
	QuerierName     string
	CurrentFeedback int

	ExpireTime time.Duration
}
