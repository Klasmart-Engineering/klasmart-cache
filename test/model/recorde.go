package model

import (
	"context"
	"encoding/json"
	"gitlab.badanamu.com.cn/calmisland/dbo"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/cache"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/constant"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/entity"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/test/global"
	"strings"
	"sync"
)

type RecordEEntity struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

func (a RecordEEntity) StringID() string {
	return a.ID
}
func (a RecordEEntity) RelatedIDs() []*cache.RelatedEntity {
	return nil
}
func (a RecordEEntity) Equal(o cache.Object) bool {
	return true
}

type RecordEQuerier struct {
}

func (r *RecordEQuerier) ConditionQueryForIDs(ctx context.Context, condition dbo.Conditions, option ...interface{}) ([]string, error) {
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordAList := make([]entity.RecordE, 0)

	err := global.DBClient.Where(paramQuery, params...).Find(&recordAList).Error
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for i := range recordAList {
		result = append(result, recordAList[i].ID)
	}
	return result, nil
}
func (r *RecordEQuerier) QueryByIDs(ctx context.Context, ids []string, option ...interface{}) ([]cache.Object, error) {
	condition := &RecordACondition{
		IDs: ids,
	}
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordEList := make([]entity.RecordE, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordEList).Error
	if err != nil {
		return nil, err
	}
	result := make([]cache.Object, len(recordEList))
	for i := range recordEList {
		result[i] = &RecordEEntity{
			ID:      recordEList[i].ID,
			Title:   recordEList[i].Title,
			Content: recordEList[i].Content,
		}
	}
	return result, nil
}
func (r *RecordEQuerier) UnmarshalObject(ctx context.Context, jsonData string) (cache.Object, error) {
	record := new(RecordEEntity)
	err := json.Unmarshal([]byte(jsonData), record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordEQuerier) Name() string {
	return constant.QuerierE
}

type RecordECondition struct {
	IDs []string
}

func (r *RecordECondition) GetConditions() ([]string, []interface{}) {
	params := make([]string, 0)
	values := make([]interface{}, 0)

	if len(r.IDs) > 0 {
		params = append(params, "id in (?)")
		values = append(values, r.IDs)
	}

	return params, values
}
func (r *RecordECondition) GetPager() *dbo.Pager {
	return nil
}
func (r *RecordECondition) GetOrderBy() string {
	return ""
}

var (
	_recordEQuerier     cache.IDataSource
	_recordEQuerierOnce sync.Once
)

func GetEQuerier() cache.IDataSource {
	_recordEQuerierOnce.Do(func() {
		_recordEQuerier = new(RecordEQuerier)
	})
	return _recordEQuerier
}
