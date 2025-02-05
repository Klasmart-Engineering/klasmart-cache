package model

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/KL-Engineering/dbo"
	"github.com/KL-Engineering/kidsloop-cache/cache"
	"github.com/KL-Engineering/kidsloop-cache/test/constant"
	"github.com/KL-Engineering/kidsloop-cache/test/entity"
	"github.com/KL-Engineering/kidsloop-cache/test/global"
)

type RecordCEntity struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	RealName string `json:"real_name"`
}

func (a RecordCEntity) StringID() string {
	return a.ID
}
func (a RecordCEntity) RelatedIDs() []*cache.RelatedEntity {
	return nil
}
func (a RecordCEntity) Equal(o cache.Object) bool {
	return true
}

type RecordCQuerier struct {
}

func (r *RecordCQuerier) ConditionQueryForIDs(ctx context.Context, condition dbo.Conditions, option ...interface{}) ([]string, error) {
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordAList := make([]entity.RecordC, 0)
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
func (r *RecordCQuerier) QueryByIDs(ctx context.Context, ids []string, option ...interface{}) ([]cache.Object, error) {
	condition := &RecordACondition{
		IDs: ids,
	}
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordCList := make([]entity.RecordC, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordCList).Error
	if err != nil {
		return nil, err
	}
	result := make([]cache.Object, len(recordCList))
	for i := range recordCList {
		result[i] = &RecordCEntity{
			ID:       recordCList[i].ID,
			Name:     recordCList[i].Name,
			RealName: recordCList[i].RealName,
		}
	}
	return result, nil
}
func (r *RecordCQuerier) UnmarshalObject(ctx context.Context, jsonData string) (cache.Object, error) {
	record := new(RecordCEntity)
	err := json.Unmarshal([]byte(jsonData), record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordCQuerier) Name() string {
	return constant.QuerierC
}

type RecordCCondition struct {
	IDs      []string
	NameLike string
}

func (r *RecordCCondition) GetConditions() ([]string, []interface{}) {
	params := make([]string, 0)
	values := make([]interface{}, 0)

	if len(r.IDs) > 0 {
		params = append(params, "id in (?)")
		values = append(values, r.IDs)
	}
	if r.NameLike != "" {
		params = append(params, "name like ?")
		values = append(values, r.NameLike+"%")
	}
	return params, values
}
func (r *RecordCCondition) GetPager() *dbo.Pager {
	return nil
}
func (r *RecordCCondition) GetOrderBy() string {
	return ""
}

var (
	_recordCQuerier     cache.IConditionalDataSource
	_recordCQuerierOnce sync.Once
)

func GetCQuerier() cache.IConditionalDataSource {
	_recordCQuerierOnce.Do(func() {
		_recordCQuerier = new(RecordCQuerier)
	})
	return _recordCQuerier
}
