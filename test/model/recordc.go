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

func (r *RecordCQuerier) QueryForIDs(ctx context.Context, condition dbo.Conditions) ([]string, error) {
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
func (r *RecordCQuerier) BatchGet(ctx context.Context, ids []string) ([]cache.Object, error) {
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

func (r *RecordCQuerier) ID() string {
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
	_recordCQuerier     cache.IConditionQuerier
	_recordCQuerierOnce sync.Once
)

func GetCQuerier() cache.IConditionQuerier {
	_recordCQuerierOnce.Do(func() {
		_recordCQuerier = new(RecordCQuerier)
	})
	return _recordCQuerier
}
