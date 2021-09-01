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

type RecordDEntity struct {
	ID      string         `json:"id"`
	Title   string         `json:"title"`
	Content string         `json:"content"`
	EID     string         `json:"eid"`
	ERecord *RecordEEntity `json:"e_record"`
}

func (a RecordDEntity) StringID() string {
	return a.ID
}
func (a RecordDEntity) RelatedIDs() []*cache.RelatedEntity {
	return []*cache.RelatedEntity{
		{
			QuerierName: constant.QuerierE,
			RelatedIDs:  []string{a.EID},
		},
	}
}
func (a RecordDEntity) Equal(o cache.Object) bool {
	return true
}

type RecordDQuerier struct {
}

func (r *RecordDQuerier) QueryForIDs(ctx context.Context, condition dbo.Conditions) ([]string, error) {
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordAList := make([]entity.RecordD, 0)
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
func (r *RecordDQuerier) BatchGet(ctx context.Context, ids []string) ([]cache.Object, error) {
	condition := &RecordACondition{
		IDs: ids,
	}
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordDList := make([]entity.RecordD, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordDList).Error
	if err != nil {
		return nil, err
	}
	entities := make([]*RecordDEntity, len(recordDList))
	for i := range recordDList {
		entities[i] = &RecordDEntity{
			ID:      recordDList[i].ID,
			Title:   recordDList[i].Title,
			Content: recordDList[i].Content,
			EID:     recordDList[i].EID,
		}
	}
	err = r.fillObjects(ctx, entities)
	if err != nil {
		return nil, err
	}
	result := make([]cache.Object, len(entities))
	for i := range entities {
		result[i] = cache.Object(entities[i])
	}
	return result, nil
}
func (r *RecordDQuerier) UnmarshalObject(ctx context.Context, jsonData string) (cache.Object, error) {
	record := new(RecordDEntity)
	err := json.Unmarshal([]byte(jsonData), record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordDQuerier) ID() string {
	return constant.QuerierD
}

func (r *RecordDQuerier) fillObjects(ctx context.Context, entities []*RecordDEntity) error {
	eids := make([]string, len(entities))
	for i := range entities {
		eids[i] = entities[i].EID
	}
	eRecordsMap, err := queryObjectMap(ctx, GetEQuerier(), eids)
	if err != nil {
		return err
	}
	for i := range entities {
		entities[i].ERecord = eRecordsMap[entities[i].EID].(*RecordEEntity)
	}
	return nil
}

type RecordDCondition struct {
	IDs []string
}

func (r *RecordDCondition) GetConditions() ([]string, []interface{}) {
	params := make([]string, 0)
	values := make([]interface{}, 0)

	if len(r.IDs) > 0 {
		params = append(params, "id in (?)")
		values = append(values, r.IDs)
	}

	return params, values
}
func (r *RecordDCondition) GetPager() *dbo.Pager {
	return nil
}
func (r *RecordDCondition) GetOrderBy() string {
	return ""
}

var (
	_recordDQuerier     cache.IQuerier
	_recordDQuerierOnce sync.Once
)

func GetDQuerier() cache.IQuerier {
	_recordDQuerierOnce.Do(func() {
		_recordDQuerier = new(RecordDQuerier)
	})
	return _recordDQuerier
}
