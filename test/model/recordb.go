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

type RecordBEntity struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	DID     string         `json:"did"`
	RecordD *RecordDEntity `json:"record_d"`
}

func (a RecordBEntity) StringID() string {
	return a.ID
}
func (a RecordBEntity) RelatedIDs() []*cache.RelatedEntity {
	return []*cache.RelatedEntity{
		{
			DataSourceName: constant.QuerierD,
			RelatedIDs:     []string{a.DID},
		},
	}
}
func (a RecordBEntity) Equal(o cache.Object) bool {
	return true
}

type RecordBQuerier struct {
}

func (r *RecordBQuerier) ConditionQueryForIDs(ctx context.Context, condition dbo.Conditions, option ...interface{}) ([]string, error) {
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordBList := make([]entity.RecordB, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordBList).Error
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for i := range recordBList {
		result = append(result, recordBList[i].ID)
	}
	return result, nil
}
func (r *RecordBQuerier) QueryByIDs(ctx context.Context, ids []string, option ...interface{}) ([]cache.Object, error) {
	condition := &RecordACondition{
		IDs: ids,
	}
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordBList := make([]entity.RecordB, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordBList).Error
	if err != nil {
		return nil, err
	}
	entities := make([]*RecordBEntity, len(recordBList))
	for i := range recordBList {
		entities[i] = &RecordBEntity{
			ID:   recordBList[i].ID,
			Name: recordBList[i].Name,
			DID:  recordBList[i].DID,
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
func (r *RecordBQuerier) UnmarshalObject(ctx context.Context, jsonData string) (cache.Object, error) {
	record := new(RecordBEntity)
	err := json.Unmarshal([]byte(jsonData), record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordBQuerier) Name() string {
	return constant.QuerierB
}

func (r *RecordBQuerier) fillObjects(ctx context.Context, entities []*RecordBEntity) error {
	dids := make([]string, len(entities))
	for i := range entities {
		dids[i] = entities[i].DID
	}
	dRecordsMap, err := queryObjectMap(ctx, GetDQuerier(), dids)
	if err != nil {
		return err
	}
	for i := range entities {
		entities[i].RecordD = dRecordsMap[entities[i].DID].(*RecordDEntity)
	}
	return nil
}

type RecordBCondition struct {
	IDs      []string
	NameLike string
}

func (r *RecordBCondition) GetConditions() ([]string, []interface{}) {
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
func (r *RecordBCondition) GetPager() *dbo.Pager {
	return nil
}
func (r *RecordBCondition) GetOrderBy() string {
	return ""
}

var (
	_recordBQuerier     cache.IConditionalDataSource
	_recordBQuerierOnce sync.Once
)

func GetBQuerier() cache.IConditionalDataSource {
	_recordBQuerierOnce.Do(func() {
		_recordBQuerier = new(RecordBQuerier)
	})
	return _recordBQuerier
}
