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

type RecordAEntity struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Keywords    string `json:"keywords"`
	Description string `json:"description"`

	BID string `json:"bid"`
	CID string `json:"cid"`
	DID string `json:"did"`

	RecordB *RecordBEntity `json:"record_b"`
	RecordC *RecordCEntity `json:"record_c"`
	RecordD *RecordDEntity `json:"record_d"`
}

func (a RecordAEntity) StringID() string {
	return a.ID
}
func (a RecordAEntity) RelatedIDs() []*cache.RelatedEntity {
	return []*cache.RelatedEntity{
		{
			QuerierName: constant.QuerierB,
			RelatedIDs:  []string{a.BID},
		},
		{
			QuerierName: constant.QuerierC,
			RelatedIDs:  []string{a.CID},
		},
		{
			QuerierName: constant.QuerierD,
			RelatedIDs:  []string{a.DID},
		},
	}
}
func (a RecordAEntity) Equal(o cache.Object) bool {
	a0, ok := o.(*RecordAEntity)
	if !ok {
		return false
	}
	if a0.Name != a.Name {
		return false
	}
	if a0.Description != a.Description {
		return false
	}
	if a0.Keywords != a.Keywords {
		return false
	}
	return true
}

type RecordAQuerier struct {
}

func (r *RecordAQuerier) DeleteByID(ctx context.Context, ids []string) error {
	return global.DBClient.Where("id in (?)", ids).Delete(entity.RecordA{}).Error
}
func (r *RecordAQuerier) ConditionQueryForIDs(ctx context.Context, condition dbo.Conditions, option ...interface{}) ([]string, error) {
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordAList := make([]entity.RecordA, 0)
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

func (r *RecordAQuerier) QueryByIDs(ctx context.Context, ids []string, option ...interface{}) ([]cache.Object, error) {
	condition := &RecordACondition{
		IDs: ids,
	}
	query, params := condition.GetConditions()
	paramQuery := strings.Join(query, " and ")
	recordAList := make([]entity.RecordA, 0)
	err := global.DBClient.Where(paramQuery, params...).Find(&recordAList).Error
	if err != nil {
		return nil, err
	}
	entities := make([]*RecordAEntity, len(recordAList))
	for i := range recordAList {
		entities[i] = &RecordAEntity{
			ID:          recordAList[i].ID,
			Name:        recordAList[i].Name,
			Keywords:    recordAList[i].Keywords,
			Description: recordAList[i].Description,
			BID:         recordAList[i].BID,
			CID:         recordAList[i].CID,
			DID:         recordAList[i].DID,
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
func (r *RecordAQuerier) UnmarshalObject(ctx context.Context, jsonData string) (cache.Object, error) {
	record := new(RecordAEntity)
	err := json.Unmarshal([]byte(jsonData), record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordAQuerier) Name() string {
	return constant.QuerierA
}

func (r *RecordAQuerier) fillObjects(ctx context.Context, entities []*RecordAEntity) error {
	bids := make([]string, len(entities))
	cids := make([]string, len(entities))
	dids := make([]string, len(entities))
	for i := range entities {
		bids[i] = entities[i].BID
		cids[i] = entities[i].CID
		dids[i] = entities[i].DID
	}
	bRecordsMap, err := queryObjectMap(ctx, GetBQuerier(), bids)
	if err != nil {
		return err
	}
	cRecordsMap, err := queryObjectMap(ctx, GetCQuerier(), cids)
	if err != nil {
		return err
	}
	dRecordsMap, err := queryObjectMap(ctx, GetDQuerier(), dids)
	if err != nil {
		return err
	}
	for i := range entities {
		if bRecordsMap[entities[i].BID] != nil {
			entities[i].RecordB = bRecordsMap[entities[i].BID].(*RecordBEntity)
		}
		if cRecordsMap[entities[i].CID] != nil {
			entities[i].RecordC = cRecordsMap[entities[i].CID].(*RecordCEntity)
		}
		if dRecordsMap[entities[i].DID] != nil {
			entities[i].RecordD = dRecordsMap[entities[i].DID].(*RecordDEntity)
		}
	}
	return nil
}

type RecordACondition struct {
	IDs         []string
	NameLike    string
	Keywords    string
	Description string

	BID []string
	CID []string
	DID []string
}

func (r *RecordACondition) GetConditions() ([]string, []interface{}) {
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
	if r.Keywords != "" {
		params = append(params, "keywords like ?")
		values = append(values, r.Keywords)
	}
	if r.Description != "" {
		params = append(params, "description like ?")
		values = append(values, r.Description)
	}
	if len(r.BID) > 0 {
		params = append(params, "bid in (?)")
		values = append(values, r.BID)
	}
	if len(r.CID) > 0 {
		params = append(params, "cid in (?)")
		values = append(values, r.CID)
	}
	if len(r.DID) > 0 {
		params = append(params, "did in (?)")
		values = append(values, r.DID)
	}
	return params, values
}

func (r *RecordACondition) GetPager() *dbo.Pager {
	return nil
}

func (r *RecordACondition) GetOrderBy() string {
	return ""
}

func queryObjectMap(ctx context.Context, querier cache.IDataSource, ids []string) (map[string]cache.Object, error) {
	data, err := querier.QueryByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	res := make(map[string]cache.Object)
	for i := range data {
		res[data[i].StringID()] = data[i]
	}
	return res, nil
}

var (
	_recordAQuerier     *RecordAQuerier
	_recordAQuerierOnce sync.Once
)

func GetAQuerier() *RecordAQuerier {
	_recordAQuerierOnce.Do(func() {
		_recordAQuerier = new(RecordAQuerier)
	})
	return _recordAQuerier
}
