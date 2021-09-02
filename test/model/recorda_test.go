package model

import (
	"encoding/json"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/cache"
	"reflect"
	"testing"
)

func newStruct(rda interface{}) {
	switch reflect.TypeOf(rda).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(rda)
		s = reflect.Append(s, reflect.ValueOf(RecordEEntity{
			ID:      "15",
			Title:   "25",
			Content: "35",
		}))
		t := reflect.TypeOf(rda)

		fmt.Println(t.Elem())
		for i := 0; i < s.Len(); i++ {
			fmt.Println(s.Index(i).Type())
			obj := s.Index(i).Interface().(cache.Object)
			jsonData, err := json.Marshal(obj)
			if err != nil {
				panic(err)
			}
			x := reflect.New(s.Index(i).Type()).Interface().(cache.Object)
			err = json.Unmarshal(jsonData, x)
			if err != nil {
				panic(err)
			}
			fmt.Println("x:", x)
		}
	}
}

func TestJSONUnmarshal(t *testing.T) {
	//rda := make([]RecordEEntity, 0)
	r := []RecordEEntity{{
		ID:      "1",
		Title:   "2",
		Content: "3",
	}, {
		ID:      "14",
		Title:   "24",
		Content: "34",
	}}
	newStruct(r)
}
