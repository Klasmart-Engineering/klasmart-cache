package model

import (
	"encoding/json"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/kidsloop-cache/cache"
	"testing"
)

func TestAddSlice(t *testing.T) {
	a := make([]*RecordEEntity, 0)
	slice, err := cache.NewReflectObjectSlice(&a)
	if err != nil {
		panic(err)
	}
	slice.Append(&RecordEEntity{
		ID:      "a",
		Title:   "bbb",
		Content: "ccc",
	})
	slice.Iterator(func(o cache.Object) {
		t.Log(o)
	})
	for i := range a {
		t.Log(a[i])
	}

	newSlice := make([]cache.Object, 0)
	newSlice = append(newSlice, &RecordEEntity{
		ID:      "a1",
		Title:   "bbb4",
		Content: "ccc4",
	},
	)
	newSlice = append(newSlice, &RecordEEntity{
		ID:      "a2",
		Title:   "bbb2",
		Content: "ccc2",
	},
	)

	slice.SetSlice(newSlice)

	for i := range a {
		t.Log(a[i])
	}
	//newSlice := slice.NewSlice(10)
	//newSlice
}

func TestJSONMarshal(t *testing.T) {
	a := make([]RecordEEntity, 0)
	slice, err := cache.NewReflectObjectSlice(&a)
	if err != nil {
		panic(err)
	}
	slice.Append(&RecordEEntity{
		ID:      "a",
		Title:   "bbb",
		Content: "ccc",
	})
	slice.Iterator(func(o cache.Object) {
		t.Log(o)
	})
	bx := &RecordEEntity{
		ID:      "bb0",
		Title:   "bb1",
		Content: "cc1",
	}

	bxJson, err := json.Marshal(bx)
	if err != nil {
		panic(err)
	}
	cx := slice.NewElement()
	err = json.Unmarshal(bxJson, cx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", cx)

}
