package cache

import (
	"reflect"
)

type ReflectObjectSlice struct {
	ptr      reflect.Value
	slice    reflect.Value
	elemType reflect.Type
}

func (r *ReflectObjectSlice) NewElement() Object {
	return reflect.New(r.elemType).Interface().(Object)
}

func (r *ReflectObjectSlice) SetSlice(o []Object) {
	r.ptr.Elem().Set(reflect.MakeSlice(r.ptr.Type().Elem(), 0, r.ptr.Elem().Cap()))
	for i := range o {
		r.Append(o[i])
	}
}

func (r *ReflectObjectSlice) Iterator(do func(o Object)) {
	for i := 0; i < r.slice.Len(); i++ {
		obj := r.slice.Index(i).Interface().(Object)
		do(obj)
	}
}

func (r *ReflectObjectSlice) Append(o ...Object) {
	for i := range o {
		r.slice.Set(reflect.Append(r.slice, reflect.ValueOf(o[i])))
	}
}

func NewReflectObjectSlice(o interface{}) (*ReflectObjectSlice, error) {
	switch reflect.TypeOf(o).Kind() {
	case reflect.Ptr:
		ptr := reflect.ValueOf(o)
		s := ptr.Elem()
		pt := reflect.TypeOf(o).Elem().Elem()

		if pt.Kind() != reflect.Struct {
			pt = pt.Elem()
		}
		return &ReflectObjectSlice{ptr: ptr, slice: s, elemType: pt}, nil
	default:
		return nil, ErrInvalidObjectSlice
	}
}
