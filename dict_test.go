package lww

import (
	"reflect"
	"testing"
)

func incr(ts *uint64) uint64 {
	*ts++
	return *ts
}

func TestDict_CRUD(t *testing.T) {
	var ts uint64 = 1
	dict := NewDict()

	dict.Put("a", []byte("1"), incr(&ts))
	if val, _, ok := dict.Get("a"); !(string(val) == "1" && ok) {
		t.Errorf("put error %v %v", val, ok)
	}

	dict.Put("b", []byte("2"), incr(&ts))
	if val, _, ok := dict.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("put error %v %v", val, ok)
	}

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	}) {
		t.Errorf("dict export not match %v", data)
	}

	if clone := dict.Clone(); !reflect.DeepEqual(dict, clone) {
		t.Errorf("dict clone mismatch %v %v", dict, clone)
	}

	dict.Delete("a", incr(&ts))
	if val, _, ok := dict.Get("a"); !(string(val) == "" && !ok) {
		t.Errorf("delete error %v %v", val, ok)
	}

	dict.Delete("b", 1)
	if val, _, ok := dict.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("should not delete ts less %v %v", val, ok)
	}

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"b": []byte("2"),
	}) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_BiasDelete(t *testing.T) {
	dict := NewDict()

	dict.Put("a", []byte("1"), 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("put error %v %v %v", val, ts, ok)
	}

	dict.Delete("a", 1)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("should not delete with time less %v %v %v", val, ts, ok)
	}

	dict.Delete("a", 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "" && ts == 2 && !ok) {
		t.Errorf("bias delete error %v %v %v", val, ts, ok)
	}

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{}) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_BiasPut(t *testing.T) {
	dict := NewDict()
	dict.BiasDelete = false

	dict.Put("a", []byte("1"), 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("put error %v %v %v", val, ts, ok)
	}

	dict.Delete("a", 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("should not delete with bias put %v %v %v", val, ts, ok)
	}

	dict.Delete("a", 3)
	if val, ts, ok := dict.Get("a"); !(string(val) == "" && ts == 3 && !ok) {
		t.Errorf("should delete with bias put %v %v %v", val, ts, ok)
	}

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{}) {
		t.Errorf("dict export not match %v", data)
	}
}
