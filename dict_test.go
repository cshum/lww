package lww

import (
	"reflect"
	"testing"
)

// incr increments local timestamp
func incr(ts *uint64) uint64 {
	*ts++
	return *ts
}

// witness updates local timestamp
// after witnessing timestamp of another process
func witness(a *uint64, b uint64) {
	if *a > b {
		*a++
	} else {
		*a = b + 1
	}
}

func TestDict_CRUD(t *testing.T) {
	// Test local CRUD and methods
	var ts uint64
	a := NewDict()

	a.Put("a", []byte("1"), incr(&ts))
	a.Put("a", []byte("11"), 0)
	if val, _, ok := a.Get("a"); !(string(val) == "1" && ok) {
		t.Errorf("put error %v %v", val, ok)
	}

	a.Put("b", []byte("22"), 0)
	a.Put("b", []byte("2"), incr(&ts))
	if val, _, ok := a.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("put error %v %v", val, ok)
	}

	if data := a.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	}) {
		t.Errorf("a export not match %v", data)
	}

	a.Delete("a", incr(&ts))
	if val, _, ok := a.Get("a"); !(val == nil && !ok) {
		t.Errorf("delete error %v %v", val, ok)
	}

	a.Delete("b", 1)
	if val, _, ok := a.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("should not delete ts less %v %v", val, ok)
	}

	if data := a.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"b": []byte("2"),
	}) {
		t.Errorf("a export not match %v", data)
	}

	if clone := a.Clone(); !reflect.DeepEqual(a, clone) {
		t.Errorf("a clone mismatch %v %v", a, clone)
	}
}

func TestDict_Merge(t *testing.T) {
	// Test basic multi dict merge
	var (
		a   = NewDict()
		aTs uint64
		b   = NewDict()
		bTs uint64
	)

	a.Put("a", []byte("1"), incr(&aTs))
	a.Put("b", []byte("2"), incr(&aTs))
	b.Put("c", []byte("3"), incr(&bTs))

	a.Merge(a)
	a.Merge(nil)
	a.Merge(b)
	witness(&aTs, bTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !reflect.DeepEqual(a, b) {
		t.Errorf("a b not converge %v %v", a, b)
	}
	if data := a.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	}) {
		t.Errorf("dict export not match %v", data)
	}

	a.Delete("c", incr(&aTs))
	b.Delete("b", incr(&bTs))

	a.Merge(b)
	witness(&aTs, bTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !reflect.DeepEqual(a, b) {
		t.Errorf("a b not converge %v %v", a, b)
	}
	if data := a.Export(); !(reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
	}) && reflect.DeepEqual(data, b.Export())) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_Convergence(t *testing.T) {
	// Test multi dict merge edge case,
	// of same timestamp convergence
	var (
		a   = NewDict()
		aTs uint64
		b   = NewDict()
		bTs uint64
		c   = NewDict()
		cTs uint64
		d   = NewDict()
		dTs uint64
	)
	a.Put("a", []byte("1"), incr(&aTs))
	b.Put("a", []byte("2"), incr(&bTs))
	c.Put("a", []byte("3"), incr(&cTs))

	a.Merge(b)
	witness(&aTs, bTs)
	a.Merge(c)
	witness(&aTs, cTs)
	c.Merge(a)
	witness(&cTs, aTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !(reflect.DeepEqual(a, b) && reflect.DeepEqual(b, c)) {
		t.Errorf("a b c should converge %v %v %v", a, b, c)
	}
	if data := a.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("3"),
	}) {
		t.Errorf("dict export not match %v", data)
	}

	d.Merge(a)
	witness(&dTs, aTs)

	d.Delete("a", incr(&dTs))
	a.Merge(d)
	witness(&aTs, dTs)
	c.Merge(a)
	witness(&cTs, aTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !(reflect.DeepEqual(a, b) && reflect.DeepEqual(b, c) && reflect.DeepEqual(c, d)) {
		t.Errorf("a b c d should converge %v %v %v %v", a, b, c, d)
	}
}

func TestDict_BiasDelete(t *testing.T) {
	// Test bias delete
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
	dict.Delete("a", 3)
	if val, ts, ok := dict.Get("a"); !(val == nil && ts == 3 && !ok) {
		t.Errorf("bias delete error %v %v %v", val, ts, ok)
	}

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{}) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_BiasPut(t *testing.T) {
	// Test bias put
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

	if data := dict.Export(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
	}) {
		t.Errorf("dict export not match %v", data)
	}
}
