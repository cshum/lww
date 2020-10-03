package lww

import (
	"reflect"
	"testing"
)

func TestDict_CRUD(t *testing.T) {
	// Test local CRUD and methods
	var ts uint64
	a := NewDict()

	a.Add("a", []byte("1"), incr(&ts))
	a.Add("a", []byte("11"), 0) // should no op
	if val, _, ok := a.Get("a"); !(string(val) == "1" && ok) {
		t.Errorf("add error %v %v", val, ok)
	}

	a.Add("b", []byte("22"), 0) // should no op
	a.Add("b", []byte("2"), incr(&ts))
	if val, _, ok := a.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("add error %v %v", val, ok)
	}

	if data := a.ToMap(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	}) {
		t.Errorf("a export not match %v", data)
	}

	a.Remove("a", incr(&ts))
	if val, _, ok := a.Get("a"); !(val == nil && !ok) {
		t.Errorf("remove error %v %v", val, ok)
	}

	a.Remove("b", 1)
	if val, _, ok := a.Get("b"); !(string(val) == "2" && ok) {
		t.Errorf("should not remove ts less %v %v", val, ok)
	}

	if data := a.ToMap(); !reflect.DeepEqual(data, map[string][]byte{
		"b": []byte("2"),
	}) {
		t.Errorf("a export not match %v", data)
	}

	if clone := a.Clone(); !reflect.DeepEqual(a, clone) {
		t.Errorf("a clone mismatch %v %v", a, clone)
	}
}

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

func TestDict_Merge(t *testing.T) {
	// Test basic multi dict merge
	var (
		a   = NewDict()
		aTs uint64
		b   = NewDict()
		bTs uint64
	)

	a.Add("a", []byte("1"), incr(&aTs))
	a.Add("b", []byte("2"), incr(&aTs))
	b.Add("c", []byte("3"), incr(&bTs))

	a.Merge(a)
	a.Merge(nil)
	a.Merge(b)
	witness(&aTs, bTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !reflect.DeepEqual(a, b) {
		t.Errorf("a b not converge %v %v", a, b)
	}
	if data := a.ToMap(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	}) {
		t.Errorf("dict export not match %v", data)
	}

	a.Remove("c", incr(&aTs))
	b.Remove("b", incr(&bTs))

	a.Merge(b)
	witness(&aTs, bTs)
	b.Merge(a)
	witness(&bTs, aTs)

	if !reflect.DeepEqual(a, b) {
		t.Errorf("a b not converge %v %v", a, b)
	}
	if data := a.ToMap(); !(reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
	}) && reflect.DeepEqual(data, b.ToMap())) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_Convergence(t *testing.T) {
	// Test multi dict merge edge case,
	// of same timestamp convergence
	var (
		a = NewDict()
		b = NewDict()
		c = NewDict()
		d = NewDict()
	)
	a.Add("a", []byte("1"), 1)
	b.Add("a", []byte("2"), 1)
	c.Add("a", []byte("3"), 1)

	a.Merge(b)
	a.Merge(c)
	c.Merge(a)
	b.Merge(a)

	if !(reflect.DeepEqual(a, b) && reflect.DeepEqual(b, c)) {
		t.Errorf("a b c should converge %v %v %v", a, b, c)
	}

	d.Merge(a)
	d.Remove("a", 1)

	a.Merge(d)
	c.Merge(a)
	b.Merge(a)

	if !(reflect.DeepEqual(a, b) && reflect.DeepEqual(b, c) && reflect.DeepEqual(c, d)) {
		t.Errorf("a b c d should converge %v %v %v %v", a, b, c, d)
	}
	if data := a.ToMap(); !reflect.DeepEqual(data, map[string][]byte{}) {
		t.Errorf("bias remove error %v", data)
	}
}

func TestDict_BiasRemove(t *testing.T) {
	// Test bias remove
	dict := NewDict()

	dict.Add("a", []byte("1"), 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("add error %v %v %v", val, ts, ok)
	}

	dict.Remove("a", 1)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("should not remove with time less %v %v %v", val, ts, ok)
	}

	dict.Remove("a", 2)
	dict.Remove("a", 3)
	if val, ts, ok := dict.Get("a"); !(val == nil && ts == 3 && !ok) {
		t.Errorf("bias remove error %v %v %v", val, ts, ok)
	}

	if data := dict.ToMap(); !reflect.DeepEqual(data, map[string][]byte{}) {
		t.Errorf("dict export not match %v", data)
	}
}

func TestDict_BiasAdd(t *testing.T) {
	// Test bias add
	dict := NewDict()
	dict.BiasRemove = false

	dict.Add("a", []byte("1"), 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("add error %v %v %v", val, ts, ok)
	}

	dict.Remove("a", 2)
	if val, ts, ok := dict.Get("a"); !(string(val) == "1" && ts == 2 && ok) {
		t.Errorf("should not remove with bias add %v %v %v", val, ts, ok)
	}

	if data := dict.ToMap(); !reflect.DeepEqual(data, map[string][]byte{
		"a": []byte("1"),
	}) {
		t.Errorf("dict export not match %v", data)
	}
}
