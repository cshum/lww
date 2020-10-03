package lww

import "bytes"

// Dict is a state-based LWW-Element-Dictionary,
// consisting MapPut and MapDelete for underlying structure, which are not thread safe.
// It is expected to add separate locking  or coordination under goroutines
//
// BiasDelete denotes bias towards delete if true or bias towards put if false.
type Dict struct {
	MapPut     map[string]Item
	MapDelete  map[string]uint64
	BiasDelete bool
}

// Item is a dictionary item of Dict element,
// consists of dictionary value and timestamp
type Item struct {
	Time  uint64
	Value []byte
}

// NewDict returns a new Dict default bias for deletes
func NewDict() *Dict {
	return &Dict{
		MapPut:     map[string]Item{},
		MapDelete:  map[string]uint64{},
		BiasDelete: true,
	}
}

// Put sets the value and timestamp ts for a key.
func (a *Dict) Put(key string, value []byte, ts uint64) {
	if curr, ok := a.MapPut[key]; !ok ||
		ts > curr.Time ||
		(ts == curr.Time && bytes.Compare(value, curr.Value) == 1) {
		// if timestamp equals
		// bytes compare values for deterministic result
		a.MapPut[key] = Item{ts, value}
	}
}

// Get returns the value bytes stored in the dict for a key, or nil if no value is present.
// ts refers to the timestamp where put or delete exists.
// ok indicates whether value was found in the map.
func (a *Dict) Get(key string) (value []byte, ts uint64, ok bool) {
	if item, hasPut := a.MapPut[key]; hasPut {
		if t, hasDel := a.MapDelete[key]; hasDel && t >= item.Time {
			// if timestamp equals, bias delete op based on BiasDelete flag
			if t > item.Time || a.BiasDelete {
				ts = t
				return
			}
		}
		value = item.Value
		ts = item.Time
		ok = true
	}
	return
}

// Delete deletes the value for a key with timestamp.
func (a *Dict) Delete(key string, ts uint64) {
	if t, ok := a.MapDelete[key]; !ok || ts > t {
		a.MapDelete[key] = ts
	}
}

// Merge merges another Dict to itself
func (a *Dict) Merge(b *Dict) {
	if b == nil || a == b {
		return
	}
	for key, item := range b.MapPut {
		a.Put(key, item.Value, item.Time)
	}
	for key, ts := range b.MapDelete {
		a.Delete(key, ts)
	}
}

// Clone returns a new copy of itself
func (a *Dict) Clone() (result *Dict) {
	result = NewDict()
	result.BiasDelete = a.BiasDelete
	for key, item := range a.MapPut {
		result.MapPut[key] = item
	}
	for key, ts := range a.MapDelete {
		result.MapDelete[key] = ts
	}
	return
}

// ToMap returns native go map from the Dict values
// without the timestamps and deletes.
func (a *Dict) ToMap() (result map[string][]byte) {
	result = map[string][]byte{}
	for key := range a.MapPut {
		if value, _, ok := a.Get(key); ok {
			result[key] = value
		}
	}
	return
}
