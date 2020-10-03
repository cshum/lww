package lww

import "bytes"

// Dict is a state-based LWW-Element-Dictionary,
// consisting MapAdd and MapRemove for underlying structure, which are not thread safe.
// It is expected to add separate locking  or coordination under goroutines
//
// BiasRemove denotes bias towards delete if true or bias towards put if false.
//
// run test: `go test -v`
type Dict struct {
	MapAdd     map[string]Item
	MapRemove  map[string]uint64
	BiasRemove bool
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
		MapAdd:     map[string]Item{},
		MapRemove:  map[string]uint64{},
		BiasRemove: true,
	}
}

// Add adds or updates the value and timestamp ts for a key.
//
// In addition to adding a value like lww set, this also updates value of the same key.
// If timestamps are equal, the larger bytes value would be flavoured in order to preserve convergence.
func (dict *Dict) Add(key string, value []byte, ts uint64) {
	if curr, ok := dict.MapAdd[key]; !ok ||
		ts > curr.Time ||
		(ts == curr.Time && bytes.Compare(value, curr.Value) == 1) {
		// if timestamp equals
		// bytes compare values for deterministic result
		dict.MapAdd[key] = Item{ts, value}
	}
}

// Get returns the value bytes stored in the dict for a key, or nil if no value is present.
// ts refers to the timestamp where add or remove exists.
// ok indicates whether value was found in the map.
//
// With `dict.BiasRemove = true` being set, it will flavors remove over add if timestamps are equal.
func (dict *Dict) Get(key string) (value []byte, ts uint64, ok bool) {
	if item, hasPut := dict.MapAdd[key]; hasPut {
		if t, hasDel := dict.MapRemove[key]; hasDel && t >= item.Time {
			// if timestamp equals, bias remove op based on BiasRemove flag
			if t > item.Time || dict.BiasRemove {
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

// Remove removes the value for a key with timestamp.
func (dict *Dict) Remove(key string, ts uint64) {
	if t, ok := dict.MapRemove[key]; !ok || ts > t {
		dict.MapRemove[key] = ts
	}
}

// Merge merges other Dict to itself
func (dict *Dict) Merge(other *Dict) {
	if other == nil || dict == other {
		return
	}
	for key, item := range other.MapAdd {
		dict.Add(key, item.Value, item.Time)
	}
	for key, ts := range other.MapRemove {
		dict.Remove(key, ts)
	}
}

// Clone returns a new copy of itself
func (dict *Dict) Clone() (result *Dict) {
	result = NewDict()
	result.BiasRemove = dict.BiasRemove
	for key, item := range dict.MapAdd {
		result.MapAdd[key] = item
	}
	for key, ts := range dict.MapRemove {
		result.MapRemove[key] = ts
	}
	return
}

// ToMap returns native go map from the Dict values
// without the timestamps and deletes.
func (dict *Dict) ToMap() (result map[string][]byte) {
	result = map[string][]byte{}
	for key := range dict.MapAdd {
		if value, _, ok := dict.Get(key); ok {
			result[key] = value
		}
	}
	return
}
