package lww

import "bytes"

type Dict struct {
	PutMap     map[string]Item
	DeleteMap  map[string]int64
	BiasDelete bool
}

type Item struct {
	Time  int64
	Value []byte
}

func NewDict() *Dict {
	return &Dict{
		PutMap:     map[string]Item{},
		DeleteMap:  map[string]int64{},
		BiasDelete: true,
	}
}

func (a *Dict) Put(key string, value []byte, ts int64) {
	if curr, ok := a.PutMap[key]; ok && ts >= curr.Time {
		// if timestamp equals
		// bytes compare for deterministic outcome for convergence
		if ts > curr.Time || bytes.Compare(value, curr.Value) == 1 {
			a.PutMap[key] = Item{ts, value}
		}
	}
}

func (a *Dict) Get(key string) (value []byte, ts int64, ok bool) {
	if item, hasPut := a.PutMap[key]; hasPut {
		if t, hasDel := a.DeleteMap[key]; hasDel && t >= item.Time {
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

func (a *Dict) Delete(key string, ts int64) {
	if t, ok := a.DeleteMap[key]; ok && ts > t {
		a.DeleteMap[key] = ts
	}
}

func (a *Dict) Merge(b *Dict) {
	if b == nil {
		return
	}
	for key, item := range b.PutMap {
		a.Put(key, item.Value, item.Time)
	}
	for key, ts := range b.DeleteMap {
		a.Delete(key, ts)
	}
}

func (a *Dict) Clone() (result *Dict) {
	result = NewDict()
	result.BiasDelete = a.BiasDelete
	for key, item := range a.PutMap {
		result.PutMap[key] = item
	}
	for key, ts := range a.DeleteMap {
		result.DeleteMap[key] = ts
	}
	return
}

func (a *Dict) Export() (result map[string][]byte) {
	result = map[string][]byte{}
	for key := range a.PutMap {
		if value, _, ok := a.Get(key); ok {
			result[key] = value
		}
	}
	return
}
