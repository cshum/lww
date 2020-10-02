package lww

import "bytes"

type Dict struct {
	MapPut     map[string]Item
	MapDelete  map[string]uint64
	BiasDelete bool
}

type Item struct {
	Time  uint64
	Value []byte
}

func NewDict() *Dict {
	return &Dict{
		MapPut:     map[string]Item{},
		MapDelete:  map[string]uint64{},
		BiasDelete: true,
	}
}

func (a *Dict) Put(key string, value []byte, ts uint64) {
	if curr, ok := a.MapPut[key]; !ok || ts >= curr.Time {
		// if timestamp equals
		// bytes compare for deterministic outcome for convergence
		if ts > curr.Time || bytes.Compare(value, curr.Value) == 1 {
			a.MapPut[key] = Item{ts, value}
		}
	}
}

func (a *Dict) Get(key string) (value []byte, ts uint64, ok bool) {
	if item, hasPut := a.MapPut[key]; hasPut {
		if t, hasDel := a.MapDelete[key]; hasDel && t >= item.Time {
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

func (a *Dict) Delete(key string, ts uint64) {
	if t, ok := a.MapDelete[key]; !ok || ts > t {
		a.MapDelete[key] = ts
	}
}

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

func (a *Dict) Export() (result map[string][]byte) {
	result = map[string][]byte{}
	for key := range a.MapPut {
		if value, _, ok := a.Get(key); ok {
			result[key] = value
		}
	}
	return
}
