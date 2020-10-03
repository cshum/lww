# lww
--
    import "github.com/cshum/lww"


## Usage

#### type Dict

```go
type Dict struct {
	MapAdd     map[string]Item
	MapRemove  map[string]uint64
	BiasRemove bool
}
```

Dict is a state-based LWW-Element-Dictionary, consisting MapAdd and MapRemove
for underlying structure, which are not thread safe. It is expected to add
separate locking or coordination under goroutines

BiasRemove denotes bias towards delete if true or bias towards put if false.

run test: `go test -v`

#### func  NewDict

```go
func NewDict() *Dict
```
NewDict returns a new Dict default bias for deletes

#### func (*Dict) Add

```go
func (dict *Dict) Add(key string, value []byte, ts uint64)
```
Add sets the value and timestamp ts for a key.

In addition to adding a value like lww set, this also updates value and
timestamp of the same key. If timestamp equals, the larger bytes value would be
flavoured in order to preserve convergence.

#### func (*Dict) Clone

```go
func (dict *Dict) Clone() (result *Dict)
```
Clone returns a new copy of itself

#### func (*Dict) Get

```go
func (dict *Dict) Get(key string) (value []byte, ts uint64, ok bool)
```
Get returns the value bytes stored in the dict for a key, or nil if no value is
present. ts refers to the timestamp where add or remove exists. ok indicates
whether value was found in the map.

With `dict.BiasRemove = true` being set, it will flavors remove over add if
timestamps are equal.

#### func (*Dict) Merge

```go
func (dict *Dict) Merge(other *Dict)
```
Merge merges other Dict to itself

#### func (*Dict) Remove

```go
func (dict *Dict) Remove(key string, ts uint64)
```
Remove removes the value for a key with timestamp.

#### func (*Dict) ToMap

```go
func (dict *Dict) ToMap() (result map[string][]byte)
```
ToMap returns native go map from the Dict values without the timestamps and
deletes.

#### type Item

```go
type Item struct {
	Time  uint64
	Value []byte
}
```

Item is a dictionary item of Dict element, consists of dictionary value and
timestamp
