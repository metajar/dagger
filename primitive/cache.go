package primitive

import (
	"sync"
)

func newCacheMap() *NamespacedSyncMap {
	return &NamespacedSyncMap{
		cacheMap:  map[string]*SyncCache{},
		mu:        sync.RWMutex{},
		closeOnce: sync.Once{},
	}
}

type namespaceCache interface {
	Len(namespace string) int
	Namespaces() []string
	Get(namespace string, key interface{}) (interface{}, bool)
	Set(namespace string, key interface{}, value interface{})
	Range(namespace string, f func(key string, value interface{}) bool)
	Delete(namespace string, key interface{})
	Exists(namespace string, key interface{}) bool
	Copy(namespace string) Node
	Filter(namespace string, filter func(k, v interface{}) bool) Node
	Intersection(namespace1, namespace2 string) Node
	Union(namespace1, namespace2 string) Node
	Map(namespace string) Node
	SetAll(namespace string, m Node)
	Clear(namespace string)
	Close()
}
