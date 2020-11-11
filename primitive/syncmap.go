package primitive

import (
	"sync"
)

//NamespacedSyncMap handles an in memory sync.map
type NamespacedSyncMap struct {
	cacheMap  map[string]*SyncCache
	mu        sync.RWMutex
	closeOnce sync.Once
}

func (n *NamespacedSyncMap) Len(namespace string) int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if c, ok := n.cacheMap[namespace]; ok {
		return c.Len()
	}
	return 0
}

func (n *NamespacedSyncMap) Namespaces() []string {
	var namespaces []string
	n.mu.RLock()
	defer n.mu.RUnlock()
	for k, _ := range n.cacheMap {
		namespaces = append(namespaces, k)
	}
	return namespaces
}

func (n *NamespacedSyncMap) Get(namespace string, key interface{}) (interface{}, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if c, ok := n.cacheMap[namespace]; ok {
		return c.Get(key)
	}
	return nil, false
}

func (n *NamespacedSyncMap) Set(namespace string, key interface{}, value interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.cacheMap[namespace]; !ok {
		n.cacheMap[namespace] = &SyncCache{
			data: sync.Map{},
			once: sync.Once{},
		}

	}
	n.cacheMap[namespace].Set(key, value)
}

func (n *NamespacedSyncMap) Range(namespace string, f func(key string, value interface{}) bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if namespace == AnyType {
		for _, c := range n.cacheMap {
			c.Range(f)
		}
	} else {
		if c, ok := n.cacheMap[namespace]; ok {
			c.Range(f)
		}
	}

}

func (n *NamespacedSyncMap) Delete(namespace string, key interface{}) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if c, ok := n.cacheMap[namespace]; ok {
		c.Delete(key)
	}
}

func (n *NamespacedSyncMap) Exists(namespace string, key interface{}) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace].Exists(key)
}

func (n *NamespacedSyncMap) Copy(namespace string) Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace].Copy()
}

func (n *NamespacedSyncMap) Filter(namespace string, filter func(k, v interface{}) bool) Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace].Filter(filter)
}

func (n *NamespacedSyncMap) Intersection(namespace1, namespace2 string) Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace1].Intersection(n.cacheMap[namespace2])
}

func (n *NamespacedSyncMap) Union(namespace1, namespace2 string) Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace1].Union(n.cacheMap[namespace2])
}

func (n *NamespacedSyncMap) Map(namespace string) Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheMap[namespace].Map()
}

func (n *NamespacedSyncMap) SetAll(namespace string, m Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.cacheMap[namespace]; !ok {
		n.cacheMap[namespace] = &SyncCache{
			data: sync.Map{},
			once: sync.Once{},
		}

	}
	n.cacheMap[namespace].SetAll(m)
}

func (n *NamespacedSyncMap) Clear(namespace string) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if cache, ok := n.cacheMap[namespace]; ok {
		cache.Clear()
	}
}

func (n *NamespacedSyncMap) Close() {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		for _, c := range n.cacheMap {
			c.Close()
		}
		n.cacheMap = map[string]*SyncCache{}
	})
}

type SyncCache struct {
	// TODO: Convert this to use a better DB than a sync.map
	data sync.Map
	once sync.Once
}

func (c *SyncCache) Get(key interface{}) (interface{}, bool) {
	obj, exists := c.data.Load(key)

	if !exists {
		return nil, false
	}
	return obj, true
}

func (c *SyncCache) Set(key interface{}, value interface{}) {
	c.data.Store(key, value)
}

func (c *SyncCache) Range(f func(key string, value interface{}) bool) {
	c.data.Range(func(key interface{}, value interface{}) bool {
		return f(key.(string), value)
	})
}

func (c *SyncCache) Delete(key interface{}) {
	c.data.Delete(key)
}

func (c *SyncCache) Len() int {
	i := 0
	c.data.Range(func(key, value interface{}) bool {
		i++
		return true
	})
	return i
}

func (c *SyncCache) Exists(key interface{}) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *SyncCache) Close() {
	c.once.Do(func() {
		c.data = sync.Map{}
	})
}

func (c *SyncCache) Map() Node {
	data := make(map[string]interface{})
	c.Range(func(key string, value interface{}) bool {
		data[key] = value
		return true
	})
	return data
}

func (c *SyncCache) SetAll(m Node) {
	m.Range(func(k string, v interface{}) bool {
		c.Set(k, v)
		return true
	})
}

func (c *SyncCache) Intersection(other *SyncCache) Node {
	data := Node{}
	if c == nil {
		return data
	}
	if other != nil {
		c.Range(func(k string, v interface{}) bool {
			if other.Exists(v) {
				data.Set(k, v)
			}
			return true
		})
	}
	return data
}

func (c *SyncCache) Union(other *SyncCache) Node {
	data := Node{}
	if c != nil {
		c.Range(func(k string, v interface{}) bool {
			data.Set(k, v)
			return true
		})
	}
	if other != nil {
		other.Range(func(k string, v interface{}) bool {
			data.Set(k, v)
			return true
		})
	}
	return data
}

func (c *SyncCache) Copy() Node {
	data := Node{}
	if c == nil {
		return data
	}
	c.Range(func(k string, v interface{}) bool {
		data.Set(k, v)
		return true
	})
	return data
}

func (c *SyncCache) Filter(filter func(k, v interface{}) bool) Node {
	data := Node{}
	if c == nil {
		return data
	}
	c.Range(func(key string, value interface{}) bool {
		if filter(key, value) {
			data.Set(key, value)
		}
		return true
	})
	return data
}

func (c *SyncCache) Clear() {
	c.Range(func(key string, value interface{}) bool {
		c.Delete(key)
		return true
	})
}
