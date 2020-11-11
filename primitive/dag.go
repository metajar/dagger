package primitive

import (
	"fmt"
	"sync"
)

// Graph is a concurrency safe, mutable, in-memory directed graph
type Graph struct {
	mu        sync.RWMutex
	nodes     namespaceCache
	edges     namespaceCache
	edgesFrom namespaceCache
	edgesTo   namespaceCache
}

// NewGraphCacheMap creates a new sync.map backed cachemap.
func NewGraphCacheMap() *Graph {
	return &Graph{
		mu:        sync.RWMutex{},
		nodes:     newCacheMap(),
		edges:     newCacheMap(),
		edgesFrom: newCacheMap(),
		edgesTo:   newCacheMap(),
	}
}

func (g *Graph) EdgeTypes() []string {
	return g.edges.Namespaces()
}

func (g *Graph) NodeTypes() []string {
	return g.nodes.Namespaces()
}

func (g *Graph) AddNode(n Node) {
	if n.ID() == "" {
		n.SetID(UUID())
	}
	g.nodes.Set(n.Type(), n.ID(), n)
}

func (g *Graph) AddNodes(nodes ...Node) {
	for _, n := range nodes {
		g.AddNode(n)
	}
}
func (g *Graph) GetNode(id TypedID) (Node, bool) {
	val, ok := g.nodes.Get(id.Type(), id.ID())
	if ok {
		n, ok := val.(Node)
		if ok {
			return n, true
		}
	}
	return nil, false
}

func (g *Graph) RangeNodeTypes(typ Type, fn func(n Node) bool) {
	g.nodes.Range(typ.Type(), func(key string, val interface{}) bool {
		n, ok := val.(Node)
		if ok {
			if !fn(n) {
				return false
			}
		}
		return true
	})
}

func (g *Graph) RangeNodes(fn func(n Node) bool) {
	for _, namespace := range g.nodes.Namespaces() {
		g.nodes.Range(namespace, func(key string, val interface{}) bool {
			n, ok := val.(Node)
			if ok {
				if !fn(n) {
					return false
				}
			}
			return true
		})
	}
}

func (g *Graph) RangeEdges(fn func(e *Edge) bool) {
	for _, namespace := range g.edges.Namespaces() {
		g.edges.Range(namespace, func(key string, val interface{}) bool {
			e, ok := val.(*Edge)
			if ok {
				if !fn(e) {
					return false
				}
			}
			return true
		})
	}
}

func (g *Graph) RangeEdgeTypes(edgeType Type, fn func(e *Edge) bool) {
	g.edges.Range(edgeType.Type(), func(key string, val interface{}) bool {
		e, ok := val.(*Edge)
		if ok {
			if !fn(e) {
				return false
			}
		}
		return true
	})
}

func (g *Graph) HasNode(id TypedID) bool {
	_, ok := g.GetNode(id)
	return ok
}

func (g *Graph) DelNode(id TypedID) {
	if val, ok := g.edgesFrom.Get(id.Type(), id.ID()); ok {
		if val != nil {
			edges := val.(edgeMap)
			edges.Range(func(e *Edge) bool {
				g.DelEdge(e)
				return true
			})
		}
	}
	g.nodes.Delete(id.Type(), id.ID())
}

func (g *Graph) AddEdge(e *Edge) error {
	if e.ID() == "" {
		e.SetID(UUID())
	}
	if err := e.Validate(); err != nil {
		return err
	}
	if !g.HasNode(e.From) {
		return fmt.Errorf("node %s.%s does not exist", e.From.Type(), e.From.ID())
	}
	if !g.HasNode(e.To) {
		return fmt.Errorf("node %s.%s does not exist", e.To.Type(), e.To.ID())
	}
	g.edges.Set(e.Type(), e.ID(), e)
	if val, ok := g.edgesFrom.Get(e.From.Type(), e.From.ID()); ok {
		edges := val.(edgeMap)
		edges.AddEdge(e)
		g.edgesFrom.Set(e.From.Type(), e.From.ID(), edges)
	} else {
		edges := edgeMap{}
		edges.AddEdge(e)
		g.edgesFrom.Set(e.From.Type(), e.From.ID(), edges)
	}
	if val, ok := g.edgesTo.Get(e.To.Type(), e.To.ID()); ok {
		edges := val.(edgeMap)
		edges.AddEdge(e)
		g.edgesTo.Set(e.To.Type(), e.To.ID(), edges)
	} else {
		edges := edgeMap{}
		edges.AddEdge(e)
		g.edgesTo.Set(e.To.Type(), e.To.ID(), edges)
	}
	return nil
}

func (g *Graph) AddEdges(edges ...*Edge) error {
	for _, e := range edges {
		if err := g.AddEdge(e); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) HasEdge(id TypedID) bool {
	_, ok := g.GetEdge(id)
	return ok
}

func (g *Graph) GetEdge(id TypedID) (*Edge, bool) {
	val, ok := g.edges.Get(id.Type(), id.ID())
	if ok {
		e, ok := val.(*Edge)
		if ok {
			return e, true
		}
	}
	return nil, false
}

func (g *Graph) DelEdge(id TypedID) {
	val, ok := g.edges.Get(id.Type(), id.ID())
	if ok && val != nil {
		edge := val.(*Edge)
		fromVal, ok := g.edgesFrom.Get(edge.From.Type(), edge.From.ID())
		if ok && fromVal != nil {
			edges := fromVal.(edgeMap)
			edges.DelEdge(id)
			g.edgesFrom.Set(edge.From.Type(), edge.From.ID(), edges)
		}
		toVal, ok := g.edgesTo.Get(edge.To.Type(), edge.To.ID())
		if ok && toVal != nil {
			edges := toVal.(edgeMap)
			edges.DelEdge(id)
			g.edgesTo.Set(edge.To.Type(), edge.To.ID(), edges)
		}
	}
	g.edges.Delete(id.Type(), id.ID())
}

func (g *Graph) EdgesFrom(edgeType Type, id TypedID, fn func(e *Edge) bool) {
	val, ok := g.edgesFrom.Get(id.Type(), id.ID())
	if ok {
		if edges, ok := val.(edgeMap); ok {
			edges.RangeType(edgeType, func(e *Edge) bool {
				return fn(e)
			})
		}
	}
}

func (g *Graph) EdgesTo(edgeType Type, id TypedID, fn func(e *Edge) bool) {
	val, ok := g.edgesTo.Get(id.Type(), id.ID())
	if ok {
		if edges, ok := val.(edgeMap); ok {
			edges.RangeType(edgeType, func(e *Edge) bool {
				return fn(e)
			})
		}
	}
}

func (g *Graph) Export() *Export {
	exp := &Export{}
	g.RangeNodes(func(n Node) bool {
		exp.Nodes = append(exp.Nodes, n)
		return true
	})
	g.RangeEdges(func(e *Edge) bool {
		exp.Edges = append(exp.Edges, e)
		return true
	})
	return exp
}

func (g *Graph) Import(exp *Export) error {
	for _, n := range exp.Nodes {
		g.AddNode(n)
	}
	for _, e := range exp.Edges {
		g.AddEdge(e)
	}
	return nil
}

func (g *Graph) Close() {
	g.nodes.Close()
	g.edgesTo.Close()
	g.edgesFrom.Close()
	g.edges.Close()
}
