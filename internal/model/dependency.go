package model

// DependencyType represents the type of dependency between transactions.
type DependencyType int

const (
	DepWR DependencyType = iota // Write-Read dependency
	DepWW                       // Write-Write dependency
	DepRW                       // Read-Write (anti-dependency)
)

func (d DependencyType) String() string {
	switch d {
	case DepWR:
		return "WR"
	case DepWW:
		return "WW"
	case DepRW:
		return "RW"
	default:
		return "UNKNOWN"
	}
}

// Edge represents a dependency edge between two transaction templates.
type Edge struct {
	From string         // Source template name
	To   string         // Target template name
	Type DependencyType // Dependency type
	Key  string         // The key causing the dependency
}

// DependencyGraph is a graph where nodes are Templates and edges are dependencies.
// Used to detect cycles for serializability analysis.
type DependencyGraph struct {
	nodes map[string]*TransactionTemplate // Template name -> Template
	edges []Edge
	adj   map[string][]Edge // Adjacency list: from -> edges
}

// NewDependencyGraph creates a new dependency graph.
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		nodes: make(map[string]*TransactionTemplate),
		edges: make([]Edge, 0),
		adj:   make(map[string][]Edge),
	}
}

// AddTemplate adds a transaction template to the graph.
func (g *DependencyGraph) AddTemplate(t *TransactionTemplate) {
	g.nodes[t.Name] = t
	if _, ok := g.adj[t.Name]; !ok {
		g.adj[t.Name] = make([]Edge, 0)
	}
}

// AddEdge adds a dependency edge to the graph.
func (g *DependencyGraph) AddEdge(from, to string, depType DependencyType, key string) {
	edge := Edge{From: from, To: to, Type: depType, Key: key}
	g.edges = append(g.edges, edge)
	g.adj[from] = append(g.adj[from], edge)
}

// BuildFromTemplates analyzes templates and builds dependency edges.
func (g *DependencyGraph) BuildFromTemplates() {
	templates := make([]*TransactionTemplate, 0, len(g.nodes))
	for _, t := range g.nodes {
		templates = append(templates, t)
	}

	for i := 0; i < len(templates); i++ {
		for j := 0; j < len(templates); j++ {
			if i == j {
				continue
			}
			t1, t2 := templates[i], templates[j]

			// WR: t1 writes, t2 reads
			for wk := range t1.StaticWrites {
				if t2.HasRead(wk) {
					g.AddEdge(t1.Name, t2.Name, DepWR, wk)
				}
			}

			// WW: t1 writes, t2 writes
			for wk := range t1.StaticWrites {
				if t2.HasWrite(wk) {
					g.AddEdge(t1.Name, t2.Name, DepWW, wk)
				}
			}

			// RW: t1 reads, t2 writes
			for rk := range t1.StaticReads {
				if t2.HasWrite(rk) {
					g.AddEdge(t1.Name, t2.Name, DepRW, rk)
				}
			}
		}
	}
}

// HasCycle detects if there is a cycle in the dependency graph.
func (g *DependencyGraph) HasCycle() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(node string) bool
	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true

		for _, edge := range g.adj[node] {
			if !visited[edge.To] {
				if dfs(edge.To) {
					return true
				}
			} else if recStack[edge.To] {
				return true
			}
		}

		recStack[node] = false
		return false
	}

	for node := range g.nodes {
		if !visited[node] {
			if dfs(node) {
				return true
			}
		}
	}
	return false
}

// FindCycles returns all cycles in the graph.
func (g *DependencyGraph) FindCycles() [][]string {
	var cycles [][]string
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	path := make([]string, 0)

	var dfs func(node string)
	dfs = func(node string) {
		visited[node] = true
		recStack[node] = true
		path = append(path, node)

		for _, edge := range g.adj[node] {
			if !visited[edge.To] {
				dfs(edge.To)
			} else if recStack[edge.To] {
				// Found cycle, extract it
				cycleStart := -1
				for i, n := range path {
					if n == edge.To {
						cycleStart = i
						break
					}
				}
				if cycleStart >= 0 {
					cycle := make([]string, len(path)-cycleStart)
					copy(cycle, path[cycleStart:])
					cycles = append(cycles, cycle)
				}
			}
		}

		path = path[:len(path)-1]
		recStack[node] = false
	}

	for node := range g.nodes {
		if !visited[node] {
			dfs(node)
		}
	}
	return cycles
}

// Edges returns all edges in the graph.
func (g *DependencyGraph) Edges() []Edge {
	return g.edges
}

// Templates returns all templates in the graph.
func (g *DependencyGraph) Templates() []*TransactionTemplate {
	templates := make([]*TransactionTemplate, 0, len(g.nodes))
	for _, t := range g.nodes {
		templates = append(templates, t)
	}
	return templates
}
