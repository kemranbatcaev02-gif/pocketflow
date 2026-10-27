package dag

import (
	"fmt"
	"sort"
	"strings"
)

type Node struct {
	Name         string
	Dependencies []string
	Dependents   []string
}

type Graph struct {
	nodes map[string]*Node
}

func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*Node),
	}
}

func (g *Graph) AddNode(name string, dependencies ...string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("node name cannot be empty")
	}

	node, exists := g.nodes[name]
	if !exists {
		node = &Node{Name: name}
		g.nodes[name] = node
	}

	depSet := make(map[string]struct{})
	for _, d := range node.Dependencies {
		depSet[d] = struct{}{}
	}

	for _, dep := range dependencies {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}
		if dep == name {
			return fmt.Errorf("node %q cannot depend on itself", name)
		}
		if _, seen := depSet[dep]; !seen {
			node.Dependencies = append(node.Dependencies, dep)
			depSet[dep] = struct{}{}
		}
	}

	return nil
}

func (g *Graph) Validate() error {
	for name, node := range g.nodes {
		for _, dep := range node.Dependencies {
			if _, exists := g.nodes[dep]; !exists {
				return fmt.Errorf("node %q depends on non-existent step %q", name, dep)
			}
		}
	}

	for _, node := range g.nodes {
		node.Dependents = nil
	}
	for name, node := range g.nodes {
		for _, dep := range node.Dependencies {
			g.nodes[dep].Dependents = append(g.nodes[dep].Dependents, name)
		}
	}

	color := make(map[string]int)
	path := make([]string, 0)

	var detectCycle func(curr string) error
	detectCycle = func(curr string) error {
		color[curr] = 1
		path = append(path, curr)

		for _, dep := range g.nodes[curr].Dependencies {
			if color[dep] == 1 {
				cycleStart := 0
				for i, n := range path {
					if n == dep {
						cycleStart = i
						break
					}
				}
				cycleNodes := append(path[cycleStart:], dep)
				return fmt.Errorf("cycle detected: %s", strings.Join(cycleNodes, " -> "))
			}
			if color[dep] == 0 {
				if err := detectCycle(dep); err != nil {
					return err
				}
			}
		}

		path = path[:len(path)-1]
		color[curr] = 2
		return nil
	}

	for name := range g.nodes {
		if color[name] == 0 {
			if err := detectCycle(name); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *Graph) TopologicalSort() ([]string, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int)
	for name, node := range g.nodes {
		inDegree[name] = len(node.Dependencies)
	}

	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)

	var order []string
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		order = append(order, curr)

		for _, depName := range g.nodes[curr].Dependents {
			inDegree[depName]--
			if inDegree[depName] == 0 {
				queue = append(queue, depName)
			}
		}
		sort.Strings(queue)
	}

	if len(order) != len(g.nodes) {
		return nil, fmt.Errorf("topological sort failed: graph contains unresolved dependencies or cycles")
	}

	return order, nil
}

func (g *Graph) ComputeStages() ([][]string, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int)
	for name, node := range g.nodes {
		inDegree[name] = len(node.Dependencies)
	}

	var currentStage []string
	for name, deg := range inDegree {
		if deg == 0 {
			currentStage = append(currentStage, name)
		}
	}
	sort.Strings(currentStage)

	var stages [][]string
	processed := 0

	for len(currentStage) > 0 {
		stages = append(stages, currentStage)
		processed += len(currentStage)

		var nextStage []string
		for _, name := range currentStage {
			for _, depName := range g.nodes[name].Dependents {
				inDegree[depName]--
				if inDegree[depName] == 0 {
					nextStage = append(nextStage, depName)
				}
			}
		}

		sort.Strings(nextStage)
		currentStage = nextStage
	}

	if processed != len(g.nodes) {
		return nil, fmt.Errorf("failed to compute stages: cycle detected")
	}

	return stages, nil
}

func (g *Graph) GetDependencies(name string) ([]string, error) {
	node, exists := g.nodes[name]
	if !exists {
		return nil, fmt.Errorf("node %q not found in graph", name)
	}
	deps := make([]string, len(node.Dependencies))
	copy(deps, node.Dependencies)
	return deps, nil
}

func (g *Graph) GetDependents(name string) ([]string, error) {
	node, exists := g.nodes[name]
	if !exists {
		return nil, fmt.Errorf("node %q not found in graph", name)
	}
	deps := make([]string, len(node.Dependents))
	copy(deps, node.Dependents)
	return deps, nil
}

func (g *Graph) NodeCount() int {
	return len(g.nodes)
}
