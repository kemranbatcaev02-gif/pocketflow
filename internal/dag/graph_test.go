package dag

import (
	"reflect"
	"strings"
	"testing"
)

func TestGraph_LinearOrder(t *testing.T) {
	g := NewGraph()
	// A -> B -> C
	_ = g.AddNode("A")
	_ = g.AddNode("B", "A")
	_ = g.AddNode("C", "B")

	order, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"A", "B", "C"}
	if !reflect.DeepEqual(order, expected) {
		t.Errorf("got %v, want %v", order, expected)
	}

	stages, err := g.ComputeStages()
	if err != nil {
		t.Fatalf("unexpected error in ComputeStages: %v", err)
	}

	expectedStages := [][]string{
		{"A"},
		{"B"},
		{"C"},
	}
	if !reflect.DeepEqual(stages, expectedStages) {
		t.Errorf("got stages %v, want %v", stages, expectedStages)
	}
}

func TestGraph_DiamondParallelStages(t *testing.T) {
	g := NewGraph()
	// Start -> (Process1, Process2) -> Aggregator
	_ = g.AddNode("Start")
	_ = g.AddNode("Process1", "Start")
	_ = g.AddNode("Process2", "Start")
	_ = g.AddNode("Aggregator", "Process1", "Process2")

	stages, err := g.ComputeStages()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedStages := [][]string{
		{"Start"},
		{"Process1", "Process2"},
		{"Aggregator"},
	}
	if !reflect.DeepEqual(stages, expectedStages) {
		t.Errorf("got stages %v, want %v", stages, expectedStages)
	}
}

func TestGraph_CycleDetection(t *testing.T) {
	g := NewGraph()
	// A -> B -> C -> A
	_ = g.AddNode("A", "C")
	_ = g.AddNode("B", "A")
	_ = g.AddNode("C", "B")

	err := g.Validate()
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}

	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("expected error to mention 'cycle detected', got %q", err.Error())
	}
}

func TestGraph_SelfDependency(t *testing.T) {
	g := NewGraph()
	err := g.AddNode("A", "A")
	if err == nil {
		t.Fatal("expected self-dependency error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot depend on itself") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGraph_MissingDependency(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("A", "NonExistent")

	err := g.Validate()
	if err == nil {
		t.Fatal("expected missing dependency error, got nil")
	}
	if !strings.Contains(err.Error(), "depends on non-existent step") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGraph_DisjointParallelNodes(t *testing.T) {
	g := NewGraph()
	// Two independent pipelines: (A -> B) and (C -> D)
	_ = g.AddNode("A")
	_ = g.AddNode("B", "A")
	_ = g.AddNode("C")
	_ = g.AddNode("D", "C")

	stages, err := g.ComputeStages()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedStages := [][]string{
		{"A", "C"},
		{"B", "D"},
	}
	if !reflect.DeepEqual(stages, expectedStages) {
		t.Errorf("got stages %v, want %v", stages, expectedStages)
	}
}
