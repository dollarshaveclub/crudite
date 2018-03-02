package main

import (
	"context"
	"fmt"

	"github.com/dollarshaveclub/crudite"
)

func main() {

	// Connect to brokers kafka[1,2].example.com and use topic "crudite" (must already exist)
	ops := crudite.Options{
		Brokers: []string{"kafka1.example.com", "kakfa2.example.com"},
		Topic:   "crudite",
	}

	tm, _ := crudite.NewTypeManager(ops)

	// Initialize the type manager with any datatypes that already exist in the Kafka log
	tm.Init(context.Background())

	// Get all existing CRDTs
	dtm := tm.Contains()
	for n, dt := range dtm {
		fmt.Printf("name: %v; data type: %v\n", n, dt.Type.String())
	}

	// Create a counter. It will update in the background over time as other nodes increment/decrement it.
	c := tm.NewCounter("mycounter")

	// or get an existing counter or return error if it doesn't exist
	// c, err := tm.GetCounter("mycounter")

	// Increment it
	c.Increment(4)

	// Decrement it
	c.Increment(-2)

	// Get the current value.
	val := c.Value()
	fmt.Printf("counter: %v", val)

	// Create a set
	s := tm.NewSet("myset")

	// Add an element
	s.Add([]byte("1234"))

	// Check if an element exists
	if s.Contains([]byte("1234")) {
		fmt.Println("1234 exists in set")
	}

	// Get all elements
	elements := s.Elements()
	for _, e := range elements {
		fmt.Printf("element: %v\n", e)
	}

	// Shut down
	tm.Stop()
}
