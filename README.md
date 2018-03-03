# Crudite 🥒🥕🌶
[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs]
[![CircleCI](https://circleci.com/gh/dollarshaveclub/crudite.svg?style=svg)](https://circleci.com/gh/dollarshaveclub/crudite)

[godocs]: https://godoc.org/github.com/dollarshaveclub/crudite

Crudite is a Go library for creating and managing CRDTs<sup>[1](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)</sup> (specifically CmRDTs) using Kafka as a datastore backend.

# What are CRDTs? (tl;dr)

CRDTs are data structures shared among multiple distributed nodes that provide eventual consistency without central coordination.

# Why use CRDTs?

CRDTs are useful when you need distributed systems to share a data structure and eventual consistency is acceptable. Lack
of central coordination means that writes scale horizontally without a single point of failure. Reads are entirely local to the node.

# Supported CRDTs

- PN Counter (Positive/Negative Counter)
- LWW Set (Last-Write-Wins Set)

# How is Kafka used?

Kafka provides a distributed, replicated log which is utilized as a broadcast mechanism for data structure operations. Data structure IDs are used as the
partition key to leveraging Kafka ordering guarantees. All operations for a particular data structure are within a single Kafka partition.

# How should Kafka be configured?

You need a single dedicated topic on a Kafka cluster of version 0.8+.

Topic settings:

- Replication: a good idea but not strictly necessary. Ideal replica count depends upon desired durability and required read throughput.
- Log cleanup: must be time-based, retention period depends upon operation volume vs available storage. Retention must be greater than SnapshotDuration.
- Partitions: the ideal number of partitions will depend upon the number of created data structures. There is no point in having more partitions than data structures as the additional partitions will be unused. The ideal number of data structures per partition depends upon operation volume.

# Example

```go
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
```
