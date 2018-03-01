# Crudite 🥒🥕🌶
[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs]

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
