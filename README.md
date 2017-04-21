# huton
Huton is an embeddable and distributed in-memory datastore inspired by the likes of [Hazelcast](https://hazelcast.com/) and [Apache Ignite](https://ignite.apache.org/) and written in the Go programming language. Data is stored in a replicated [boltdb](https://github.com/boltdb/bolt) database. This database is a single, memory-mapped file providing extremely quick access to off-heap memory.

Replication is handled via the [Raft](https://raft.github.io/) consensus algorithm and data is replicated to all members of the cluster. Existing cluster members will see near-immediate consistency, while new members coming online will see eventual consistency.

Huton is considered **prototype** status, and is not yet indended for production. It is under active development and welcomes contributions.
