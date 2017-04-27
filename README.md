# huton
Huton is an embeddable and distributed in-memory datastore inspired by the likes of [Hazelcast](https://hazelcast.com/) and [Apache Ignite](https://ignite.apache.org/) and written in the Go programming language. Data is stored in a replicated [boltdb](https://github.com/boltdb/bolt) database. This database is a single, memory-mapped file providing extremely quick access to off-heap memory.

Replication is handled via the [Raft](https://raft.github.io/) consensus algorithm and data is replicated to all members of the cluster. Existing cluster members will see near-immediate consistency, while new members coming online will see eventual consistency.

Huton is considered **prototype** status, and is not yet indended for production. It is under active development and welcomes contributions.

# Example Usage
```go
import (
	"fmt"
	"github.com/jonbonazza/huton/lib"
)

func main() {
	var instanceName string
	config := huton.DefaultConfig()
	instance, err := huton.NewInstance(instanceName, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := instance.Shutdown(); err != nil {
			panic(err)
		}
	}()
	bucket, err := instance.Bucket("exampleBucket")
	if err != nil {
		panic(err)
	}
	// Sets foo to bar and replicates to other nodes
	if err = bucket.Set([]byte("foo"), []byte("bar")); err != nil {
		panic(err)
	}
	// Retrieves the value for "foo" and prints it.
	if err = bucket.Get([]byte("foo"), func(val []byte) {
		// val is only valid within the scope of this function.
		fmt.Println(string(val))
	}); err != nil {
		panic(err)
	}
}
```
