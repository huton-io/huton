# huton
Huton is an embeddable and distributed in-memory key-value store inspired by the likes of [Redis](https://redis.io/) and [memcached](https://memcached.org/) and written in the Go programming language. Data is stored in a replicated [boltdb](https://github.com/boltdb/bolt) database. This database is a single, memory-mapped file providing extremely quick access to off-heap memory.

Replication is handled via the [Raft](https://raft.github.io/) consensus algorithm and data is replicated to all members of the cluster. Existing cluster members will see near-immediate consistency, while new members coming online will see eventual consistency.

Huton is considered **prototype** status, and is not yet indended for production use. It is under active development and welcomes contributions.

# Example Usage
Huton currently expects a quorum of nodes before the cluster is considered stable. This means that you need _at least_ 3 nodes to complete a cluster.
To begin, start two huton agents:
```bash
$ huton agent -name agent1 -bindPort 8100
$ huton agent -name agent2 -bindPort 8200 -peers 127.0.0.1:8100
```
Huton requires that the name of each node in a cluster be unique. If one or more nodes have the same name, this can cause havoc with the internal peer list.

It should also be noted that even though in this example, we are seeding the cluster with huton agents, the agents aren't really needed at all. A cluster can (and probably will, in a real world scenario) consist entirely of huton instances embedded in an application.

Once the two agents are started, only one more node is needed to reach quorum. If you run the below Go code, it will join the cluster and provide an HTTP resource to get and put data.
```go
package main

import (
	"strings"
	"net/http"
	"github.com/jonbonazza/huton/lib"
	"flag"
)

func main() {
	var instanceName string
	var peers string
	var port int
	flag.StringVar(&instanceName, "name", "", "unique instance name")
	flag.StringVar(&peers, "peers", "", "comma delimited list of seed peers")
	flag.IntVar(&port, "port", 8100, "bind port")
	flag.Parse()
	if instanceName == "" {
		panic("No instance name provided")
	}
	if peers == "" {
		panic("No peers provided.")
	}
	config := huton.DefaultConfig()
	config.Peers = strings.Split(peers, ",")
	config.BindPort = port
	instance, err := huton.NewInstance(instanceName, config)
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/", handler(instance))
	
	go func () {
		if err = http.ListenAndServe("127.0.0.1:8080", nil); err != nil {
			panic(err)
		}
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-signalCh
	c.instance.Leave()
	c.instance.Shutdown()
}

func handler(instance huton.Instance) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		bucket, err := instance.Bucket("exampleBucket")
		if err != nil {
			http.Error(w, "Failed to get bucket.", http.StatusInternalServerError)
			return
		}
		switch req.Method {
		case http.MethodGet:
			if err = bucket.Get([]byte("key"), func(val []byte {
				w.Write(val)
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case http.MethodPost:
			if err = bucket.Set([]byte("key"), []byte("value")); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
}

```
The above code example illustrates how you can embed Huton within an application.

Once all three nodes are started and connected to each other, put some data:
```bash
curl -v -X POST http://127.0.0.1:8080/
```
Now retrieve the data:
```bash
curl -v http://127.0.0.1:8080/
```
The HTTP resource should return the text _value_.

