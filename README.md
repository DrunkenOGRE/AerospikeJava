## Aerospike Server for Integration Test

This is a simple Aerospike Server for the integration test.
This does not support full functions of Aerospike Server. This supports some commands belows. And  also this does not support functions for lifecycle (ttl, generation, expiration).

* Get
* Get Header
* Batch Get
* Batch Get Header
* Exists
* Put
* Append
* Prepend
* Add
* Touch
* Delete

### Run
Just create `AerospikeServer` and run.
```
AerospikeServer server = new NettyAerospikeServer(1, 10);
server.start("localhost", 3000, "namespace1", "namespace2");
```
`NettyAerospikeServer` is one of implementation. It needs # of IO threads, and # of Worker threads.
```
public NettyAerospikeServer(int ioThreads, int workerThreads)
```
And when start, you have to pass host, port, namespaces.
```
void start(String host, int port, String... namespaces)
```
Parameter 'namespaces' must be set.
These are the names of namespace that Aerospike Server has.
When connecting to the Aerospike Server, the server informs the node information that has the namespace data.
If parameter 'namespaces' is not set, the client does not know which node the desired record exists and thus throws Exception.