package io.dogre.aerospike;

/**
 * Aerospike Mock Server
 *
 * @author dogre
 */
public interface AerospikeServer {

    /**
     * Start Aerospike Mock Server.
     * <p>
     * Parameter 'namespaces' must be set. These are the names of namespace that Aerospike Server has. When connecting
     * to the Aerospike Server, the server informs the node information that has the namespace data. If parameter
     * 'namespaces' is not set, the client does not know which node the desired record exists and thus throws
     * Exception.
     *
     * @param host host
     * @param port port
     * @param namespaces the names of namespaces
     */
    void start(String host, int port, String... namespaces);

    /**
     * Whether Server is started.
     *
     * @return <code>true</code> if server is started, otherwise <code>false</code>.
     */
    boolean isStarted();

}
