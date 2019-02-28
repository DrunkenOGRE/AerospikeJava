package io.dogre.aerospike;

/**
 * Service Handler that handles Aerospike Protocol.
 * <p>
 * This does not support full functions of Aerospike Server. This supports commands belows.
 * <ul>
 * <li>Get</li>
 * <li>Get Header</li>
 * <li>Batch Get</li>
 * <li>Batch Get Header</li>
 * <li>Exists</li>
 * <li>Put</li>
 * <li>Append</li>
 * <li>Prepend</li>
 * <li>Add</li>
 * <li>Touch</li>
 * <li>Delete</li>
 * </ul>
 * And also this does not support functions for lifecycle (ttl, generation, expiration).
 *
 * @author dogre
 */
public interface ServiceHandler {

    /**
     * Handle Aerospike Request
     *
     * @param request request
     * @return response
     */
    byte[] handleRequest(byte[] request);

}
