package io.dogre.aerospike;

public interface ServiceHandler {

    byte[] handleRequest(byte[] request);

}
