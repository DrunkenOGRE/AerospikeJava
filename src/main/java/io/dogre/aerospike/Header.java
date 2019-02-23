package io.dogre.aerospike;

import com.aerospike.client.command.Command;

public class Header {

    private int length;

    private int info1; // read attribute

    private int info2; // write attribute

    private int info3; // info attribute

    private int unused;

    private int resultCode;

    private int generation;

    private int expiration;

    private int ttl; // batch index

    private int fieldCount;

    private int operationCount;

    public Header() {
        this.length = Command.MSG_REMAINING_HEADER_SIZE;
    }

    public Header(int info1, int info2, int info3, int unused, int resultCode, int generation, int expiration, int ttl,
            int fieldCount, int operationCount) {
        this();
        this.info1 = info1;
        this.info2 = info2;
        this.info3 = info3;
        this.unused = unused;
        this.resultCode = resultCode;
        this.generation = generation;
        this.expiration = expiration;
        this.ttl = ttl;
        this.fieldCount = fieldCount;
        this.operationCount = operationCount;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getInfo1() {
        return info1;
    }

    public void setInfo1(int info1) {
        this.info1 = info1;
    }

    public int getInfo2() {
        return info2;
    }

    public void setInfo2(int info2) {
        this.info2 = info2;
    }

    public int getInfo3() {
        return info3;
    }

    public void setInfo3(int info3) {
        this.info3 = info3;
    }

    public int getUnused() {
        return unused;
    }

    public void setUnused(int unused) {
        this.unused = unused;
    }

    public int getResultCode() {
        return resultCode;
    }

    public void setResultCode(int resultCode) {
        this.resultCode = resultCode;
    }

    public int getGeneration() {
        return generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    public int getOperationCount() {
        return operationCount;
    }

    public void setOperationCount(int operationCount) {
        this.operationCount = operationCount;
    }

}
