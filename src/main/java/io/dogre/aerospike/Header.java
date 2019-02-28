package io.dogre.aerospike;

import com.aerospike.client.command.Command;

/**
 * Aerospike Message Header.
 * <p>
 * <table>
 * <thead>
 * <tr>
 * <th>Offset</th>
 * <th>Size</th>
 * <th>Description</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>0</td>
 * <td>1</td>
 * <td>The length of Header, fixed to 22.</td>
 * </tr>
 * <tr>
 * <td>1</td>
 * <td>1</td>
 * <td>Info1, read attribute</td>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>1</td>
 * <td>Info2, write attribute</td>
 * </tr>
 * <tr>
 * <td>3</td>
 * <td>1</td>
 * <td>Info3, info attribute</td>
 * </tr>
 * <tr>
 * <td>4</td>
 * <td>1</td>
 * <td>unused</td>
 * </tr>
 * <tr>
 * <td>5</td>
 * <td>1</td>
 * <td>Result Code</td>
 * </tr>
 * <tr>
 * <td>6</td>
 * <td>4</td>
 * <td>Generation</td>
 * </tr>
 * <tr>
 * <td>10</td>
 * <td>4</td>
 * <td>Expiration</td>
 * </tr>
 * <tr>
 * <td>14</td>
 * <td>4</td>
 * <td>TTL. If batch get, batch index</td>
 * </tr>
 * <tr>
 * <td>18</td>
 * <td>2</td>
 * <td>field count</td>
 * </tr>
 * <tr>
 * <td>20</td>
 * <td>2</td>
 * <td>operation count</td>
 * </tr>
 * </tbody>
 * </table>
 *
 * @author dogre
 */
public class Header {

    /**
     * The length of Header, fixed to 22.
     */
    private int length;

    /**
     * Info1, read attribute.
     */
    private int info1;

    /**
     * Info2, write attribute.
     */
    private int info2;

    /**
     * Info3, info attribute.
     */
    private int info3;

    /**
     * unused
     */
    private int unused;

    /**
     * Result Code.
     */
    private int resultCode;

    /**
     * Generation.
     */
    private int generation;

    /**
     * Expiration.
     */
    private int expiration;

    /**
     * TTL. If batch get, batch index.
     */
    private int ttl;

    /**
     * Field count.
     */
    private int fieldCount;

    /**
     * Operation count.
     */
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

    /**
     * Whether info1 flag is set.
     *
     * @param flag info1 flag.
     * @return <code>true</code> if flag is set, otherwise <code>false</code>.
     */
    public boolean isInfo1Set(int flag) {
        return (this.info1 & flag) != 0;
    }

    /**
     * Whether info2 flag is set.
     *
     * @param flag info2 flag.
     * @return <code>true</code> if flag is set, otherwise <code>false</code>.
     */
    public boolean isInfo2Set(int flag) {
        return (this.info2 & flag) != 0;
    }

    /**
     * Whether info3 flag is set.
     *
     * @param flag info3 flag.
     * @return <code>true</code> if flag is set, otherwise <code>false</code>.
     */
    public boolean isInfo3Set(int flag) {
        return (this.info3 & flag) != 0;
    }

}
