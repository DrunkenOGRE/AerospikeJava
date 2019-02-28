package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Helper class for writing Aerospike Message.
 *
 * @author dogre
 */
public class ByteWriter {

    /**
     * Message version.
     */
    private long messageVersion = 2;

    /**
     * Message type.
     */
    private long messageType;

    /**
     * current length.
     */
    private int length;

    /**
     * list of byte array.
     */
    private List<byte[]> bytesList;

    /**
     * Constructor.
     *
     * @param messageType message type.
     */
    public ByteWriter(int messageType) {
        this.messageType = messageType;
        this.length = 0;
        this.bytesList = new ArrayList<>();
    }

    /**
     * Get current length.
     *
     * @return current length.
     */
    public int getLength() {
        return this.length;
    }

    /**
     * Write byte array, and increase length.
     *
     * @param bytes byte array.
     */
    public void writeBytes(byte[] bytes) {
        this.bytesList.add(bytes);
        this.length += bytes.length;
    }

    /**
     * Write {@link Header Aerospike Message Header}, and increase length.
     *
     * @param header header.
     */
    public void writeHeader(Header header) {
        byte[] bytes = new byte[Command.MSG_REMAINING_HEADER_SIZE];
        bytes[0] = Command.MSG_REMAINING_HEADER_SIZE;
        bytes[1] = (byte) header.getInfo1();
        bytes[2] = (byte) header.getInfo2();
        bytes[3] = (byte) header.getInfo3();
        bytes[5] = (byte) header.getResultCode();
        Buffer.intToBytes(header.getGeneration(), bytes, 6);
        Buffer.intToBytes(header.getExpiration(), bytes, 10);
        Buffer.intToBytes(header.getTtl(), bytes, 14);
        Buffer.shortToBytes(header.getFieldCount(), bytes, 18);
        Buffer.shortToBytes(header.getOperationCount(), bytes, 20);

        writeBytes(bytes);
    }

    /**
     * Write info, and increase length.
     *
     * @param name the name of info.
     * @param info the value of info.
     */
    public void writeInfo(String name, byte[] info) {
        int nameLength = Buffer.estimateSizeUtf8(name);
        int length = nameLength + 1 + info.length + 1;

        byte[] bytes = new byte[length];
        Buffer.stringToUtf8(name, bytes, 0);
        bytes[nameLength] = '\t';
        System.arraycopy(info, 0, bytes, nameLength + 1, info.length);
        bytes[length - 1] = '\n';

        writeBytes(bytes);
    }

    /**
     * Write {@link Operation}, and increase length.
     *
     * @param operation operation.
     */
    public void writeOperation(Operation operation) {
        int nameLength = operation.binName != null ? Buffer.estimateSizeUtf8(operation.binName) : 0;
        int valueLength = operation.value != null ? operation.value.estimateSize() : 0;

        byte[] bytes = new byte[8 + nameLength + valueLength];
        Buffer.intToBytes(4 + nameLength + valueLength, bytes, 0);
        bytes[4] = (byte) operation.type.protocolType;
        bytes[5] = (byte) operation.value.getType();
        bytes[7] = (byte) nameLength;
        Buffer.stringToUtf8(operation.binName, bytes, 8);
        if (operation.value != null) {
            operation.value.write(bytes, 8 + nameLength);
        }

        writeBytes(bytes);
    }

    /**
     * Write {@link Key}, and increase length.
     *
     * @param key key.
     */
    public void writeKey(Key key) {
        int namespaceLength = Buffer.estimateSizeUtf8(key.namespace);
        int digestLength = key.digest.length;

        byte[] bytes = new byte[5 + namespaceLength + 5 + digestLength];
        int offset = 0;
        // namespace
        Buffer.intToBytes(namespaceLength + 1, bytes, offset);
        offset += 4;
        bytes[offset] = FieldType.NAMESPACE;
        offset++;
        Buffer.stringToUtf8(key.namespace, bytes, offset);
        offset += namespaceLength;
        // digest
        Buffer.intToBytes(digestLength + 1, bytes, offset);
        offset += 4;
        bytes[offset] = FieldType.DIGEST_RIPE;
        offset++;
        System.arraycopy(key.digest, 0, bytes, offset, digestLength);

        writeBytes(bytes);
    }

    /**
     * Estimate the length of record.
     *
     * @param key the key of record.
     * @param bins the bins of record.
     * @param binNames bin names to be written. if <code>null</code>, write all bins.
     * @param noBinData flag for exclude bins. if <code>true</code>, write no bins.
     * @return the length of record.
     */
    public static int estimateRecord(Key key, Map<String, Value> bins, Set<String> binNames, boolean noBinData) {
        int length = 0;

        // fields
        int namespaceLength = Buffer.estimateSizeUtf8(key.namespace);
        int digestLength = key.digest.length;
        length += 5 + namespaceLength + 5 + digestLength;

        if (bins != null && !noBinData) {
            // operations
            for (Entry<String, Value> entry : bins.entrySet()) {
                String name = entry.getKey();
                if (binNames != null && !binNames.contains(name)) {
                    continue;
                }
                Value value = entry.getValue();

                int nameLength = Buffer.estimateSizeUtf8(name);
                int valueLength = value.estimateSize();

                length += 8 + nameLength + valueLength;
            }
        }
        return length;
    }

    /**
     * Write record, and increase length.
     *
     * @param batchIndex batch index.
     * @param key the key of record.
     * @param bins the bins of record.
     * @param binNames bin names to be written. if <code>null</code>, write all bins.
     * @param noBinData flag for exclude bins. if <code>true</code>, write no bins.
     */
    public void writeRecord(int batchIndex, Key key, Map<String, Value> bins, Set<String> binNames, boolean noBinData) {
        Header header = new Header();
        header.setTtl(batchIndex);
        header.setFieldCount(2);

        int length = estimateRecord(key, bins, binNames, noBinData);

        byte[] bytes = new byte[length];

        int offset = 0;
        // fields
        // namespace
        int namespaceLength = Buffer.stringToUtf8(key.namespace, bytes, offset + 5);
        Buffer.intToBytes(namespaceLength + 1, bytes, offset);
        offset += 4;
        bytes[offset] = FieldType.NAMESPACE;
        offset++;
        offset += namespaceLength;
        // digest
        int digestLength = key.digest.length;
        System.arraycopy(key.digest, 0, bytes, offset + 5, digestLength);
        Buffer.intToBytes(digestLength + 1, bytes, offset);
        offset += 4;
        bytes[offset] = FieldType.DIGEST_RIPE;
        offset++;
        offset += digestLength;

        if (bins != null) {
            if (!noBinData) {
                // operations
                int operationCount = 0;
                for (Entry<String, Value> entry : bins.entrySet()) {
                    String name = entry.getKey();
                    if (binNames != null && !binNames.contains(name)) {
                        continue;
                    }
                    Value value = entry.getValue();

                    int nameLength = Buffer.stringToUtf8(name, bytes, offset + 8);
                    int valueLength = value.write(bytes, offset + 8 + nameLength);

                    Buffer.intToBytes(4 + nameLength + valueLength, bytes, offset);
                    offset += 4;
                    bytes[offset] = 1;
                    offset++;
                    bytes[offset] = (byte) value.getType();
                    offset += 2;
                    bytes[offset] = (byte) nameLength;
                    offset++;
                    offset += nameLength + valueLength;

                    operationCount++;
                }

                // operation count
                header.setOperationCount(operationCount);
            }
        } else {
            // result code
            header.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
        }

        writeHeader(header);
        writeBytes(bytes);
    }

    /**
     * Concatenate the byte arrays in {@link #bytesList}.
     * <p>
     * It just not only concatenate the byte arrays, but also prepend size header at front of byte array. So, the length
     * of response is 8 + length.
     *
     * @return byte array.
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[8 + this.length];
        int offset = 0;

        long sizeHeader = ((long) this.length) | (this.messageVersion << 56) | (this.messageType << 48);
        Buffer.longToBytes(sizeHeader, bytes, offset);
        offset += 8;
        for (byte[] b : this.bytesList) {
            System.arraycopy(b, 0, bytes, offset, b.length);
            offset += b.length;
        }
        return bytes;
    }

}
