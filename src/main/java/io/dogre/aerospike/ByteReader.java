package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Operation.Type;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.ParticleType;

import java.util.*;

/**
 * Helper class for reading Aerospike Message.
 *
 * @author dogre
 */
public class ByteReader {

    /**
     * byte array.
     */
    private byte[] bytes;

    /**
     * current offset.
     */
    private int offset;

    /**
     * Constructor
     *
     * @param bytes byte array.
     */
    public ByteReader(byte[] bytes) {
        this.bytes = bytes;
        this.offset = 0;
    }

    /**
     * Get the length of byte array.
     *
     * @return The length of byte array.
     */
    public int getLength() {
        return this.bytes.length;
    }

    /**
     * Get current offset.
     *
     * @return Current offset.
     */
    public int getOffset() {
        return this.offset;
    }

    /**
     * skip some bytes.
     *
     * @param length The length of bytes to skip.
     */
    public void skip(int length) {
        this.offset += length;
    }

    /**
     * Read 1 byte, and increase offset by 1.
     *
     * @return byte.
     */
    public byte readByte() {
        byte value = this.bytes[this.offset];
        this.offset++;
        return value;
    }

    /**
     * Read short value, and increase offset by 2.
     *
     * @return short value.
     */
    public int readShort() {
        int value = Buffer.bytesToShort(this.bytes, this.offset);
        this.offset += 2;
        return value;
    }

    /**
     * Read integer value, and increase offset by 4.
     *
     * @return int value.
     */
    public int readInt() {
        int value = Buffer.bytesToInt(this.bytes, this.offset);
        this.offset += 4;
        return value;
    }

    /**
     * Read long value, and increase offset by 8.
     *
     * @return long value.
     */
    public long readLong() {
        long value = Buffer.bytesToLong(this.bytes, this.offset);
        this.offset += 8;
        return value;
    }

    /**
     * Read string value encoded in UTF-8, and increase offset by the length.
     *
     * @param length The length of bytes contains string encoded in UTF-8.
     * @return string value.
     */
    public String readUtf8String(int length) {
        String value = Buffer.utf8ToString(this.bytes, this.offset, length);
        this.offset += length;
        return value;
    }

    /**
     * Read {@link Header Aerospike Message Header}, and increase offset by 22.
     *
     * @return header.
     */
    public Header readHeader() {
        skip(1); // length
        int info1 = readByte();
        int info2 = readByte();
        int info3 = readByte();
        skip(1);
        int resultCode = readByte();
        int generation = readInt();
        int expiration = readInt();
        int ttl = readInt();
        int fieldCount = readShort();
        int operationCount = readShort();
        return new Header(info1, info2, info3, 0, resultCode, generation, expiration, ttl, fieldCount, operationCount);
    }

    /**
     * Read bytes, and increase offset by the length of byte array.
     *
     * @param bytes byte array.
     */
    public void readBytes(byte[] bytes) {
        System.arraycopy(this.bytes, this.offset, bytes, 0, bytes.length);
        this.offset += bytes.length;
    }

    /**
     * Read key value of {@link Key}.
     *
     * @param length the length of byte array contains key value.
     * @return key value.
     */
    public Value readKeyValue(int length) {
        int type = readByte();
        Value keyValue = Buffer.bytesToKeyValue(type, this.bytes, this.offset, length);
        this.offset += length;
        return keyValue;
    }

    /**
     * Read particle, and increase offset by the length.
     * <p>
     * The particle is the value of bins.
     *
     * @param type particle type.
     * @param length the length of byte array contains particle.
     * @return particle.
     * @see ParticleType
     * @see Value
     */
    public Object readParticle(int type, int length) {
        Object particle = Buffer.bytesToParticle(type, this.bytes, this.offset, length);
        this.offset += length;
        return particle;
    }

    /**
     * Read {@link Operation}, and increase offset by the length of operation.
     *
     * @return operation.
     */
    public Operation readOperation() {
        int length = readInt() - 4;
        int operationType = readByte();
        int valueType = readByte();
        skip(1);
        int nameLength = readByte();
        int valueLength = length - nameLength;

        String name = 0 < nameLength ? readUtf8String(nameLength) : null;
        Value value = 0 < valueLength ? Value.get(readParticle(valueType, valueLength)) : null;

        return new Operation(findOperationType(operationType), name, value);
    }

    /**
     * The map for finding the operation type by protocol type.
     */
    private static final Map<Integer, Type> OPERATION_TYPE_MAP = new HashMap<>();

    static {
        // Support READ, WRITE, ADD, APPEND, PREPEND, TOUCH only.
        for (Type type : new Type[] { Type.READ, Type.WRITE, Type.ADD, Type.APPEND, Type.PREPEND, Type.TOUCH }) {
            OPERATION_TYPE_MAP.put(type.protocolType, type);
        }
    }

    /**
     * Find {@link Type} of operation by protocol type.
     *
     * @param protocolType protocol type.
     * @return type of operation.
     */
    public static Type findOperationType(int protocolType) {
        return OPERATION_TYPE_MAP.get(protocolType);
    }

    /**
     * Read {@link Key}, and increase offset by the length of bytes contains Key.
     *
     * @param fieldCount The number of fields of Key.
     * @return Key.
     */
    public Key readKey(int fieldCount) {
        String namespace = null;
        String set = null;
        byte[] digest = null;
        Value userKey = null;
        for (int i = 0; i < fieldCount; i++) {
            int fieldSize = readInt() - 1;
            int fieldType = readByte();
            switch (fieldType) {
                case FieldType.NAMESPACE:
                    namespace = readUtf8String(fieldSize);
                    break;
                case FieldType.TABLE:
                    set = readUtf8String(fieldSize);
                    break;
                case FieldType.DIGEST_RIPE:
                    digest = new byte[fieldSize];
                    readBytes(digest);
                    break;
                case FieldType.KEY:
                    userKey = readKeyValue(fieldSize);
                    break;
            }
        }

        return new Key(namespace, digest, set, userKey);
    }

    /**
     * Read operations, and increase offset by the length of bytes.
     *
     * @param operationCount The number of operations.
     * @return List of {@link Operation}.
     */
    public List<Operation> readOperations(int operationCount) {
        List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < operationCount; i++) {
            operations.add(readOperation());
        }
        return operations;
    }

    /**
     * Read bin names, and increase offset by the length of bytes.
     *
     * @param operationCount The number of operations.
     * @return Set of bin names.
     */
    public Set<String> readBinNames(int operationCount) {
        Set<String> binNames = new HashSet<>();
        List<Operation> operations = readOperations(operationCount);
        for (Operation operation : operations) {
            binNames.add(operation.binName);
        }
        return binNames;
    }

}
