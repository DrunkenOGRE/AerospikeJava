package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Operation.Type;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.FieldType;

import java.util.*;

public class ByteReader {

    private byte[] bytes;

    private int offset;

    public ByteReader(byte[] bytes) {
        this.bytes = bytes;
        this.offset = 0;
    }

    public int getLength() {
        return this.bytes.length;
    }

    public void skip(int length) {
        this.offset += length;
    }

    public byte readByte() {
        byte value = this.bytes[this.offset];
        this.offset++;
        return value;
    }

    public int readShort() {
        int value = Buffer.bytesToShort(this.bytes, this.offset);
        this.offset += 2;
        return value;
    }

    public int readInt() {
        int value = Buffer.bytesToInt(this.bytes, this.offset);
        this.offset += 4;
        return value;
    }

    public long readLong() {
        long value = Buffer.bytesToLong(this.bytes, this.offset);
        this.offset += 8;
        return value;
    }

    public String readUtf8String(int length) {
        String value = Buffer.utf8ToString(this.bytes, this.offset, length);
        this.offset += length;
        return value;
    }

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

    public void readBytes(byte[] bytes) {
        System.arraycopy(this.bytes, this.offset, bytes, 0, bytes.length);
        this.offset += bytes.length;
    }

    public Value readKeyValue(int length) {
        int type = readByte();
        Value keyValue = Buffer.bytesToKeyValue(type, this.bytes, this.offset, length);
        this.offset += length;
        return keyValue;
    }

    public Object readParticle(int type, int length) {
        Object particle = Buffer.bytesToParticle(type, this.bytes, this.offset, length);
        this.offset += length;
        return particle;
    }

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

    private static final Map<Integer, Type> OPERATION_TYPE_MAP = new HashMap<>();

    static {
        for (Type type : Type.values()) {
            OPERATION_TYPE_MAP.put(type.protocolType, type);
        }
    }

    public static Type findOperationType(int protocolType) {
        return OPERATION_TYPE_MAP.get(protocolType);
    }

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

    public List<Operation> readOperations(int operationCount) {
        List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < operationCount; i++) {
            operations.add(readOperation());
        }
        return operations;
    }

    public Set<String> readBinNames(int operationCount) {
        Set<String> binNames = new HashSet<>();
        List<Operation> operations = readOperations(operationCount);
        for (Operation operation : operations) {
            binNames.add(operation.binName);
        }
        return binNames;
    }

}
