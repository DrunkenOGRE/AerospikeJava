package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Operation.Type;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.ParticleType;

import java.util.*;
import java.util.Map.Entry;

public class ServiceHandlerImpl implements ServiceHandler {

    protected Map<String, byte[]> infos = new HashMap<>();

    protected Map<Key, Map<String, Value>> records = new HashMap<>();

    public ServiceHandlerImpl(String service, String... namespaces) {
        Map<String, String> map = new HashMap<>();
        map.put("node", "BB9E152A39B2100");
        map.put("partition-generation", "1");
        map.put("features",
                "peers;cdt-list;cdt-map;pipelining;geo;float;batch-index;replicas-all;replicas-master;replicas-prole;udf;");
        map.put("service-clear-std", service);
        map.put("peers-generation", "1");
        map.put("peers-clear-std", "1,,[]");
        StringBuilder builder = new StringBuilder();
        for (String namespace : namespaces) {
            if (0 < builder.length()) {
                builder.append(",");
            }
            builder.append(namespace)
                    .append(":1,//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8=");
        }
        map.put("replicas-all", builder.toString());
        map.put("service", service);
        map.put("services", service);
        map.put("version", "Aerospike Enterprise Edition 3.5.14");
        for (Entry<String, String> entry : map.entrySet()) {
            int length = Buffer.estimateSizeUtf8(entry.getValue());
            byte[] bytes = new byte[length];
            Buffer.stringToUtf8(entry.getValue(), bytes, 0);
            this.infos.put(entry.getKey(), bytes);
        }
    }

    @Override
    public byte[] handleRequest(byte[] request) {
        ByteReader reader = new ByteReader(request);

        long sizeHeader = reader.readLong();
        int type = (int) (sizeHeader >> 48) & 0xff;
        // long length = sizeHeader & 0xffffffffffffL;

        ByteWriter writer = new ByteWriter(type);

        if (type == 1) {
            handleInfo(reader, writer);
        } else {
            Header header = reader.readHeader();
            int readAttr = header.getInfo1();
            if (header.isInfo1Set(Command.INFO1_READ) && header.isInfo1Set(Command.INFO1_BATCH)) {
                handleBatchGet(header, reader, writer);
            } else {
                handleOperations(header, reader, writer);
            }
        }

        return writer.toBytes();
    }

    protected void handleInfo(ByteReader reader, ByteWriter writer) {
        StringTokenizer tokenizer = new StringTokenizer(reader.readUtf8String(reader.getLength() - 8), "\n");
        while (tokenizer.hasMoreTokens()) {
            String key = tokenizer.nextToken();
            if (this.infos.containsKey(key)) {
                writer.writeInfo(key, this.infos.get(key));
            }
        }
    }

    protected void handleBatchGet(Header header, ByteReader reader, ByteWriter writer) {
        reader.skip(4); // field size
        int fieldType = reader.readByte();
        boolean sendSetName = (fieldType == FieldType.BATCH_INDEX_WITH_SET);
        boolean noBinData = header.isInfo1Set(Command.INFO1_NOBINDATA);

        int keyCount = reader.readInt();
        reader.skip(1); // allowInline

        String namespace = null;
        String set = null;
        Set<String> binNames = null;
        for (int i = 0; i < keyCount; i++) {
            int index = reader.readInt();
            byte[] digest = new byte[20];
            reader.readBytes(digest);
            boolean repeat = (reader.readByte() == 1);
            if (!repeat) {
                int readAttribute = reader.readByte();
                int fieldCount = reader.readShort();
                int operationCount = reader.readShort();
                int fieldSize = reader.readInt() - 1;
                reader.skip(1);
                namespace = reader.readUtf8String(fieldSize);
                if (sendSetName) {
                    fieldSize = reader.readInt() - 1;
                    reader.skip(1);
                    set = reader.readUtf8String(fieldSize);
                }
                binNames = null;
                if (0 < operationCount) {
                    binNames = reader.readBinNames(operationCount);
                }
            }

            Key key = new Key(namespace, digest, set, null);
            writer.writeRecord(index, key, this.records.get(key), binNames, noBinData);
        }

        Header lastHeader = new Header();
        lastHeader.setInfo3(Command.INFO3_LAST);
        writer.writeHeader(lastHeader);
    }

    protected static final Operation GET_ALL_OPERATION = Operation.get();

    protected void handleOperations(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());
        Map<String, Value> current = this.records.get(key);
        List<Operation> operations = reader.readOperations(header.getOperationCount());
        int remainds = reader.getLength() - reader.getOffset();

        boolean hasRead = header.isInfo1Set(Command.INFO1_READ);
        boolean hasWrite = header.isInfo2Set(Command.INFO2_WRITE);
        boolean createOnly = hasWrite && header.isInfo2Set(Command.INFO2_CREATE_ONLY);
        boolean mustRecordExists = hasWrite &&
                (header.isInfo3Set(Command.INFO3_UPDATE_ONLY) || header.isInfo3Set(Command.INFO3_REPLACE_ONLY));
        boolean replace = hasWrite &&
                (header.isInfo3Set(Command.INFO3_CREATE_OR_REPLACE) || header.isInfo3Set(Command.INFO3_REPLACE_ONLY));
        boolean noBinData = header.isInfo1Set(Command.INFO1_NOBINDATA);
        boolean writeKey = hasRead && (!noBinData || 0 < remainds);

        int resultCode = ResultCode.OK;
        if (createOnly && current != null) {
            resultCode = ResultCode.KEY_EXISTS_ERROR;
        } else if (mustRecordExists && current == null) {
            resultCode = ResultCode.KEY_NOT_FOUND_ERROR;
        } else if (hasRead && !hasWrite && current == null) {
            resultCode = ResultCode.KEY_NOT_FOUND_ERROR;
        }

        List<Operation> responseOperations = new ArrayList<>();
        if (header.isInfo2Set(Command.INFO2_DELETE)) {
            if (this.records.containsKey(key)) {
                this.records.remove(key);
            } else {
                resultCode = ResultCode.KEY_NOT_FOUND_ERROR;
            }
        } else if (resultCode == ResultCode.OK) {
            Map<String, Value> next;
            if (hasWrite) {
                if (current == null || replace) {
                    next = new HashMap<>();
                } else {
                    next = new HashMap<>(current);
                }
            } else {
                next = current;
            }

            if (header.isInfo1Set(Command.INFO1_GET_ALL)) {
                operations.add(GET_ALL_OPERATION);
            }
            boolean hasGetHeader = false;
            boolean hasFullGet = false;
            for (Operation operation : operations) {
                if (resultCode != ResultCode.OK) {
                    break;
                }

                String binName = operation.binName;
                Value opValue = operation.value;
                int opType = opValue != null ? opValue.getType() : ParticleType.NULL;

                switch (operation.type) {
                    case READ:
                        if (hasFullGet) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else if (binName == null && hasGetHeader) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else if (!noBinData) {
                            if (binName != null) {
                                if (next.containsKey(binName)) {
                                    responseOperations.add(new Operation(Type.READ, binName, next.get(binName)));
                                }
                            } else {
                                for (Entry<String, Value> entry : next.entrySet()) {
                                    responseOperations.add(new Operation(Type.READ, entry.getKey(), entry.getValue()));
                                }
                                hasFullGet = true;
                            }
                        }
                        break;
                    case WRITE:
                        if (opType == ParticleType.NULL) {
                            next.remove(binName);
                        } else {
                            next.put(binName, operation.value);
                        }
                        break;
                    case ADD:
                        if (replace) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else if (opType == ParticleType.NULL) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else {
                            Value value = next.get(binName);
                            if ((value != null && value.getType() != ParticleType.INTEGER) ||
                                    opType != ParticleType.INTEGER) {
                                resultCode = ResultCode.BIN_TYPE_ERROR;
                            } else {
                                next.put(binName,
                                        value == null ? opValue : Value.get(value.toLong() + opValue.toLong()));
                            }
                        }
                        break;
                    case APPEND:
                        if (replace) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else if (opType == ParticleType.NULL) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else {
                            Value value = next.get(binName);
                            if ((value != null && value.getType() != ParticleType.STRING) ||
                                    opType != ParticleType.STRING) {
                                resultCode = ResultCode.BIN_TYPE_ERROR;
                            } else {
                                next.put(binName,
                                        value == null ? opValue : Value.get(value.toString() + opValue.toString()));
                            }
                        }
                        break;
                    case PREPEND:
                        if (replace) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else if (opType == ParticleType.NULL) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        } else {
                            Value value = next.get(binName);
                            if ((value != null && value.getType() != ParticleType.STRING) ||
                                    opType != ParticleType.STRING) {
                                resultCode = ResultCode.BIN_TYPE_ERROR;
                            } else {
                                next.put(binName,
                                        value == null ? opValue : Value.get(opValue.toString() + value.toString()));
                            }
                        }
                        break;
                    case TOUCH:
                        if (current == null) {
                            resultCode = ResultCode.KEY_NOT_FOUND_ERROR;
                        } else if (replace) {
                            resultCode = ResultCode.PARAMETER_ERROR;
                        }
                        break;
                }
            }

            if (resultCode == ResultCode.OK && hasWrite) {
                this.records.put(key, next);
            }
        }

        Header responseHeader = new Header();
        responseHeader.setResultCode(resultCode);
        if (resultCode == ResultCode.OK) {
            if (writeKey) {
                responseHeader.setFieldCount(2);
            }
            responseHeader.setOperationCount(responseOperations.size());
            writer.writeHeader(responseHeader);
            if (writeKey) {
                writer.writeKey(key);
            }
            for (Operation operation : responseOperations) {
                writer.writeOperation(operation);
            }
        } else {
            writer.writeHeader(responseHeader);
        }
    }

}