package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Operation.Type;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.NullValue;
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
            int writeAttr = header.getInfo2();
            if ((readAttr & Command.INFO1_READ) != 0) {
                handleRead(header, reader, writer);
            } else if ((writeAttr & Command.INFO2_WRITE) != 0) {
                handleWrite(header, reader, writer);
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

    protected void handleRead(Header header, ByteReader reader, ByteWriter writer) {
        int readAttr = header.getInfo1();
        if ((readAttr & Command.INFO1_NOBINDATA) != 0) {
            handleExists(header, reader, writer);
        } else if ((readAttr & Command.INFO1_BATCH) != 0) {
            handleBatchGet(header, reader, writer);
        } else {
            handleGet(header, reader, writer);
        }
    }

    protected void handleExists(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Header responseHeader = new Header();
        if (!this.records.containsKey(key)) {
            responseHeader.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
        }

        writer.writeHeader(responseHeader);
    }

    protected void handleBatchGet(Header header, ByteReader reader, ByteWriter writer) {
        reader.skip(4); // field size
        int fieldType = reader.readByte();
        boolean sendSetName = (fieldType == FieldType.BATCH_INDEX_WITH_SET);

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
            writer.writeRecord(index, key, this.records.get(key), binNames);
        }

        Header lastHeader = new Header();
        lastHeader.setInfo3(Command.INFO3_LAST);
        writer.writeHeader(lastHeader);
    }

    protected void handleGet(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Set<String> binNames = null;
        int operationCount = header.getOperationCount();
        if (0 < operationCount) {
            binNames = reader.readBinNames(operationCount);
        }

        if (this.records.containsKey(key)) {
            int readAttr = header.getInfo1();
            Map<String, Value> bins = (readAttr & Command.INFO1_NOBINDATA) != 0 ? null : this.records.get(key);
            writer.writeRecord(0, key, bins, binNames);
        } else {
            Header responseHeader = new Header();
            responseHeader.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
            writer.writeHeader(responseHeader);
        }
    }

    protected void handleWrite(Header header, ByteReader reader, ByteWriter writer) {
        int writeAttr = header.getInfo2();
        if ((writeAttr & Command.INFO2_DELETE) != 0) {
            handleDelete(header, reader, writer);
        } else {
            handlePut(header, reader, writer);
        }
    }

    protected void handleDelete(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Header responseHeader = new Header();
        if (!this.records.containsKey(key)) {
            header.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
        }

        writer.writeHeader(responseHeader);
    }

    protected void handlePut(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Map<String, Value> src = this.records.get(key);

        int resultCode = ResultCode.OK;

        List<Operation> operations = reader.readOperations(header.getOperationCount());
        Type operationType = operations.get(0).type;

        if (operationType == Type.WRITE) {
            resultCode = handleWrite(header, key, src, operations);
        } else if (operationType == Type.APPEND) {
            resultCode = handleAppendPrepend(header, key, src, operations, true);
        } else if (operationType == Type.PREPEND) {
            resultCode = handleAppendPrepend(header, key, src, operations, false);
        } else if (operationType == Type.ADD) {
            resultCode = handleAdd(header, key, src, operations);
        } else if (operationType == Type.TOUCH) {
            resultCode = handleTouch(header, key, src, operations);
        }

        Header responseHeader = new Header();
        responseHeader.setResultCode(resultCode);
        writer.writeHeader(responseHeader);
    }

    protected int handleWrite(Header header, Key key, Map<String, Value> src, List<Operation> operations) {
        int writeAttr = header.getInfo2();
        int infoAttr = header.getInfo3();

        if ((writeAttr & Command.INFO2_CREATE_ONLY) != 0 && src != null) {
            return ResultCode.KEY_EXISTS_ERROR;
        } else if (((infoAttr & Command.INFO3_UPDATE_ONLY) != 0 || (infoAttr & Command.INFO3_REPLACE_ONLY) != 0) &&
                src == null) {
            return ResultCode.KEY_NOT_FOUND_ERROR;
        }

        if ((infoAttr & Command.INFO3_CREATE_OR_REPLACE) != 0 || src == null) {
            Map<String, Value> bins = new HashMap<>();
            for (Operation operation : operations) {
                bins.put(operation.binName, operation.value);
            }
            this.records.put(key, bins);
        } else {
            for (Operation operation : operations) {
                if (operation.value == null || operation.value instanceof NullValue) {
                    src.remove(operation.binName);
                } else {
                    src.put(operation.binName, operation.value);
                }
            }
            this.records.put(key, src);
        }

        return ResultCode.OK;
    }

    protected int handleAppendPrepend(Header header, Key key, Map<String, Value> src, List<Operation> operations,
            boolean append) {
        int writeAttr = header.getInfo2();
        int infoAttr = header.getInfo3();

        if ((writeAttr & Command.INFO2_CREATE_ONLY) != 0 && src != null) {
            for (Operation operation : operations) {
                if (src.containsKey(operation.binName)) {
                    return ResultCode.KEY_EXISTS_ERROR;
                }
            }
        } else if ((infoAttr & Command.INFO3_UPDATE_ONLY) != 0 && src == null) {
            return ResultCode.KEY_NOT_FOUND_ERROR;
        } else if ((infoAttr & Command.INFO3_REPLACE_ONLY) != 0) {
            return ResultCode.PARAMETER_ERROR;
        }
        for (Operation operation : operations) {
            Value value = src.get(operation.binName);
            if (value != null && value.getType() != ParticleType.STRING && value.getType() != ParticleType.BLOB) {
                return ResultCode.BIN_TYPE_ERROR;
            }
            if (operation.value == null || operation.value.getType() == ParticleType.NULL) {
                return ResultCode.PARAMETER_ERROR;
            } else if (operation.value.getType() != ParticleType.STRING) {
                return ResultCode.BIN_TYPE_ERROR;
            }
        }

        if (src == null) {
            src = new HashMap<>();
        }
        for (Operation operation : operations) {
            Value value = src.get(operation.binName);
            if (value == null) {
                value = operation.value;
            } else {
                value = Value.get(append ? value.toString() + operation.value.toString() :
                        operation.value.toString() + value.toString());
            }
            src.put(operation.binName, value);
        }
        this.records.put(key, src);

        return ResultCode.OK;
    }

    protected int handleAdd(Header header, Key key, Map<String, Value> src, List<Operation> operations) {
        int writeAttr = header.getInfo2();
        int infoAttr = header.getInfo3();

        if ((writeAttr & Command.INFO2_CREATE_ONLY) != 0 && src != null) {
            for (Operation operation : operations) {
                if (src.containsKey(operation.binName)) {
                    return ResultCode.KEY_EXISTS_ERROR;
                }
            }
        } else if ((infoAttr & Command.INFO3_UPDATE_ONLY) != 0 && src == null) {
            return ResultCode.KEY_NOT_FOUND_ERROR;
        } else if ((infoAttr & Command.INFO3_REPLACE_ONLY) != 0) {
            return ResultCode.PARAMETER_ERROR;
        }
        for (Operation operation : operations) {
            Value value = src.get(operation.binName);
            if (value != null && value.getType() != ParticleType.INTEGER) {
                return ResultCode.BIN_TYPE_ERROR;
            }
            if (operation.value == null || operation.value.getType() == ParticleType.NULL) {
                return ResultCode.PARAMETER_ERROR;
            } else if (operation.value.getType() != ParticleType.INTEGER) {
                return ResultCode.BIN_TYPE_ERROR;
            }
        }

        if (src == null) {
            src = new HashMap<>();
        }
        for (Operation operation : operations) {
            Value value = src.get(operation.binName);
            if (value == null) {
                value = operation.value;
            } else {
                value = Value.get(value.toLong() + operation.value.toLong());
            }
            src.put(operation.binName, value);
        }
        this.records.put(key, src);

        return ResultCode.OK;
    }

    protected int handleTouch(Header header, Key key, Map<String, Value> src, List<Operation> operations) {
        int writeAttr = header.getInfo2();
        int infoAttr = header.getInfo3();

        if ((writeAttr & Command.INFO2_CREATE_ONLY) != 0 && src != null) {
            return ResultCode.KEY_EXISTS_ERROR;
        } else if ((infoAttr & Command.INFO3_UPDATE_ONLY) != 0 && src == null) {
            return ResultCode.KEY_NOT_FOUND_ERROR;
        } else if ((infoAttr & Command.INFO3_REPLACE_ONLY) != 0) {
            return ResultCode.PARAMETER_ERROR;
        }

        return ResultCode.OK;
    }

}