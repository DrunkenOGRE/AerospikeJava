package io.dogre.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.NullValue;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;

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
            builder.append(namespace).append(":1,//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8=");
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
            if ((readAttr & Command.INFO1_READ) != 0) {
                if ((readAttr & Command.INFO1_BATCH) != 0) {
                    handleBatchGet(header, reader, writer);
                } else {
                    handleGet(header, reader, writer);
                }
            }
            int writeAttr = header.getInfo2();
            if ((writeAttr & Command.INFO2_WRITE) != 0) {
                if ((writeAttr & Command.INFO2_DELETE) != 0) {
                    handleDelete(header, reader, writer);
                } else {
                    handlePut(header, reader, writer);
                }
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
            Map<String, Value> bins = this.records.get(key);
            writer.writeRecord(0, key, bins, binNames);
        } else {
            Header responseHeader = new Header();
            responseHeader.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
            writer.writeHeader(responseHeader);
        }
    }

    public void handlePut(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Header responseHeader = new Header();

        int writeAttribute = header.getInfo2();
        int infoAttribute = header.getInfo3();
        Map<String, Value> src = this.records.get(key);

        List<Operation> operations = reader.readOperations(header.getOperationCount());

        int resultCode = checkResultCode(header, src, operations);

        if (resultCode == ResultCode.OK) {
            if ((infoAttribute & Command.INFO3_CREATE_OR_REPLACE) != 0 || src == null) {
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
        }

        header.setResultCode(resultCode);
        writer.writeHeader(responseHeader);
    }

    protected static int checkResultCode(Header header, Map<String, Value> src, List<Operation> operations) {
        int writeAttribute = header.getInfo2();
        int infoAttribute = header.getInfo3();

        if ((writeAttribute & Command.INFO2_CREATE_ONLY) != 0 && src != null) {
            return ResultCode.KEY_EXISTS_ERROR;
        } else if (((infoAttribute & Command.INFO3_UPDATE_ONLY) != 0 ||
                (infoAttribute & Command.INFO3_REPLACE_ONLY) != 0) && src == null) {
            return ResultCode.KEY_NOT_FOUND_ERROR;
        }

        return ResultCode.OK;
    }

    public void handleDelete(Header header, ByteReader reader, ByteWriter writer) {
        Key key = reader.readKey(header.getFieldCount());

        Header responseHeader = new Header();
        if (!this.records.containsKey(key)) {
            header.setResultCode(ResultCode.KEY_NOT_FOUND_ERROR);
        }

        writer.writeHeader(responseHeader);
    }

}