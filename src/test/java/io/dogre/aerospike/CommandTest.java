package io.dogre.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

public class CommandTest {

    private static IAerospikeClient client;

    private static Key key;

    private static String[] userKeys;
    private static Key[] keys;

    private static Thread thread;

    private static void runAerospikeServer(String host, int port, String... namespaces) {
        ServiceHandler serviceHandler = new ServiceHandlerImpl(host + ":" + port, namespaces);
        Server server = new NettyServer(port, 1, 10, serviceHandler);
        thread = new Thread() {
            @Override
            public void run() {
                server.start();
            }
        };
        thread.start();

        try {
            while (!server.isStarted()) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {

        }
    }

    private static void stopAerospikeServer() {
        thread.interrupt();
    }

    @BeforeClass
    public static void beforeClass() {
        String host = "localhost";
        int port = 3000;
        String namespace = "test";
        String set = "test";

        runAerospikeServer(host, port, namespace);

        client = new AerospikeClient(host, port);
        key = new Key(namespace, set, "test");

        userKeys = new String[] { "test1", "test2", "test3", "test4" };
        keys = new Key[userKeys.length];
        for (int i = 0; i < userKeys.length; i++) {
            keys[i] = new Key(namespace, set, userKeys[i]);
        }
    }

    @AfterClass
    public static void afterClass() {
        client.close();

        stopAerospikeServer();
    }

    @Test
    public void testPut() throws InterruptedException {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // replace record not existing
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        Throwable thrown = catchThrowable(() -> {
            client.put(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);

        // create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");

        // add new bin
        client.put(policy, key, new Bin("age", 10));
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // put record existing
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        thrown = catchThrowable(() -> {
            client.put(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_EXISTS_ERROR);

        // replace record
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        client.put(policy, key, new Bin("name", "test2"));
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test2");
        assertThat(record.bins.containsKey("age")).isFalse();

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testAppend() throws InterruptedException {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"), new Bin("age", 10));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // append
        client.append(policy, key, new Bin("name", " TEST"));
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test TEST");

        // append to non-string bin
        Throwable thrown = catchThrowable(() -> {
            client.append(policy, key, new Bin("age", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.BIN_TYPE_ERROR);

        // append null
        thrown = catchThrowable(() -> {
            client.append(policy, key, Bin.asNull("name"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // append to existing bin
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        thrown = catchThrowable(() -> {
            client.append(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_EXISTS_ERROR);

        // check policy REPLACE, REPLACE_ONLY
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        thrown = catchThrowable(() -> {
            client.append(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        thrown = catchThrowable(() -> {
            client.append(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testPrepend() throws InterruptedException {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"), new Bin("age", 10));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // prepend
        client.prepend(policy, key, new Bin("name", "TEST "));
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("TEST test");

        // prepend to non-string bin
        Throwable thrown = catchThrowable(() -> {
            client.prepend(policy, key, new Bin("age", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.BIN_TYPE_ERROR);

        // prepend null
        thrown = catchThrowable(() -> {
            client.prepend(policy, key, Bin.asNull("name"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // prepend to existing bin
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        thrown = catchThrowable(() -> {
            client.prepend(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_EXISTS_ERROR);

        // check policy REPLACE, REPLACE_ONLY
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        thrown = catchThrowable(() -> {
            client.prepend(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        thrown = catchThrowable(() -> {
            client.prepend(policy, key, new Bin("name", "test"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testAdd() throws InterruptedException {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"), new Bin("age", 10));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // add
        client.add(policy, key, new Bin("age", 15));
        record = client.get(null, key);
        assertThat(record.getInt("age")).isEqualTo(25);

        // add to non-integer bin
        Throwable thrown = catchThrowable(() -> {
            client.add(policy, key, new Bin("name", 10));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.BIN_TYPE_ERROR);

        // add null
        thrown = catchThrowable(() -> {
            client.add(policy, key, Bin.asNull("age"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // add to existing bin
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        thrown = catchThrowable(() -> {
            client.add(policy, key, new Bin("age", 20));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_EXISTS_ERROR);

        // check policy REPLACE, REPLACE_ONLY
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        thrown = catchThrowable(() -> {
            client.add(policy, key, new Bin("age", 15));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        thrown = catchThrowable(() -> {
            client.add(policy, key, new Bin("age", 15));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testDelete() {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // init : create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"), new Bin("age", 10));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // delete record
        client.delete(policy, key);
        record = client.get(null, key);
        assertThat(record).isNull();

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testTouch() {
        WritePolicy policy = new WritePolicy();

        // initialize : delete record
        client.delete(null, key);

        // touch record not existing
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        Throwable thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);
        policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);

        // init : create new record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, key, new Bin("name", "test"), new Bin("age", 10));
        Record record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // touch record existing
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_EXISTS_ERROR);

        // touch record
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.touch(policy, key);
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // check policy REPLACE, REPLACE_ONLY
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        thrown = catchThrowable(() -> {
            client.touch(policy, key);
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testExists() {
        Policy policy = new Policy();

        // initialize : delete record
        client.delete(null, key);

        // check not existing
        assertThat(client.exists(policy, key)).isFalse();

        // check existing
        client.put(null, key, new Bin("name", "test"));
        assertThat(client.exists(policy, key)).isTrue();

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testGet() {
        Policy policy = new Policy();

        // initialize : delete record
        client.delete(null, key);

        // get not existing
        Record record = client.get(null, key);
        assertThat(record).isNull();

        // initialize : put record
        client.put(null, key, new Bin("name", "test"), new Bin("age", 10));

        // test get
        record = client.get(null, key);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);
        assertThat(record.bins).hasSize(2);

        // test get with bin names
        record = client.get(null, key, "name");
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.bins).hasSize(1);

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testGetHeader() {
        Policy policy = new Policy();

        // initialize : delete record
        client.delete(null, key);

        // get not existing
        Record record = client.getHeader(null, key);
        assertThat(record).isNull();

        // initialize : put record
        client.put(null, key, new Bin("name", "test"), new Bin("age", 10));

        // test get header
        record = client.getHeader(null, key);
        assertThat(record.bins).isNull();

        // finalize : delete record
        client.delete(null, key);
    }

    @Test
    public void testBatchGet() {
        BatchPolicy policy = new BatchPolicy();

        // initialize : delete records
        for (Key key : keys) {
            client.delete(null, key);
        }

        // batch get not existing
        Record[] records = client.get(policy, keys);
        assertThat(records).hasSize(keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertThat(records[i]).isNull();
        }

        // initialize : put records at even index
        for (int i = 0; i < keys.length; i += 2) {
            client.put(null, keys[i], new Bin("name", "test" + i), new Bin("age", i));
        }

        // batch get
        records = client.get(policy, keys);
        assertThat(records).hasSize(keys.length);
        for (int i = 0; i < keys.length; i++) {
            if (i % 2 == 0) {
                assertThat(records[i].bins).hasSize(2);
                assertThat(records[i].getString("name")).isEqualTo("test" + i);
                assertThat(records[i].getInt("age")).isEqualTo(i);
            } else {
                assertThat(records[i]).isNull();
            }
        }

        // batch get with bin names
        records = client.get(policy, keys, "name");
        assertThat(records).hasSize(keys.length);
        for (int i = 0; i < keys.length; i++) {
            if (i % 2 == 0) {
                assertThat(records[i].bins).hasSize(1);
                assertThat(records[i].getString("name")).isEqualTo("test" + i);
            } else {
                assertThat(records[i]).isNull();
            }
        }

        // finalize : delete records
        for (Key key : keys) {
            client.delete(null, key);
        }
    }

    @Test
    public void testBatchGetHeader() {
        BatchPolicy policy = new BatchPolicy();

        // initialize : delete records
        for (Key key : keys) {
            client.delete(null, key);
        }

        // batch get header not existing
        Record[] records = client.get(policy, keys);
        assertThat(records).hasSize(keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertThat(records[i]).isNull();
        }

        // initialize : put records at even index
        for (int i = 0; i < keys.length; i += 2) {
            client.put(null, keys[i], new Bin("name", "test" + i), new Bin("age", i));
        }

        // batch get header
        records = client.getHeader(policy, keys);
        assertThat(records).hasSize(keys.length);
        for (int i = 0; i < keys.length; i++) {
            if (i % 2 == 0) {
                assertThat(records[i].bins).isNull();
            } else {
                assertThat(records[i]).isNull();
            }
        }

        // finalize : delete records
        for (Key key : keys) {
            client.delete(null, key);
        }
    }

    @Test
    public void testOperate() {
        WritePolicy policy = new WritePolicy();

        // finalize : delete record
        client.delete(null, key);

        // get not existing
        Record record = client.operate(policy, key, Operation.get("name"), Operation.get("age"));
        assertThat(record).isNull();

        // put and get not existing
        policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        Throwable thrown = catchThrowable(() -> {
            client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.get("name"),
                    Operation.get("age"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);
        policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        thrown = catchThrowable(() -> {
            client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.get("name"),
                    Operation.get("age"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.KEY_NOT_FOUND_ERROR);

        // put and get
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        record = client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                Operation.get("name"), Operation.get("age"));
        assertThat(record.bins).hasSize(2);
        assertThat(record.getString("name")).isEqualTo("test");
        assertThat(record.getInt("age")).isEqualTo(10);

        // test sequential executions
        record = client.operate(policy, key, Operation.append(new Bin("name", " TEST")), Operation.get("name"),
                Operation.prepend(new Bin("name", "TEST ")), Operation.get("age"), Operation.add(new Bin("age", 15)));
        assertThat(record.bins).hasSize(2);
        assertThat(record.getString("name")).isEqualTo("test TEST");
        assertThat(record.getInt("age")).isEqualTo(10);
        record = client.get(null, key);
        assertThat(record.bins).hasSize(2);
        assertThat(record.getString("name")).isEqualTo("TEST test TEST");
        assertThat(record.getInt("age")).isEqualTo(25);

        // test multiple get
        record = client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                Operation.get("name"), Operation.get("age"), Operation.append(new Bin("name", " TEST")),
                Operation.get("name"));
        assertThat(record.bins).hasSize(2);
        assertThat((List<String>) record.getList("name")).containsExactly("test", "test TEST");
        assertThat(record.getInt("age")).isEqualTo(10);

        // put and get header
        record = client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                Operation.getHeader());
        assertThat(record.bins).isNull();

        // use get and get header
        thrown = catchThrowable(() -> {
            client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                    Operation.get(), Operation.getHeader());
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // use get and get with bin name
        thrown = catchThrowable(() -> {
            client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                    Operation.get(), Operation.get("name"));
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // use multiple full get
        thrown = catchThrowable(() -> {
            client.operate(policy, key, Operation.put(new Bin("name", "test")), Operation.put(new Bin("age", 10)),
                    Operation.get(), Operation.get());
        });
        assertThat(thrown).isInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", ResultCode.PARAMETER_ERROR);

        // finalize : delete record
        client.delete(null, key);
    }

}
