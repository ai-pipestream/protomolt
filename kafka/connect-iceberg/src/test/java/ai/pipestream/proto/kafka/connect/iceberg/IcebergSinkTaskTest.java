package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Iceberg sink against a real in-memory catalog: records in every value format become table
 * rows, a partition config creates a partitioned table, and undecodable values fail as
 * {@link DataException}s. The whole put/append/read-back path runs, so a break in the emitter or
 * the catalog wiring fails here, not in production.
 */
class IcebergSinkTaskTest {

    private static final String PROTO = """
            syntax = "proto3";
            package connect.test;
            message Order { string id = 1; int64 qty = 2; string region = 3; }
            """;

    private static final java.util.concurrent.atomic.AtomicInteger NEXT =
            new java.util.concurrent.atomic.AtomicInteger();

    private static FileDescriptor file;
    private InMemoryCatalog catalog;
    private IcebergSinkTask task;

    @BeforeEach
    void setUp() throws Exception {
        file = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("connect/test/order.proto", PROTO, "test").build())
                .descriptorFor("connect/test/order.proto").orElseThrow();
        catalog = new InMemoryCatalog();
        // A unique name per test: an InMemoryCatalog shares state by name, and the task's
        // stop() closes the catalog, so a shared name leaks a torn-down store into the next test.
        catalog.initialize("test-" + NEXT.incrementAndGet(), Map.of());
        catalog.createNamespace(Namespace.of("connect"));
    }

    @AfterEach
    void tearDown() {
        if (task != null) {
            task.stop();
            task = null;
        }
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("connect/test/order.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private Map<String, String> config(String format) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergSinkConfig.DESCRIPTOR_SET, descriptorSetBase64());
        props.put(IcebergSinkConfig.MESSAGE_TYPE, "connect.test.Order");
        props.put(IcebergSinkConfig.VALUE_FORMAT, format);
        props.put(IcebergSinkConfig.TABLE, "connect.orders");
        return props;
    }

    private IcebergSinkTask startTask(Map<String, String> props) {
        IcebergSinkTask started = new IcebergSinkTask();
        started.catalogFactory = config -> catalog;
        started.start(props);
        task = started;
        return started;
    }

    private static DynamicMessage order(String id, long qty, String region) {
        Descriptor type = file.findMessageTypeByName("Order");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), id)
                .setField(type.findFieldByName("qty"), qty)
                .setField(type.findFieldByName("region"), region)
                .build();
    }

    private static SinkRecord record(Object value, long offset) {
        return new SinkRecord("orders", 0, null, null, null, value, offset);
    }

    private List<Record> rows(String table) {
        List<Record> rows = new ArrayList<>();
        Table loaded = catalog.loadTable(TableIdentifier.parse(table));
        try (CloseableIterable<Record> scan = IcebergGenerics.read(loaded).build()) {
            scan.forEach(rows::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    @Test
    void protobufRecordsBecomeTableRows() throws Exception {
        IcebergSinkTask sink = startTask(config("protobuf"));
        sink.put(List.of(
                record(order("o-1", 3, "us").toByteArray(), 0),
                record(order("o-2", 5, "eu").toByteArray(), 1)));

        List<Record> rows = rows("connect.orders");
        assertThat(rows).hasSize(2);
        rows.sort(java.util.Comparator.comparing(r -> (String) r.getField("id")));
        assertThat(rows.get(1).getField("id")).isEqualTo("o-2");
        assertThat(rows.get(1).getField("qty")).isEqualTo(5L);
        assertThat(rows.get(1).getField("region")).isEqualTo("eu");
    }

    @Test
    void confluentFramedValuesDecode() throws Exception {
        IcebergSinkTask sink = startTask(config("confluent"));
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        framed.write(0);
        framed.writeBytes(new byte[]{0, 0, 0, 7});   // schema id 7
        framed.write(0);                              // message-indexes: [0]
        framed.writeBytes(order("c-1", 9, "us").toByteArray());
        sink.put(List.of(record(framed.toByteArray(), 0)));
        assertThat(rows("connect.orders")).hasSize(1)
                .first().satisfies(r -> assertThat(r.getField("qty")).isEqualTo(9L));
    }

    @Test
    void jsonValuesDecode() throws Exception {
        IcebergSinkTask sink = startTask(config("json"));
        sink.put(List.of(record("{\"id\": \"j-1\", \"qty\": \"4\", \"region\": \"eu\"}", 0)));
        assertThat(rows("connect.orders")).hasSize(1)
                .first().satisfies(r -> assertThat(r.getField("id")).isEqualTo("j-1"));
    }

    @Test
    void aPartitionConfigCreatesAPartitionedTable() throws Exception {
        Map<String, String> props = config("protobuf");
        props.put(IcebergSinkConfig.PARTITION, "region:identity");
        IcebergSinkTask sink = startTask(props);

        Table table = catalog.loadTable(TableIdentifier.parse("connect.orders"));
        assertThat(table.spec().isPartitioned()).isTrue();

        // Two regions in one batch -> two partitions, one snapshot.
        sink.put(List.of(
                record(order("o-1", 1, "us").toByteArray(), 0),
                record(order("o-2", 2, "eu").toByteArray(), 1),
                record(order("o-3", 3, "us").toByteArray(), 2)));
        assertThat(rows("connect.orders")).hasSize(3);
        table.refresh();
        assertThat(table.currentSnapshot().addedDataFiles(table.io())).hasSize(2);
    }

    @Test
    void nullAndUndecodableValuesAreDataExceptions() throws Exception {
        IcebergSinkTask sink = startTask(config("json"));
        assertThatThrownBy(() -> sink.put(List.of(record(null, 0))))
                .isInstanceOf(DataException.class);
        assertThatThrownBy(() -> sink.put(List.of(record("{not json", 0))))
                .isInstanceOf(DataException.class);
    }

    /**
     * The frame reader lives in another module now; a malformed Confluent frame must still fail
     * as a DataException the worker can route. The zigzag-negative message-index count is the case
     * that must stay covered: skipping the index array rather than refusing it would hand the index
     * bytes to the parser as payload. The count is built with Kafka's own ByteUtils.writeVarint.
     */
    @Test
    void malformedConfluentFramesAreDataExceptions() throws Exception {
        IcebergSinkTask sink = startTask(config("confluent"));

        // Wrong magic byte / too short to hold a prefix.
        assertThatThrownBy(() -> sink.put(List.of(record(new byte[]{1, 2, 3, 4, 5, 6}, 0))))
                .isInstanceOf(DataException.class);

        ByteBuffer count = ByteBuffer.allocate(8);
        ByteUtils.writeVarint(-1, count);
        count.flip();
        byte[] countBytes = new byte[count.remaining()];
        count.get(countBytes);
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        framed.write(0);
        framed.writeBytes(new byte[]{0, 0, 0, 7});   // schema id 7
        framed.writeBytes(countBytes);                // zigzag-negative message-index count
        framed.writeBytes(order("c-1", 9, "us").toByteArray());
        assertThatThrownBy(() -> sink.put(List.of(record(framed.toByteArray(), 0))))
                .isInstanceOf(DataException.class);
    }

    @Test
    void theConnectorValidatesEagerlyAndFansOutTasks() throws Exception {
        IcebergSinkConnector connector = new IcebergSinkConnector();
        connector.start(config("protobuf"));
        assertThat(connector.taskConfigs(3)).hasSize(3);
        assertThat(connector.taskClass()).isEqualTo(IcebergSinkTask.class);
        connector.stop();

        Map<String, String> bad = new HashMap<>(config("protobuf"));
        bad.put(IcebergSinkConfig.MESSAGE_TYPE, "connect.test.Nope");
        assertThatThrownBy(() -> new IcebergSinkConnector().start(bad))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Nope");
    }
}
