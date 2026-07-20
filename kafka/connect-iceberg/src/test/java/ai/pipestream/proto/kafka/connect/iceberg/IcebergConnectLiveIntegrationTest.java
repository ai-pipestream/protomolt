package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sink task driven against a genuine REST catalog (the {@code iceberg-rest-fixture} image
 * docker-compose.integration.yml runs). This is what the in-memory unit test cannot cover: the
 * {@code iceberg.catalog.*} config actually builds a catalog, the connector commits real
 * snapshots through it, and Iceberg's own reader gets the rows back. A Testcontainers instance
 * provides the catalog; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class IcebergConnectLiveIntegrationTest {

    private static final String LOCAL_FILE_IO = "ai.pipestream.proto.lake.iceberg.LocalFileIO";

    // The warehouse is bind-mounted at the SAME absolute path inside the container, so the
    // catalog and this JVM both resolve file:// data locations without an object store.
    private static final String WAREHOUSE = "/tmp/protomolt-iceberg-warehouse";

    static {
        // Docker creates a missing bind source root-owned; the fixture runs as uid 1000 and
        // must be able to write here.
        try {
            Files.createDirectories(Path.of(WAREHOUSE));
            Files.setPosixFilePermissions(Path.of(WAREHOUSE),
                    PosixFilePermissions.fromString("rwxrwxrwx"));
        } catch (Exception ignored) {
            // best effort: a non-POSIX filesystem or an owner left by an earlier run
        }
    }

    @Container
    static final GenericContainer<?> CATALOG_SERVICE = new GenericContainer<>(
            DockerImageName.parse("apache/iceberg-rest-fixture:1.10.1"))
            .withExposedPorts(8181)
            .withEnv("CATALOG_WAREHOUSE", "file://" + WAREHOUSE)
            .withFileSystemBind(WAREHOUSE, WAREHOUSE, BindMode.READ_WRITE)
            .waitingFor(Wait.forHttp("/v1/config").forPort(8181));

    private static String catalogUri() {
        return "http://" + CATALOG_SERVICE.getHost() + ":" + CATALOG_SERVICE.getMappedPort(8181);
    }

    private static final String PROTO = """
            syntax = "proto3";
            package connectit.v1;
            message Tick {
              string symbol = 1;
              double price = 2;
              int64 sequence = 3;
            }
            """;

    private static FileDescriptor file;
    private static RESTCatalog reader;

    @BeforeAll
    static void start() throws Exception {
        file = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("connectit/v1/tick.proto", PROTO, "test").build())
                .descriptorFor("connectit/v1/tick.proto").orElseThrow();
        reader = new RESTCatalog();
        reader.initialize("reader", Map.of(
                CatalogProperties.URI, catalogUri(),
                CatalogProperties.FILE_IO_IMPL, LOCAL_FILE_IO));
        try {
            reader.createNamespace(Namespace.of("protomolt_connect_it"));
        } catch (AlreadyExistsException ignored) {
            // reruns share the namespace
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    private static String descriptorSetBase64() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("connectit/v1/tick.proto", PROTO, "test").build());
        return Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray());
    }

    private static DynamicMessage tick(int i) {
        Descriptor type = file.findMessageTypeByName("Tick");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "SYM" + i)
                .setField(type.findFieldByName("price"), 100.0 + i)
                .setField(type.findFieldByName("sequence"), (long) i)
                .build();
    }

    private static SinkRecord record(int i) {
        return new SinkRecord("ticks", 0, null, null, null, tick(i).toByteArray(), i);
    }

    @Test
    void theTaskCommitsSnapshotsThroughARealRestCatalog() throws Exception {
        String name = "ticks_" + Long.toUnsignedString(System.nanoTime(), 36);
        TableIdentifier id = TableIdentifier.of("protomolt_connect_it", name);

        // A client-owned location, pre-created world-writable: on CI the catalog container and
        // the test JVM run as different users, so whoever owns the tree must be able to write it.
        Path base = Path.of(WAREHOUSE + "/client/connect-" + name);
        for (String dir : new String[]{"", "metadata", "data"}) {
            Path path = dir.isEmpty() ? base : base.resolve(dir);
            Files.createDirectories(path);
            try {
                Files.setPosixFilePermissions(path,
                        PosixFilePermissions.fromString("rwxrwxrwx"));
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX filesystem: single-user anyway
            }
        }

        Map<String, String> props = new HashMap<>();
        props.put(IcebergSinkConfig.DESCRIPTOR_SET, descriptorSetBase64());
        props.put(IcebergSinkConfig.MESSAGE_TYPE, "connectit.v1.Tick");
        props.put(IcebergSinkConfig.VALUE_FORMAT, "protobuf");
        props.put(IcebergSinkConfig.TABLE, "protomolt_connect_it." + name);
        props.put(IcebergSinkConfig.TABLE_LOCATION, "file://" + base);
        props.put("iceberg.catalog.name", "connect-live");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.catalog.uri", catalogUri());
        props.put("iceberg.catalog.io-impl", LOCAL_FILE_IO);

        IcebergSinkTask task = new IcebergSinkTask();
        task.start(props);
        try {
            task.put(List.of(record(0), record(1)));
            task.put(List.of(record(2)));

            Table table = reader.loadTable(id);
            table.refresh();
            List<Snapshot> snapshots = new ArrayList<>();
            table.snapshots().forEach(snapshots::add);
            assertThat(snapshots).hasSize(2);

            List<Record> rows = new ArrayList<>();
            try (CloseableIterable<Record> scan = IcebergGenerics.read(table).build()) {
                scan.forEach(rows::add);
            }
            assertThat(rows).hasSize(3);
            rows.sort(java.util.Comparator.comparing(r -> (String) r.getField("symbol")));
            assertThat(rows.get(2).getField("symbol")).isEqualTo("SYM2");
            assertThat(rows.get(2).getField("price")).isEqualTo(102.0);
            assertThat(rows.get(2).getField("sequence")).isEqualTo(2L);
        } finally {
            task.stop();
            // No purge: the data files belong to this user, not the catalog service.
            reader.dropTable(id, false);
        }
    }
}
