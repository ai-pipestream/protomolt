package ai.pipestream.proto.lake.iceberg;

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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole lane against a genuine REST catalog service
 * ({@code apache/iceberg-rest-fixture}, the image docker-compose.integration.yml runs):
 * tables created over the wire, snapshots committed through REST transactions, and the data
 * read back by Iceberg's own reader from the shared warehouse. A Testcontainers instance
 * provides the catalog; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class IcebergRestLiveIntegrationTest {

    // The warehouse is bind-mounted at the SAME absolute path inside the container, so the
    // catalog and this JVM both resolve file:// data locations without an object store.
    private static final String WAREHOUSE = "/tmp/protomolt-iceberg-warehouse";

    static {
        // Docker creates a missing bind source root-owned; the fixture runs as uid 1000 and
        // must be able to write here.
        try {
            java.nio.file.Path warehouse = java.nio.file.Path.of(WAREHOUSE);
            java.nio.file.Files.createDirectories(warehouse);
            java.nio.file.Files.setPosixFilePermissions(warehouse,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rwxrwxrwx"));
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
            package lakeit.v1;
            message Tick {
              string symbol = 1;
              double price = 2;
              repeated string venues = 3;
              Meta meta = 4;
            }
            message Meta { string source = 1; int64 sequence = 2; }
            """;

    private static FileDescriptor file;
    private static RESTCatalog catalog;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lakeit/v1/tick.proto", PROTO, "test").build());
        file = compiled.descriptorFor("lakeit/v1/tick.proto").orElseThrow();
        catalog = new RESTCatalog();
        catalog.initialize("live", Map.of(
                "uri", catalogUri(),
                // file:// without Hadoop: JEP 486 removed what HadoopFileIO leans on.
                org.apache.iceberg.CatalogProperties.FILE_IO_IMPL,
                LocalFileIO.class.getName()));
        try {
            catalog.createNamespace(Namespace.of("protomolt_it"));
        } catch (AlreadyExistsException ignored) {
            // reruns share the namespace
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private static DynamicMessage tick(Descriptor type, int i) {
        Descriptor meta = file.findMessageTypeByName("Meta");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "SYM" + i)
                .setField(type.findFieldByName("price"), 100.0 + i)
                .setField(type.findFieldByName("meta"), DynamicMessage.newBuilder(meta)
                        .setField(meta.findFieldByName("source"), "live-test")
                        .setField(meta.findFieldByName("sequence"), (long) i)
                        .build());
        builder.addRepeatedField(type.findFieldByName("venues"), "NYSE");
        return builder.build();
    }

    @Test
    void appendsAndReadsBackThroughTheRestCatalog() throws Exception {
        Descriptor type = file.findMessageTypeByName("Tick");
        // Unique per run so reruns (CI uses --rerun-tasks) never collide.
        TableIdentifier id = TableIdentifier.of("protomolt_it",
                "ticks_" + Long.toUnsignedString(System.nanoTime(), 36));

        // A client-owned location: the catalog container and the test JVM run as different
        // users; whoever owns the table tree must be the data writer.
        java.nio.file.Path base = java.nio.file.Path.of(
                WAREHOUSE + "/client/" + id.name());
        // Pre-create the WHOLE table tree world-writable: the catalog service (uid 1000
        // in the container) writes metadata.json here while this JVM (a different uid on
        // CI) writes manifests and data - whoever creates a directory first owns it, so
        // neither side may be the one to create a directory the other must write into.
        for (String dir : new String[]{"", "metadata", "data"}) {
            java.nio.file.Path path = dir.isEmpty() ? base : base.resolve(dir);
            java.nio.file.Files.createDirectories(path);
            try {
                java.nio.file.Files.setPosixFilePermissions(path,
                        java.nio.file.attribute.PosixFilePermissions.fromString("rwxrwxrwx"));
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX filesystem: single-user anyway
            }
        }
        Table table = IcebergSink.ensureTable(catalog, id, type, "file://" + base);
        try {
            org.apache.iceberg.DataFile first = IcebergSink.append(table, type,
                    List.of(tick(type, 0), tick(type, 1))).getFirst();
            // Column metrics survive the round trip through the REST catalog's commit.
            int priceId = table.schema().findField("price").fieldId();
            assertThat(first.upperBounds()).containsKey(priceId);
            IcebergSink.append(table, type, List.of(tick(type, 2)));

            table.refresh();
            List<org.apache.iceberg.Snapshot> snapshots = new ArrayList<>();
            table.snapshots().forEach(snapshots::add);
            assertThat(snapshots).hasSize(2);

            List<Record> rows = new ArrayList<>();
            try (CloseableIterable<Record> scan = IcebergGenerics.read(table).build()) {
                scan.forEach(rows::add);
            }
            assertThat(rows).hasSize(3);
            rows.sort(java.util.Comparator.comparing(r -> (String) r.getField("symbol")));
            Record row = rows.get(2);
            assertThat(row.getField("symbol")).isEqualTo("SYM2");
            assertThat(row.getField("price")).isEqualTo(102.0);
            assertThat((List<?>) row.getField("venues")).first().isEqualTo("NYSE");
            assertThat(((Record) row.getField("meta")).getField("sequence")).isEqualTo(2L);

            // The catalog service itself lists what we created.
            assertThat(catalog.listTables(Namespace.of("protomolt_it"))).contains(id);
        } finally {
            // No purge: the data files belong to this user, not the catalog service.
            catalog.dropTable(id, false);
        }
    }
}
