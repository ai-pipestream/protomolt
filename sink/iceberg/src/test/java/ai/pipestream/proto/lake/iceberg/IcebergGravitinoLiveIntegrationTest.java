package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lake lane against <a href="https://gravitino.apache.org">Apache Gravitino</a>, whose Iceberg
 * REST service (the {@code gravitino-iceberg-rest} image docker-compose.integration.yml runs)
 * stands in for the reference catalog. Nothing in the sink changes: Gravitino speaks the Iceberg
 * REST spec, so the same {@code ensureTable}/{@code append} that drives any catalog lands tables in
 * a federated metadata lake, and Iceberg's own reader gets the rows back. A Testcontainers
 * instance provides the service; the suite skips when Docker is unavailable.
 *
 * <p>Gravitino serves the REST endpoint under {@code /iceberg}, not the root.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class IcebergGravitinoLiveIntegrationTest {

    // Its own warehouse tree: sharing one with the reference catalog means two services (each
    // its own container user) creating entries under the same root.
    private static final String WAREHOUSE = "/tmp/protomolt-gravitino-warehouse";

    static {
        // Docker creates a missing bind source root-owned; the service must be able to write.
        try {
            Files.createDirectories(Path.of(WAREHOUSE));
            Files.setPosixFilePermissions(Path.of(WAREHOUSE),
                    PosixFilePermissions.fromString("rwxrwxrwx"));
        } catch (Exception ignored) {
            // best effort: a non-POSIX filesystem or an owner left by an earlier run
        }
    }

    @Container
    static final GenericContainer<?> GRAVITINO = new GenericContainer<>(
            DockerImageName.parse("apache/gravitino-iceberg-rest:1.3.0"))
            .withExposedPorts(9001)
            .withEnv("GRAVITINO_ICEBERG_REST_WAREHOUSE", "file://" + WAREHOUSE)
            // A test fixture does not need the 1 GB default heap.
            .withEnv("GRAVITINO_MEM", "-Xms256m -Xmx512m -XX:MaxMetaspaceSize=256m")
            .withFileSystemBind(WAREHOUSE, WAREHOUSE, BindMode.READ_WRITE)
            // The aliyun bundle shades an older jackson-core that can win the libs/* glob and
            // break every write with a NoSuchMethodError; this lane is file:// only, so the
            // compose fixture drops the jar and so do we.
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "/bin/bash", "-c",
                    "rm -f /opt/gravitino-iceberg-rest-server/libs/gravitino-iceberg-aliyun-bundle-*.jar"
                            + " && exec /opt/gravitino-iceberg-rest-server/bin/start-iceberg-rest-server.sh"))
            .waitingFor(Wait.forHttp("/iceberg/v1/config").forPort(9001)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    private static String catalogUri() {
        return "http://" + GRAVITINO.getHost() + ":" + GRAVITINO.getMappedPort(9001) + "/iceberg";
    }

    private static final String PROTO = """
            syntax = "proto3";
            package gravitinoit.v1;
            message Tick {
              string symbol = 1;
              double price = 2;
              int64 sequence = 3;
            }
            """;

    private static FileDescriptor file;
    private static RESTCatalog catalog;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("gravitinoit/v1/tick.proto", PROTO, "test").build());
        file = compiled.descriptorFor("gravitinoit/v1/tick.proto").orElseThrow();
        catalog = new RESTCatalog();
        catalog.initialize("gravitino", Map.of(
                CatalogProperties.URI, catalogUri(),
                // file:// without Hadoop: JEP 486 removed what HadoopFileIO leans on.
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
        try {
            catalog.createNamespace(Namespace.of("protomolt_gravitino"));
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
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "SYM" + i)
                .setField(type.findFieldByName("price"), 100.0 + i)
                .setField(type.findFieldByName("sequence"), (long) i)
                .build();
    }

    @Test
    void appendsAndReadsBackThroughGravitino() throws Exception {
        Descriptor type = file.findMessageTypeByName("Tick");
        // Unique per run so reruns (CI uses --rerun-tasks) never collide.
        String name = "ticks_" + Long.toUnsignedString(System.nanoTime(), 36);
        TableIdentifier id = TableIdentifier.of("protomolt_gravitino", name);

        // A client-owned location: the catalog container and this JVM run as different users on
        // CI, and whoever owns the tree must be the data writer.
        Path base = Path.of(WAREHOUSE + "/client/" + name);
        for (String dir : new String[]{"", "metadata", "data"}) {
            Path path = dir.isEmpty() ? base : base.resolve(dir);
            Files.createDirectories(path);
            try {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX filesystem: single-user anyway
            }
        }

        Table table = IcebergSink.ensureTable(catalog, id, type, "file://" + base);
        try {
            List<DataFile> first = IcebergSink.append(table, type,
                    List.of(tick(type, 0), tick(type, 1)));
            assertThat(first).hasSize(1);
            IcebergSink.append(table, type, List.of(tick(type, 2)));

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
            Record row = rows.get(2);
            assertThat(row.getField("symbol")).isEqualTo("SYM2");
            assertThat(row.getField("price")).isEqualTo(102.0);
            assertThat(row.getField("sequence")).isEqualTo(2L);

            // Gravitino itself lists what we created.
            assertThat(catalog.listTables(Namespace.of("protomolt_gravitino"))).contains(id);
        } finally {
            // No purge: the data files belong to this user, not the catalog service.
            catalog.dropTable(id, false);
        }
    }
}
