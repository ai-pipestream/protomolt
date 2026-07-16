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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The whole lane against a genuine REST catalog service
 * ({@code apache/iceberg-rest-fixture} from docker-compose.integration.yml): tables created
 * over the wire, snapshots committed through REST transactions, and the data read back by
 * Iceberg's own reader from the shared warehouse. Skips when the catalog is not running;
 * CI runs it with the compose stack up and fails if it skipped.
 */
class IcebergRestLiveIntegrationTest {

    private static final String CATALOG_URI = System.getProperty(
            "protomolt.it.iceberg.rest", "http://127.0.0.1:18181");

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
        assumeTrue(reachable(), "Iceberg REST catalog not reachable at " + CATALOG_URI
                + "; start docker-compose.integration.yml to run this suite");
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lakeit/v1/tick.proto", PROTO, "test").build());
        file = compiled.descriptorFor("lakeit/v1/tick.proto").orElseThrow();
        catalog = new RESTCatalog();
        catalog.initialize("live", Map.of(
                "uri", CATALOG_URI,
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

    private static boolean reachable() {
        try (HttpClient http = HttpClient.newHttpClient()) {
            return http.send(HttpRequest.newBuilder(URI.create(CATALOG_URI + "/v1/config"))
                                    .GET().build(),
                            HttpResponse.BodyHandlers.discarding())
                    .statusCode() == 200;
        } catch (Exception e) {
            return false;
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

        // A client-owned location: on CI the catalog container and the test JVM run as
        // different users; whoever owns the table tree must be the data writer.
        java.nio.file.Path base = java.nio.file.Path.of(
                "/tmp/protomolt-iceberg-warehouse/client/" + id.name());
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
