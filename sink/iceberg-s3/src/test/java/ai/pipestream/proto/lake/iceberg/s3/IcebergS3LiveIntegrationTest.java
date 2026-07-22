package ai.pipestream.proto.lake.iceberg.s3;

import ai.pipestream.proto.lake.iceberg.IcebergSink;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole lane on an S3-compatible object store: an Iceberg REST catalog whose warehouse lives
 * on LocalStack through {@code S3FileIO}, both provisioned by Testcontainers (the catalog is the
 * {@code apache/iceberg-rest-fixture} image docker-compose.integration.yml runs). Tables are
 * created and committed over REST, the data files land on LocalStack as {@code s3://} objects,
 * and Iceberg's own reader gets every value back. Unlike the file:// suite there is no shared
 * warehouse volume: the catalog container and this JVM each reach the store by URL and resolve
 * the same keys. The suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class IcebergS3LiveIntegrationTest {

    private static final String BUCKET = "protomolt-lake";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.13"))
            .withServices("s3")
            .withNetwork(NETWORK)
            // The catalog container reaches the store by this alias; the test JVM uses the
            // mapped port on the host. Both resolve the same s3:// keys.
            .withNetworkAliases("localstack");

    @Container
    static final GenericContainer<?> CATALOG_SERVICE = new GenericContainer<>(
            DockerImageName.parse("apache/iceberg-rest-fixture:1.10.1"))
            .withNetwork(NETWORK)
            .dependsOn(S3)
            .withExposedPorts(8181)
            .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/warehouse")
            .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .withEnv("CATALOG_S3_ENDPOINT", "http://localstack:4566")
            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .withEnv("AWS_ACCESS_KEY_ID", S3.getAccessKey())
            .withEnv("AWS_SECRET_ACCESS_KEY", S3.getSecretKey())
            .withEnv("AWS_REGION", S3.getRegion())
            .waitingFor(Wait.forHttp("/v1/config").forPort(8181));

    private static String catalogUri() {
        return "http://" + CATALOG_SERVICE.getHost() + ":" + CATALOG_SERVICE.getMappedPort(8181);
    }

    private static final String PROTO = """
            syntax = "proto3";
            package lakes3.v1;
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
        createBucket();
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lakes3/v1/tick.proto", PROTO, "test").build());
        file = compiled.descriptorFor("lakes3/v1/tick.proto").orElseThrow();

        Map<String, String> props = new HashMap<>(S3Catalogs.pathStyle(
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey()));
        props.put(CatalogProperties.URI, catalogUri());
        catalog = new RESTCatalog();
        catalog.initialize("live-s3", props);
        try {
            catalog.createNamespace(Namespace.of("protomolt_s3"));
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

    /** The warehouse bucket has to exist before the catalog writes metadata into it. */
    private static void createBucket() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(S3.getEndpoint())
                .region(Region.of(S3.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            try {
                s3.createBucket(b -> b.bucket(BUCKET));
            } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
                // reruns share the bucket
            }
        }
    }

    private static DynamicMessage tick(Descriptor type, int i) {
        Descriptor meta = file.findMessageTypeByName("Meta");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "SYM" + i)
                .setField(type.findFieldByName("price"), 100.0 + i)
                .setField(type.findFieldByName("meta"), DynamicMessage.newBuilder(meta)
                        .setField(meta.findFieldByName("source"), "s3-test")
                        .setField(meta.findFieldByName("sequence"), (long) i)
                        .build());
        builder.addRepeatedField(type.findFieldByName("venues"), "NYSE");
        return builder.build();
    }

    @Test
    void appendsAndReadsBackThroughS3() throws Exception {
        Descriptor type = file.findMessageTypeByName("Tick");
        TableIdentifier id = TableIdentifier.of("protomolt_s3",
                "ticks_" + Long.toUnsignedString(System.nanoTime(), 36));

        Table table = IcebergSink.ensureTable(catalog, id, type);
        try {
            List<DataFile> files = IcebergSink.append(table, type,
                    List.of(tick(type, 0), tick(type, 1)));
            // The data file is an object on the store, not a local path.
            assertThat(files.getFirst().location()).startsWith("s3://" + BUCKET + "/");
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
            rows.sort(Comparator.comparing(r -> (String) r.getField("symbol")));
            Record row = rows.get(2);
            assertThat(row.getField("symbol")).isEqualTo("SYM2");
            assertThat(row.getField("price")).isEqualTo(102.0);
            assertThat((List<?>) row.getField("venues")).first().isEqualTo("NYSE");
            assertThat(((Record) row.getField("meta")).getField("sequence")).isEqualTo(2L);

            assertThat(catalog.listTables(Namespace.of("protomolt_s3"))).contains(id);
        } finally {
            // On S3 the data files use the same credentials, so a purging drop is safe.
            catalog.dropTable(id, true);
        }
    }
}
