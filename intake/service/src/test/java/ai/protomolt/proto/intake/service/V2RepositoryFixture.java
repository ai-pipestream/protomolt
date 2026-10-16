package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.repo.accession.*;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v2.StackRecord;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Clock;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Real v2 catalog/admission/stack fixture; no legacy RepoServices implementation. */
final class V2RepositoryFixture implements AutoCloseable {
    final PostgresCatalog catalog;
    final BlobStoreStack stack;
    final S3Client client;
    final Server server;

    V2RepositoryFixture(PostgreSQLContainer postgres, LocalStackContainer localstack, String name) throws Exception {
        catalog = new PostgresCatalog(new CatalogConfig(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), 4));
        client = S3Client.builder().endpointOverride(localstack.getEndpoint()).region(Region.of(localstack.getRegion()))
                .forcePathStyle(true).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create()).build();
        client.createBucket(CreateBucketRequest.builder().bucket(name).build());
        String id = catalog.registerStack(StackRecord.newBuilder().setStackId(name).setProvider("s3")
                .setEndpoint(localstack.getEndpoint().toString()).setNamespace(name).build());
        stack = new BlobStoreStack(id, new S3BlobStore(client), name, "objects");
        var drives = new DocumentSaves.Drives("intake", Map.of("intake", stack));
        // Explicit fixture resource budget, not an application document limit.
        var limits = new DocumentSchemaAdmission.Limits(16, 64L * 1024 * 1024, 64,
                4L * 1024 * 1024, 32, 32L * 1024 * 1024, 64 * 1024 * 1024);
        var saves = new DocumentSaves(catalog, drives, selection -> null, limits, bytes -> () -> {}, Clock.systemUTC());
        var reads = new DocumentReads(catalog, drives, limits, bytes -> () -> {}, Clock.systemUTC());
        server = InProcessServerBuilder.forName(name).addService(new AccessionGrpcService(saves, reads,
                authentication -> new RepositoryCaller(authentication.caller().name(), authentication.caller().unrestricted())))
                .build().start();
    }

    byte[] retainedBlob(ai.protomolt.proto.repo.v1.NodeAddress address, long revision) throws Exception {
        var ref = catalog.manifest(address, revision).orElseThrow().getPartsList().stream().filter(p -> p.getBlobRefsCount() > 0).findFirst().orElseThrow().getBlobRefs(0).getRef();
        try (var input = stack.open(ref)) { return input.readAllBytes(); }
    }

    @Override public void close() {
        server.shutdownNow();
        catalog.close();
        client.close();
    }
}
