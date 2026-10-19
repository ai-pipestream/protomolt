package ai.protomolt.proto.parse.service;

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

    V2RepositoryFixture(PostgreSQLContainer postgres, LocalStackContainer localstack, String name, String account) throws Exception {
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
        var saves = new DocumentSaves(catalog, drives, selection -> selection.typeUrl().equals("type.googleapis.com/ai.protomolt.proto.repo.v1.SearchMetadata")
                ? BuiltinDocumentSchema.definition(ai.protomolt.proto.repo.v1.SearchMetadata.getDescriptor()) : null, limits, bytes -> () -> {}, Clock.systemUTC());
        var reads = new DocumentReads(catalog, drives, limits, bytes -> () -> {}, Clock.systemUTC());
        var credentialId = java.util.UUID.randomUUID();
        catalog.registerCredential("d8-test", credentialId, "parse-coordinator");
        var credential = ai.protomolt.proto.repo.v2.WriterCredential.newBuilder()
                .setIssuer("d8-test").setCredentialId(credentialId.toString());
        catalog.installGrant(ai.protomolt.proto.repo.v2.WriterGrant.newBuilder().setGrantId("parse")
                .setAccountId(account).setCredential(credential)
                .setStep(ai.protomolt.proto.repo.v2.StepWriterScope.newBuilder().setModuleId("parse-coordinator")).build());
        catalog.installGrant(ai.protomolt.proto.repo.v2.WriterGrant.newBuilder().setGrantId("seed")
                .setAccountId(account).setCredential(credential).setAllowCreate(true)
                .setIntake(ai.protomolt.proto.repo.v2.IntakeOrigin.newBuilder().setDatasourceId("ds-parse")).build());
        var caller = ai.protomolt.proto.actions.Caller.scoped("parse-coordinator", java.util.Set.of());
        var authentication = new ai.protomolt.proto.authz.AuthenticatedCaller(caller,
                java.util.Optional.of(new ai.protomolt.proto.authz.CredentialBinding("d8-test", credentialId, 1)));
        var service = new AccessionGrpcService(saves, new DocumentUploads(saves), reads,
                new DocumentDeletes(catalog, Clock.systemUTC()), new DocumentLists(catalog),
                new DocumentPurges(catalog, drives, (identity, tenant) ->
                        new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(false, false), Clock.systemUTC()), auth -> new RepositoryCaller(auth.caller().name(), false,
                java.util.Set.of(account), java.util.Set.of(ai.protomolt.proto.repo.v1.Principal.newBuilder()
                        .setIdentityType("service").setIdentity("parse-coordinator").build()),
                java.util.Optional.of(new ai.protomolt.proto.repo.spi.RepositoryCredentialBinding("d8-test", credentialId, 1))));
        // Fixed, trusted in-process fixture authentication, not a client-controlled identity header.
        var interceptor = new io.grpc.ServerInterceptor() {
            @Override public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                    io.grpc.ServerCall<ReqT, RespT> call, io.grpc.Metadata headers,
                    io.grpc.ServerCallHandler<ReqT, RespT> next) {
                return io.grpc.Contexts.interceptCall(io.grpc.Context.current()
                        .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.CALLER, caller)
                        .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.AUTHENTICATED_CALLER, authentication),
                        call, headers, next);
            }
        };
        server = InProcessServerBuilder.forName(name).addService(io.grpc.ServerInterceptors.intercept(service, interceptor))
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
