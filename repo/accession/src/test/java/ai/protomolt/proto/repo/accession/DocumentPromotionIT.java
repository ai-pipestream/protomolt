package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Real catalog/provider transactions with failures ordered at actual destination writes. */
class DocumentPromotionIT {
    @Test void failedTransferDoesNotPublishAndSameIntentCanRetry() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).build());
            var before = f.catalog.manifest(address, 1).orElseThrow();
            rpc.target.afterPut = () -> { throw new IOException("destination disconnected after storing one object"); };
            var request = promotion(address);
            assertCode(Status.Code.UNAVAILABLE, () -> rpc.stub.saveDocument(request));
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.outcome("a", "promote")).isEmpty();
            assertThat(f.catalog.events(address)).hasSize(1);
            assertThat(f.catalog.manifest(address, 1)).contains(before);
            rpc.target.afterPut = () -> {};
            assertThat(rpc.stub.saveDocument(request).getAccession().getRevision()).isEqualTo(2);
            assertThat(f.catalog.events(address)).hasSize(2);
            var read = rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
            assertThat(read.getStructuredData()).isEqualTo(document("a", "doc", "kept").getStructuredData());
            assertThat(read.getParserResultsMap()).isEqualTo(document("a", "doc", "kept").getParserResultsMap());
            assertThat(f.catalog.manifest(address, 2).orElseThrow().getValidation().getPartSchemasList())
                    .isEqualTo(before.getValidation().getPartSchemasList());
        }
    }

    @Test void grantRevocationDuringPromotionRefusesTheCommit() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "doc");
            f.saves.save(processAuthority(), create("create", address, document("a", "doc", "kept", "promoter")).build());
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "promoter");
            var grant = f.intakeGrant("a", credential, "ds-1", false);
            try (var rpc = new PromotionRpc(f, writer("a", "promoter", credential, 1))) {
                rpc.target.afterPut = () -> f.catalog.revokeGrant(grant.getGrantId());
                assertCode(Status.Code.PERMISSION_DENIED, () -> rpc.stub.saveDocument(promotion(address)));
                assertThat(rpc.target.puts.get()).isGreaterThan(0);
                assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
                assertThat(f.catalog.outcome("a", "promote")).isEmpty();
                int written = rpc.target.puts.get();
                // A retry without authority fails before any further object transfer.
                assertCode(Status.Code.PERMISSION_DENIED, () -> rpc.stub.saveDocument(promotion(address)));
                assertThat(rpc.target.puts).hasValue(written);
            }
        }
    }

    @Test void concurrentRevisionWinsWithoutPublishingPartialPromotion() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "old")).build());
            var once = new AtomicBoolean();
            rpc.target.afterPut = () -> {
                if (once.compareAndSet(false, true))
                    f.saves.save(processAuthority(), create("winner", address, document("a", "doc", "newer"))
                            .setExpected(at(1)).build());
            };
            assertCode(Status.Code.ABORTED, () -> rpc.stub.saveDocument(promotion(address)));
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
            assertThat(f.catalog.manifest(address, 2).orElseThrow().getOperationId()).isEqualTo("winner");
            assertThat(f.catalog.events(address)).hasSize(2);
            assertThat(f.catalog.outcome("a", "promote")).isEmpty();
        }
    }

    @Test void copiedSourcesRemainFencedAgainstPurgeBeforeCommit() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var source = address("a", "doc");
            var child = source.toBuilder().setGraphAddressId("promoted-child").build();
            rpc.stub.saveDocument(create("create", source, document("a", "doc", "kept")).build());
            // This save writes CORE and copies PARSED, BLOBS and its raw object.
            // Purge after the final completed transfer, before the catalog transaction.
            rpc.target.afterPut = () -> {
                if (rpc.target.puts.get() == 4)
                    DocumentPurgesIT.purges(f).purge(DocumentPurgesIT.administrator(),
                            DocumentPurgesIT.request("source-purge", source));
            };
            var request = DocumentPurgesIT.source(source, child).toBuilder().setOperationId("promote-child")
                    .setDrive("durable").setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE).build();
            assertCode(Status.Code.FAILED_PRECONDITION, () -> rpc.stub.saveDocument(request));
            assertThat(rpc.target.puts).hasValue(4);
            assertThat(f.catalog.current(child)).isEmpty();
            assertThat(f.catalog.events(child)).isEmpty();
            assertThat(f.catalog.outcome("a", "promote-child").orElseThrow().outcome().getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_SOURCE_PURGED);
        }
    }

    @Test void missingHistoricalBytesAreDataLossNotAnEmptyPromotion() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).build());
            var original = f.catalog.manifest(address, 1).orElseThrow();
            f.stack.delete(original.getParts(0).getRef());
            assertCode(Status.Code.DATA_LOSS, () -> rpc.stub.saveDocument(promotion(address)));
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.outcome("a", "promote")).isEmpty();
            assertThat(f.catalog.manifest(address, 1)).contains(original);
        }
    }

    @Test void interruptedPromotionStopsBeforePublicationAndCanRetry() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).build());
            rpc.target.afterPut = () -> Thread.currentThread().interrupt();
            try {
                assertCode(Status.Code.CANCELLED, () -> rpc.stub.saveDocument(promotion(address)));
            } finally {
                // The in-process direct executor ran the interrupted worker on this test thread.
                Thread.interrupted();
            }
            assertThat(rpc.target.puts).hasValue(1);
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.outcome("a", "promote")).isEmpty();
            rpc.target.afterPut = () -> {};
            assertThat(rpc.stub.saveDocument(promotion(address)).getAccession().getRevision()).isEqualTo(2);
        }
    }

    @Test void sameLengthRawCorruptionFailsDigestVerificationBeforePublication() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).build());
            var original = f.catalog.manifest(address, 1).orElseThrow();
            var raw = original.getPartsList().stream().flatMap(part -> part.getBlobRefsList().stream())
                    .findFirst().orElseThrow().getRef();
            var bytes = s3().getObjectAsBytes(r -> r.bucket(BUCKET).key(raw.getKey())).asByteArray();
            bytes[0] ^= 1;
            s3().putObject(r -> r.bucket(BUCKET).key(raw.getKey()),
                    software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes));
            assertCode(Status.Code.DATA_LOSS, () -> rpc.stub.saveDocument(promotion(address)));
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.outcome("a", "promote")).isEmpty();
            assertThat(f.catalog.manifest(address, 1)).contains(original);
        }
    }

    @Test void promotionRejectsAmbiguousPlacementAndUnchangedOutcomeModeBeforeWriting() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new PromotionRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).build());
            for (var request : java.util.List.of(promotion(address).toBuilder().clearDrive().build(),
                    promotion(address).toBuilder().setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME).build(),
                    promotion(address).toBuilder().setStoragePlacementValue(900).build()))
                assertCode(Status.Code.INVALID_ARGUMENT, () -> rpc.stub.saveDocument(request));
            assertThat(rpc.target.puts).hasValue(0);
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
        }
    }

    private static SaveDocumentRequest promotion(ai.protomolt.proto.repo.v1.NodeAddress address) {
        return SaveDocumentRequest.newBuilder().setOperationId("promote")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDrive("durable").setOwner(intakeOwner())
                .setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE).build();
    }

    private static void assertCode(Status.Code code, org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }

    private static final class PromotionRpc implements AutoCloseable {
        final CountingStack target;
        final Server server;
        final ManagedChannel channel;
        final DocumentServiceGrpc.DocumentServiceBlockingStub stub;
        PromotionRpc(AccessionFixture f) throws Exception { this(f, processAuthority()); }
        PromotionRpc(AccessionFixture f, ai.protomolt.proto.repo.spi.RepositoryCaller caller) throws Exception {
            String prefix = "promotion/" + UUID.randomUUID();
            String id = f.catalog.registerStack(StackRecord.newBuilder().setStackId("stack-" + UUID.randomUUID())
                    .setProvider("s3").setEndpoint(S3.getEndpoint().toString()).setNamespace(BUCKET + "/" + prefix).build());
            target = new CountingStack(new BlobStoreStack(id, new S3BlobStore(s3()), BUCKET, prefix));
            var drives = new DocumentSaves.Drives("default", Map.of("default", f.stack, "durable", target));
            var saves = new DocumentSaves(f.catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
            var reads = new DocumentReads(f.catalog, drives, limits(), bytes -> () -> {}, Clock.systemUTC());
            var service = new AccessionGrpcService(saves, new DocumentUploads(saves), reads, ignored -> caller);
            var interceptor = new ServerInterceptor() {
                @Override public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                        Metadata headers, ServerCallHandler<Q, R> next) {
                    var principal = new Caller(caller.principalName(), Set.of(), caller.processAuthority());
                    var binding = caller.credentialBinding().map(value -> new ai.protomolt.proto.authz.CredentialBinding(
                            value.issuer(), value.credentialId(), value.generation()));
                    return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, principal)
                            .withValue(CallerContexts.AUTHENTICATED_CALLER,
                                    new ai.protomolt.proto.authz.AuthenticatedCaller(principal, binding)), call, headers, next);
                }
            };
            String name = "promotion-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(ServerInterceptors.intercept(service, interceptor)).build().start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            stub = DocumentServiceGrpc.newBlockingStub(channel);
        }
        @Override public void close() { channel.shutdownNow(); server.shutdownNow(); }
    }
}
