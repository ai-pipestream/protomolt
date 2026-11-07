package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Temporary-drive save semantics over real catalog transactions: durable destinations
 * refuse expiring carry/source references, placement to a temporary destination is
 * refused before copying, explicit promotion copies every temporary object to the
 * durable destination, and expired bytes fail exact reads as DATA_LOSS. The temporary
 * stack is an in-memory {@link Stack} whose classification is supplied through the
 * {@link DocumentSaves.Drives} resolver, exactly as the drive-ledger wiring does.
 */
class DocumentTemporaryDrivesIT {
    @Test
    void ordinaryDurableSaveRefusesTemporaryCarryAndInstructsPlacement() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "one")).setDrive("cache").build());
            var first = f.catalog.manifest(address, 1).orElseThrow();
            var refused = update("second", address, 1, blobsDocument("doc")).setDrive("durable")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS)).build();
            assertRejectionWithPlacementInstruction(() -> rpc.stub.saveDocument(refused));
            // The refusal is recorded, names the placement escape hatch, and publishes nothing.
            var outcome = f.catalog.outcome("a", "second").orElseThrow().outcome();
            assertThat(outcome.getRejection().getReason()).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(outcome.getRejection().getMessage()).contains("STORAGE_PLACEMENT_SELECTED_DRIVE");
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.manifest(address, 1)).contains(first);
            assertThat(f.catalog.manifest(address, 2)).isEmpty();
            assertThat(rpc.durableTarget.puts.get()).isZero();
            assertThat(rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument()
                    .getSearchMetadata().getTitle()).isEqualTo("one");
        }
    }

    @Test
    void ordinaryUnchangedSaveDoesNotResolveItsDriveTwice() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "one")).setDrive("cache").build());
            var unchanged = create("unchanged", address, document("a", "doc", "one"))
                    .setExpected(at(1))
                    .setDrive("cache")
                    .setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME).build();
            int resolutions = rpc.driveResolutions.get();

            var saved = rpc.stub.saveDocument(unchanged);

            assertThat(saved.getUnchanged()).isTrue();
            assertThat(saved.getAccession().getRevision()).isEqualTo(1);
            assertThat(rpc.driveResolutions.get()).isEqualTo(resolutions + 1);
            assertThat(f.catalog.outcome("a", "unchanged")).isPresent();
        }
    }

    @Test
    void refusedStreamingUploadCleansEveryAttemptAndReplaysOutcome() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "one")).setDrive("cache").build());
            var save = update("upload-second", address, 1, blobsDocument("doc")).setDrive("durable")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS));
            save.getDocumentBuilder().getBlobBagBuilder().getBlobBuilder().clearData();
            byte[] bytes = "raw bytes of doc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var header = UploadHeader.newBuilder().setStreamed(StreamedBody.getDefaultInstance())
                    .setSave(save).setBlobId("b1").setDeclaredLength(bytes.length)
                    .setExpectedSha256(SaveIntents.sha256Hex(bytes)).build();

            for (int attempt = 0; attempt < 2; attempt++) {
                assertThatThrownBy(() -> rpc.uploads.upload(processAuthority(), header,
                        new ByteArrayInputStream(bytes)))
                        .isInstanceOf(AccessionException.class)
                        .hasMessageContaining("STORAGE_PLACEMENT_SELECTED_DRIVE");
                assertThat(rpc.durableTarget.objects).isEmpty();
                assertThat(f.catalog.outcome("a", "upload-second")).isPresent();
                assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            }
        }
    }

    @Test
    void durableSaveRefusesTemporarySourcedPartBeforePublication() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var parent = address("a", "parent");
            var child = parent.toBuilder().setGraphAddressId("child").build();
            rpc.stub.saveDocument(create("create", parent, document("a", "parent", "kept")).setDrive("cache").build());
            var sourced = SaveDocumentRequest.newBuilder().setOperationId("child")
                    .setTarget(DocumentTarget.newBuilder().setAddress(child))
                    .setExpected(ExpectedRevision.newBuilder().setNone(true))
                    .setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId(child.getDocId()).setOwnership(ownership("a")))
                    .setOwner(intakeOwner()).setDrive("durable")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                    .addSources(PartSource.newBuilder().setFromAddress(parent).setFromRevision(1)
                            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)))
                    .build();
            assertRejectionWithPlacementInstruction(() -> rpc.stub.saveDocument(sourced));
            assertThat(f.catalog.current(child)).isEmpty();
            assertThat(rpc.durableTarget.puts.get()).isZero();
        }
    }

    @Test
    void promotionToTemporaryDestinationIsRefusedBeforeCopying() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).setDrive("default").build());
            var refused = promotion(address, "promote-down", 1).setDrive("cache").build();
            assertThatThrownBy(() -> rpc.stub.saveDocument(refused))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(failure.getMessage()).contains("temporary drive");
                    });
            assertThat(rpc.temp.puts.get()).isZero();
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            var outcome = f.catalog.outcome("a", "promote-down").orElseThrow();
            assertThat(outcome.outcome().getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThatThrownBy(() -> rpc.stub.saveDocument(refused))
                    .hasMessageContaining("temporary drive");
            assertThat(f.catalog.outcome("a", "promote-down")).contains(outcome);
        }
    }

    @Test
    void explicitPromotionCopiesEveryTemporaryObjectToTheDurableDestination() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "kept")).setDrive("cache").build());
            var original = f.catalog.manifest(address, 1).orElseThrow();
            var saved = rpc.stub.saveDocument(promotion(address, "promote", 1).build());
            assertThat(saved.getAccession().getRevision()).isEqualTo(2);
            var promoted = f.catalog.manifest(address, 2).orElseThrow();
            assertThat(f.catalog.manifest(address, 1)).contains(original);
            var durableId = rpc.durableTarget.stackId();
            for (var part : promoted.getPartsList()) {
                assertThat(part.getRef().getStackId()).isEqualTo(durableId);
                for (var blob : part.getBlobRefsList())
                    assertThat(blob.getRef().getStackId()).isEqualTo(durableId);
            }
            assertThat(rpc.durableTarget.objects).hasSize(4); // CORE, BLOBS fragment, raw blob, PARSED
            var read = rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
            assertThat(read.getSearchMetadata().getTitle()).isEqualTo("kept");
            var bytes = new java.io.ByteArrayOutputStream();
            rpc.stub.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build())
                    .forEachRemaining(frame -> { if (frame.hasData()) bytes.writeBytes(frame.getData().toByteArray()); });
            assertThat(bytes.toByteArray()).isEqualTo("raw bytes of kept".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void mixedSaveOnTemporaryDestinationKeepsDurableCarryForward() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "one")).setDrive("default").build());
            rpc.stub.saveDocument(update("second", address, 1, coreDocument("a", "doc", "two")).setDrive("cache")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build());
            var second = f.catalog.manifest(address, 2).orElseThrow();
            var core = second.getPartsList().stream()
                    .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
            var blobs = second.getPartsList().stream()
                    .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS).findFirst().orElseThrow();
            assertThat(core.getRef().getStackId()).isEqualTo(rpc.temp.stackId());
            assertThat(blobs.getRef().getStackId()).isEqualTo(f.stack.stackId());
            var read = rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument();
            assertThat(read.getSearchMetadata().getTitle()).isEqualTo("two");
            // Durable carry from a temporary revision stays refused on durable destinations.
            var third = update("third", address, 2, blobsDocument("doc")).setDrive("durable")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS)).build();
            assertRejectionWithPlacementInstruction(() -> rpc.stub.saveDocument(third));
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
        }
    }

    @Test
    void expiredExactReadsAreDataLossNeverFallbackAndPurgeCompletes() throws Exception {
        try (var f = new AccessionFixture(); var rpc = new TemporaryRpc(f)) {
            var address = address("a", "doc");
            rpc.stub.saveDocument(create("create", address, document("a", "doc", "one")).setDrive("cache").build());
            rpc.stub.saveDocument(update("second", address, 1, coreDocument("a", "doc", "two")).setDrive("cache")
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build());
            var first = f.catalog.manifest(address, 1).orElseThrow();
            var firstCore = first.getPartsList().stream()
                    .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
            rpc.temp.expire(firstCore.getRef());
            // The exact historical read fails DATA_LOSS through the existing StackDataLoss path.
            assertThatThrownBy(() -> rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
            // The current revision still reads: its carried objects did not expire with revision 1's CORE.
            assertThat(rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument()
                    .getSearchMetadata().getTitle()).isEqualTo("two");
            // Delete and purge after expiry: the reclaimer treats expired objects as physically absent.
            rpc.stub.deleteDocument(DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete")
                    .setExpected(at(2)).build());
            var purge = new DocumentPurges(f.catalog, rpc.drives,
                    (caller, account) -> new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(true, true), Clock.systemUTC());
            var result = purge.purge(writer("a", "administrator", UUID.randomUUID(), 1),
                    PurgeDocumentRequest.newBuilder().setAddress(address).setPurgeId("purge").build());
            assertThat(result.getObjectsToDelete() + result.getObjectsRetainedShared()).isGreaterThan(0);
            assertThat(rpc.temp.objects).isEmpty();
            assertThatThrownBy(() -> rpc.stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        }
    }

    private static SaveDocumentRequest.Builder update(String operation, ai.protomolt.proto.repo.v1.NodeAddress address,
            long revision, ai.protomolt.proto.repo.v1.Document document) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(revision)).setDocument(document).setOwner(intakeOwner());
    }

    /** A document fragment carrying CORE content only; every other part is carried. */
    private static ai.protomolt.proto.repo.v1.Document coreDocument(String account, String doc, String title) {
        return ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId(doc)
                .setOwnership(ownership(account))
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle(title)).build();
    }

    /** A document fragment carrying one inline raw blob for a BLOBS PUT; other parts are carried. */
    private static ai.protomolt.proto.repo.v1.Document blobsDocument(String doc) {
        return ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId(doc)
                .setBlobBag(ai.protomolt.proto.repo.v1.BlobBag.newBuilder().setBlob(ai.protomolt.proto.repo.v1.Blob
                        .newBuilder().setBlobId("b1").setData(com.google.protobuf.ByteString
                                .copyFromUtf8("raw bytes of " + doc)).build()).build())
                .build();
    }

    private static SaveDocumentRequest.Builder promotion(ai.protomolt.proto.repo.v1.NodeAddress address,
            String operation, long revision) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(revision))
                .setDrive("durable").setOwner(intakeOwner())
                .setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE);
    }

    private static void assertRejectionWithPlacementInstruction(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            var status = io.grpc.protobuf.StatusProto.fromThrowable(failure);
            assertThat(status.getDetailsList()).anySatisfy(detail -> {
                try {
                    var rejection = detail.unpack(SaveRejection.class);
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
                    assertThat(rejection.getMessage()).contains("STORAGE_PLACEMENT_SELECTED_DRIVE");
                    assertThat(rejection.getViolationsList()).isNotEmpty();
                } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
                    throw new AssertionError(invalid);
                }
            });
        });
    }

    /** The save/read surface over one durable (fixture S3), one temporary and one durable target stack. */
    private static final class TemporaryRpc implements AutoCloseable {
        final MemoryStack temp;
        final MemoryStack durableTarget;
        final DocumentSaves.Drives drives;
        final DocumentUploads uploads;
        final AtomicInteger driveResolutions = new AtomicInteger();
        final Server server;
        final ManagedChannel channel;
        final DocumentServiceGrpc.DocumentServiceBlockingStub stub;

        TemporaryRpc(AccessionFixture f) throws Exception {
            temp = new MemoryStack("stack-temp-" + UUID.randomUUID());
            durableTarget = new MemoryStack("stack-durable-" + UUID.randomUUID());
            f.catalog.registerStack(StackRecord.newBuilder().setStackId(temp.stackId())
                    .setProvider("memory").setEndpoint("memory://temporary")
                    .setNamespace("memory/" + UUID.randomUUID()).build());
            f.catalog.registerStack(StackRecord.newBuilder().setStackId(durableTarget.stackId())
                    .setProvider("memory").setEndpoint("memory://durable")
                    .setNamespace("memory/" + UUID.randomUUID()).build());
            var byName = Map.of("default", f.stack, "cache", temp, "durable", durableTarget);
            drives = DocumentSaves.Drives.dynamic("default",
                    (account, drive) -> {
                        driveResolutions.incrementAndGet();
                        return Optional.ofNullable(byName.get(drive.isEmpty() ? "default" : drive));
                    },
                    stackId -> byName.values().stream().filter(stack -> stack.stackId().equals(stackId)).findFirst(),
                    stackId -> stackId.equals(temp.stackId()) ? Optional.of(true) : Optional.empty());
            var saves = new DocumentSaves(f.catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
            var reads = new DocumentReads(f.catalog, drives, limits(), bytes -> () -> {}, Clock.systemUTC());
            uploads = new DocumentUploads(saves);
            var service = new AccessionGrpcService(saves, uploads, reads,
                    new DocumentDeletes(f.catalog, Clock.systemUTC()), null, null, ignored -> processAuthority());
            var caller = processAuthority();
            var interceptor = new ServerInterceptor() {
                @Override public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                        Metadata headers, ServerCallHandler<Q, R> next) {
                    var principal = new Caller(caller.principalName(), Set.of(), caller.processAuthority());
                    return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, principal)
                            .withValue(CallerContexts.AUTHENTICATED_CALLER,
                                    new ai.protomolt.proto.authz.AuthenticatedCaller(principal, java.util.Optional.empty())), call, headers, next);
                }
            };
            String name = "temporary-drives-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(ServerInterceptors.intercept(service, interceptor)).build().start();
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            stub = DocumentServiceGrpc.newBlockingStub(channel);
        }

        @Override public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    /** In-memory stack; removing an object models its TTL expiry. */
    static final class MemoryStack implements Stack {
        private final String stackId;
        final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        final AtomicInteger puts = new AtomicInteger();

        MemoryStack(String stackId) { this.stackId = stackId; }

        @Override public String stackId() { return stackId; }

        @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
            var bytes = body.readAllBytes();
            if (expectedLength >= 0 && bytes.length != expectedLength)
                throw new IOException("Stack write body ended at " + bytes.length + " of " + expectedLength + " bytes");
            return store(bytes);
        }

        @Override public StackRef put(byte[] body) { return store(body.clone()); }

        private StackRef store(byte[] bytes) {
            var key = UUID.randomUUID().toString();
            objects.put(key, bytes);
            puts.incrementAndGet();
            return StackRef.newBuilder().setStackId(stackId).setKey(key)
                    .setSha256(sha256(bytes)).setLength(bytes.length).build();
        }

        /** Expiry model: the bytes vanish exactly as a per-object TTL would leave them. */
        void expire(StackRef ref) { objects.remove(ref.getKey()); }

        @Override public InputStream open(StackRef ref) throws IOException {
            var bytes = objects.get(ref.getKey());
            if (bytes == null || bytes.length != ref.getLength() || !sha256(bytes).equals(ref.getSha256()))
                throw new StackDataLoss("Referenced stack object is missing");
            return new ByteArrayInputStream(bytes);
        }

        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            Stack.requireRange(ref, offset, length);
            if (offset == 0 && length == ref.getLength()) return open(ref);
            var bytes = objects.get(ref.getKey());
            if (bytes == null) throw new StackDataLoss("Referenced stack object is missing");
            return new ByteArrayInputStream(bytes, (int) offset, (int) length);
        }

        @Override public long head(StackRef ref) throws IOException {
            var bytes = objects.get(ref.getKey());
            if (bytes == null) throw new StackDataLoss("Referenced stack object is missing");
            return bytes.length;
        }

        @Override public void delete(StackRef ref) { objects.remove(ref.getKey()); }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
