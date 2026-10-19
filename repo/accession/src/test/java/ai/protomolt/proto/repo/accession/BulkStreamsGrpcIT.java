package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * The slice 6 bulk streams (requirements §11): one stream carries many saves or
 * reads, each answered under its request_id. A failed item is its own ItemError
 * and never aborts the stream; an item envelope without a correlatable identity
 * (blank or duplicate request_id, or no operation at all) fails the stream with
 * INVALID_ARGUMENT. Authentication is captured when the stream opens.
 */
class BulkStreamsGrpcIT {

    @Test void savesAnswerPerItemAndKeepTheStreamAliveAcrossFailures() throws Exception {
        try (var f = new AccessionFixture(); var server = server(f)) {
            var stub = DocumentServiceGrpc.newStub(server.channel);
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "doc-bulk");
            var created = create("op-created", address, document(account, "doc-bulk", "first")).build();
            var stale = create("op-stale", address, document(account, "doc-bulk", "stale")).setExpected(at(4)).build();
            var malformed = create("op-malformed", address, document(account, "doc-bulk", "malformed")).clearOwner().build();

            var collector = new CollectingObserver<SaveDocumentsResponse>();
            var stream = stub.saveDocuments(collector);
            stream.onNext(saveItem("req-created", created));
            stream.onNext(saveItem("req-stale", stale));
            stream.onNext(saveItem("req-malformed", malformed));
            stream.onNext(saveItem("req-replay", created));
            stream.onCompleted();
            collector.await();
            assertThat(collector.error).isNull();

            var byId = collector.responses.stream()
                    .collect(Collectors.toMap(SaveDocumentsResponse::getRequestId, Function.identity()));
            assertThat(byId.keySet()).containsExactlyInAnyOrder("req-created", "req-stale", "req-malformed", "req-replay");
            var first = byId.get("req-created");
            assertThat(first.getSaved().getAccession().getRevision()).isEqualTo(1);
            assertThat(first.getSaved().getReplayed()).isFalse();
            assertThat(first.getOperationId()).isEqualTo("op-created");

            var conflict = byId.get("req-stale");
            assertThat(conflict.getOperationId()).isEqualTo("op-stale");
            assertThat(conflict.getError().getCode()).isEqualTo(Status.Code.ABORTED.value());
            assertThat(conflict.getError().getConflict().getCurrentRevision()).isEqualTo(1);
            assertThat(f.catalog.outcome(account, "op-stale")).as("a conflict is not a recorded outcome").isEmpty();

            var refused = byId.get("req-malformed");
            assertThat(refused.getOperationId()).isEqualTo("op-malformed");
            assertThat(refused.getError().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT.value());
            assertThat(refused.getError().getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
            assertThat(f.catalog.outcome(account, "op-malformed")).isEmpty();

            var replay = byId.get("req-replay");
            assertThat(replay.getSaved().getAccession().getRevision()).isEqualTo(1);
            assertThat(replay.getSaved().getReplayed()).isTrue();
        }
    }

    @Test void saveStreamEnvelopeViolationsFailTheStream() throws Exception {
        try (var f = new AccessionFixture(); var server = server(f)) {
            var stub = DocumentServiceGrpc.newStub(server.channel);
            var account = "acct-" + UUID.randomUUID();

            var blank = new CollectingObserver<SaveDocumentsResponse>();
            var blankStream = stub.saveDocuments(blank);
            blankStream.onNext(SaveDocumentsRequest.newBuilder().setSave(create("op-blank", address(account, "doc-blank"),
                    document(account, "doc-blank", "x")).build()).build());
            blankStream.onCompleted();
            blank.await();
            assertStreamFailed(blank, Status.Code.INVALID_ARGUMENT);
            assertThat(blank.responses).isEmpty();
            assertThat(f.catalog.outcome(account, "op-blank")).isEmpty();

            var missing = new CollectingObserver<SaveDocumentsResponse>();
            var missingStream = stub.saveDocuments(missing);
            missingStream.onNext(SaveDocumentsRequest.newBuilder().setRequestId("no-save").build());
            missingStream.onCompleted();
            missing.await();
            assertStreamFailed(missing, Status.Code.INVALID_ARGUMENT);
            assertThat(missing.responses).isEmpty();

            var duplicate = new CollectingObserver<SaveDocumentsResponse>();
            var duplicateStream = stub.saveDocuments(duplicate);
            var first = saveItem("same", create("op-dup-1", address(account, "doc-dup-1"),
                    document(account, "doc-dup-1", "d1")).build());
            duplicateStream.onNext(first);
            duplicateStream.onNext(saveItem("same", create("op-dup-2", address(account, "doc-dup-2"),
                    document(account, "doc-dup-2", "d2")).build()));
            duplicateStream.onNext(saveItem("never-answered", create("op-dup-3", address(account, "doc-dup-3"),
                    document(account, "doc-dup-3", "d3")).build()));
            duplicateStream.onCompleted();
            duplicate.await();
            assertStreamFailed(duplicate, Status.Code.INVALID_ARGUMENT);
            assertThat(duplicate.responses).hasSize(1);
            assertThat(duplicate.responses.getFirst().getSaved().getAccession().getRevision()).isEqualTo(1);
            assertThat(f.catalog.outcome(account, "op-dup-2")).as("the duplicate item never ran").isEmpty();
            assertThat(f.catalog.outcome(account, "op-dup-3")).as("items after the violation never ran").isEmpty();
        }
    }

    @Test void readsStreamFramesPerItemAndIsolateFailures() throws Exception {
        try (var f = new AccessionFixture(); var server = server(f)) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "doc-read");
            var document = document(account, "doc-read", "bulk read");
            f.saves.save(processAuthority(), create("op-save", address, document).build());
            var stored = f.catalog.manifest(address, 1).orElseThrow();

            var stub = DocumentServiceGrpc.newStub(server.channel);
            var collector = new CollectingObserver<ReadDocumentsResponse>();
            var stream = stub.readDocuments(collector);
            stream.onNext(readItem("all", ReadDocumentRequest.newBuilder().setAddress(address).build()));
            stream.onNext(readItem("core-only", ReadDocumentRequest.newBuilder().setAddress(address)
                    .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_CORE))).build()));
            stream.onNext(readItem("missing", ReadDocumentRequest.newBuilder()
                    .setAddress(address(account, "doc-absent")).build()));
            stream.onNext(readItem("absent-revision", ReadDocumentRequest.newBuilder().setAddress(address)
                    .setRevision(RevisionSelector.newBuilder().setRevision(9)).build()));
            stream.onCompleted();
            collector.await();
            assertThat(collector.error).isNull();

            var byId = collector.responses.stream().collect(Collectors.groupingBy(ReadDocumentsResponse::getRequestId));
            assertThat(byId.keySet()).containsExactlyInAnyOrder("all", "core-only", "missing", "absent-revision");

            var all = byId.get("all");
            assertThat(all.getFirst().getFrame().hasManifest()).isTrue();
            assertThat(all.getLast().getFrame().hasComplete()).isTrue();
            var partFrames = all.subList(1, all.size() - 1);
            assertThat(partFrames).allSatisfy(response -> assertThat(response.getFrame().hasPartData()).isTrue());
            assertThat(partFrames.stream().map(response -> response.getFrame().getPartData().getLast()).toList())
                    .containsExactly(true, true, true);
            var merged = ai.protomolt.proto.repo.v1.Document.newBuilder();
            for (var response : partFrames) merged.mergeFrom(response.getFrame().getPartData().getData());
            assertThat(merged.build()).isEqualTo(claimChecked(document, stored));

            var coreOnly = byId.get("core-only");
            assertThat(coreOnly).hasSize(3);
            assertThat(coreOnly.get(0).getFrame().getManifest().getPartsList()).allSatisfy(part ->
                    assertThat(part.getContentIncluded()).isEqualTo(
                            part.getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE));
            assertThat(coreOnly.get(1).getFrame().getPartData().getSelector().getPart())
                    .isEqualTo(DocumentPart.DOCUMENT_PART_CORE);
            assertThat(coreOnly.get(2).getFrame().hasComplete()).isTrue();

            for (var failed : List.of("missing", "absent-revision")) {
                assertThat(byId.get(failed)).hasSize(1);
                var error = byId.get(failed).getFirst().getError();
                assertThat(error.getCode()).isEqualTo(Status.Code.NOT_FOUND.value());
            }
            for (var entry : byId.entrySet()) {
                var last = entry.getValue().getLast();
                var complete = last.hasFrame() && last.getFrame().hasComplete();
                assertThat(complete ^ last.hasError())
                        .as("item %s ends in complete or error, never both", entry.getKey()).isTrue();
            }
        }
    }

    @Test void aDamagedPartIsAnItemDataLossAndTheStreamSurvives() throws Exception {
        try (var f = new AccessionFixture(); var server = server(f)) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "doc-damaged");
            var document = document(account, "doc-damaged", "damaged");
            f.saves.save(processAuthority(), create("op-save", address, document).build());
            var stored = f.catalog.manifest(address, 1).orElseThrow();
            var core = stored.getPartsList().stream().filter(part ->
                    part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
            var damaged = s3().getObjectAsBytes(GetObjectRequest.builder().bucket(AccessionFixture.BUCKET)
                    .key(core.getRef().getKey()).build()).asByteArray();
            damaged[0] ^= 1;
            s3().putObject(PutObjectRequest.builder().bucket(AccessionFixture.BUCKET).key(core.getRef().getKey()).build(),
                    RequestBody.fromBytes(damaged));

            var stub = DocumentServiceGrpc.newStub(server.channel);
            var collector = new CollectingObserver<ReadDocumentsResponse>();
            var stream = stub.readDocuments(collector);
            stream.onNext(readItem("damaged", ReadDocumentRequest.newBuilder().setAddress(address)
                    .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_CORE))).build()));
            stream.onNext(readItem("intact", ReadDocumentRequest.newBuilder().setAddress(address)
                    .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_BLOBS))).build()));
            stream.onCompleted();
            collector.await();
            assertThat(collector.error).isNull();

            var byId = collector.responses.stream().collect(Collectors.groupingBy(ReadDocumentsResponse::getRequestId));
            var damagedItem = byId.get("damaged");
            assertThat(damagedItem.getLast().getError().getCode()).isEqualTo(Status.Code.DATA_LOSS.value());
            assertThat(damagedItem).noneMatch(response -> response.hasFrame() && response.getFrame().hasComplete());
            var intact = byId.get("intact");
            assertThat(intact.getLast().getFrame().hasComplete()).isTrue();
            var blobBytes = ByteString.empty();
            for (var response : intact)
                if (response.getFrame().hasPartData()) blobBytes = blobBytes.concat(response.getFrame().getPartData().getData());
            var blobs = ai.protomolt.proto.repo.v1.Document.parseFrom(blobBytes);
            assertThat(blobs.getBlobBag().getBlob().getBlobId()).isEqualTo("b1");
            assertThat(blobs.getBlobBag().getBlob().hasStorageRef()).isTrue();
        }
    }

    @Test void readStreamEnvelopeViolationsFailTheStream() throws Exception {
        try (var f = new AccessionFixture(); var server = server(f)) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "doc-rv");
            f.saves.save(processAuthority(), create("op-save", address, document(account, "doc-rv", "rv")).build());
            var stub = DocumentServiceGrpc.newStub(server.channel);

            var blank = new CollectingObserver<ReadDocumentsResponse>();
            var blankStream = stub.readDocuments(blank);
            blankStream.onNext(ReadDocumentsRequest.newBuilder()
                    .setRead(ReadDocumentRequest.newBuilder().setAddress(address)).build());
            blankStream.onCompleted();
            blank.await();
            assertStreamFailed(blank, Status.Code.INVALID_ARGUMENT);
            assertThat(blank.responses).isEmpty();

            var missing = new CollectingObserver<ReadDocumentsResponse>();
            var missingStream = stub.readDocuments(missing);
            missingStream.onNext(ReadDocumentsRequest.newBuilder().setRequestId("no-read").build());
            missingStream.onCompleted();
            missing.await();
            assertStreamFailed(missing, Status.Code.INVALID_ARGUMENT);

            var duplicate = new CollectingObserver<ReadDocumentsResponse>();
            var duplicateStream = stub.readDocuments(duplicate);
            duplicateStream.onNext(readItem("same", ReadDocumentRequest.newBuilder().setAddress(address).build()));
            duplicateStream.onNext(readItem("same", ReadDocumentRequest.newBuilder().setAddress(address).build()));
            duplicateStream.onCompleted();
            duplicate.await();
            assertStreamFailed(duplicate, Status.Code.INVALID_ARGUMENT);
            assertThat(duplicate.responses).allSatisfy(response -> assertThat(response.getRequestId()).isEqualTo("same"));
            assertThat(duplicate.responses.getLast().getFrame().hasComplete()).isTrue();
        }
    }

    @Test void anUnbindableCallerFailsBothStreamsAtOpen() throws Exception {
        try (var f = new AccessionFixture()) {
            var name = "bulk-unbound-" + UUID.randomUUID();
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new AccessionGrpcService(f.saves, f.reads, authentication -> null)).build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var stub = DocumentServiceGrpc.newStub(channel);
                var saves = new CollectingObserver<SaveDocumentsResponse>();
                stub.saveDocuments(saves).onCompleted();
                saves.await();
                assertStreamFailed(saves, Status.Code.PERMISSION_DENIED);
                assertThat(saves.responses).isEmpty();
                var reads = new CollectingObserver<ReadDocumentsResponse>();
                stub.readDocuments(reads).onCompleted();
                reads.await();
                assertStreamFailed(reads, Status.Code.PERMISSION_DENIED);
                assertThat(reads.responses).isEmpty();
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    // ------------------------------------------------------------------ harness

    private record ServerHarness(Server server, ManagedChannel channel) implements AutoCloseable {
        @Override public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static ServerHarness server(AccessionFixture f) throws java.io.IOException {
        var name = "bulk-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new AccessionGrpcService(f.saves, f.reads, authentication -> new RepositoryCaller(
                        authentication.caller().name(), authentication.caller().unrestricted()))).build().start();
        return new ServerHarness(server, InProcessChannelBuilder.forName(name).directExecutor().build());
    }

    private static SaveDocumentsRequest saveItem(String requestId, SaveDocumentRequest save) {
        return SaveDocumentsRequest.newBuilder().setRequestId(requestId).setSave(save).build();
    }

    private static ReadDocumentsRequest readItem(String requestId, ReadDocumentRequest read) {
        return ReadDocumentsRequest.newBuilder().setRequestId(requestId).setRead(read).build();
    }

    private static void assertStreamFailed(CollectingObserver<?> collector, Status.Code code) {
        assertThat(collector.error).as("the stream failed").isNotNull();
        assertThat(collector.error).isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }

    /** Collects responses until the stream ends; directExecutor makes every emission inline. */
    private static final class CollectingObserver<T> implements StreamObserver<T> {
        final List<T> responses = new ArrayList<>();
        volatile Throwable error;
        private final CountDownLatch done = new CountDownLatch(1);

        @Override public void onNext(T value) { responses.add(value); }
        @Override public void onError(Throwable failure) { error = failure; done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }

        void await() throws InterruptedException {
            assertThat(done.await(30, TimeUnit.SECONDS)).as("the stream ended").isTrue();
        }
    }
}
