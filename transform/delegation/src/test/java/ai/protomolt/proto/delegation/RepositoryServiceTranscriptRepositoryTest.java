package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RepositoryServiceTranscriptRepositoryTest {

    private static final String DRIVE = "protomolt";
    private static final String OBJECT_KEY = "delegation/workspace-a/transcript.pb.enc";
    private static final String KEY_REF = "env:PROTOMOLT_TRANSCRIPT_KEY";
    private static final byte[] KEY = "0123456789abcdef0123456789abcdef"
            .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final Context.Key<String> TEST_CONTEXT = Context.key("repository-test-context");

    private FakeDocumentService service;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void startServer() throws Exception {
        service = new FakeDocumentService();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterEach
    void stopServer() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void missingObjectLoadsAsEmpty() {
        assertThat(repository().load()).isEmpty();
    }

    @Test
    void roundTripsEncryptedTranscriptThroughStableRepositoryCoordinates() {
        Transcript transcript = acceptedTranscript("private objective marker");

        repository().save(transcript);

        assertThat(service.lastCompare.getKey().getDriveName()).isEqualTo(DRIVE);
        assertThat(service.lastCompare.getKey().getObjectKey()).isEqualTo(OBJECT_KEY);
        assertThat(service.lastCompare.getMimeType())
                .isEqualTo(RepositoryServiceTranscriptRepository.MIME_TYPE);
        assertThat(service.lastCompare.getData().toStringUtf8())
                .doesNotContain("private objective marker");
        EncryptedRepositoryState envelope = parseEnvelope();
        assertThat(envelope.getKeyRef()).isEqualTo(KEY_REF);
        assertThat(envelope.getNonce()).hasSize(12);
        assertThat(repository().load()).contains(transcript);
    }

    /** A typed deliverable is transcript state like any other frame field. */
    @Test
    void roundTripsACandidateCarryingItsTypedDeliverable() {
        Transcript transcript = deliverableTranscript();

        repository().save(transcript);

        assertThat(repository().load()).contains(transcript);
    }

    @Test
    void rejectsRepositoryWriteWithoutExactIntegrityConfirmation() {
        service.writeFault = WriteFault.WRONG_DIGEST;

        assertThatThrownBy(() -> repository().save(acceptedTranscript("objective")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
    }

    @Test
    void rejectsRepositoryReadWithWrongSizeMetadata() {
        repository().save(acceptedTranscript("objective"));
        service.readSizeDelta = 1;

        assertThatThrownBy(() -> repository().load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("size");
    }

    @Test
    void rejectsAuthenticatedCiphertextTampering() throws Exception {
        repository().save(acceptedTranscript("objective"));
        EncryptedRepositoryState envelope = parseEnvelope();
        byte[] changed = envelope.getCiphertext().toByteArray();
        changed[0] ^= 1;
        service.stored = envelope.toBuilder()
                .setCiphertext(ByteString.copyFrom(changed))
                .build().toByteString();

        assertThatThrownBy(() -> repository().load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("authentication failed");
    }

    @Test
    void rejectsUnknownFieldsInAuthenticatedEnvelopeAndTranscript() {
        Transcript transcript = acceptedTranscript("unknown state fields");
        service.stored = envelopeFor(transcript.toByteArray()).toBuilder()
                .setUnknownFields(unknownFields()).build().toByteString();
        service.currentEtag = "\"seed-envelope\"";
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown");

        Transcript withUnknown = transcript.toBuilder().setUnknownFields(unknownFields()).build();
        service.stored = envelopeFor(withUnknown.toByteArray()).toByteString();
        service.currentEtag = "\"seed-transcript\"";
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void rejectsWrongKeyWithoutEchoingReferenceOrKeyMaterial() {
        repository().save(acceptedTranscript("objective"));
        RepositoryStateKeyResolver wrong = ignored -> new SecretKeySpec(
                "abcdef0123456789abcdef0123456789"
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII), "AES");
        RepositoryServiceTranscriptRepository reader = repository(wrong, 1024 * 1024);

        assertThatThrownBy(reader::load)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageNotContaining(KEY_REF)
                .hasMessageNotContaining("abcdef0123456789");
    }

    @Test
    void enforcesPlaintextLimitBeforeCallingRepositoryService() {
        RepositoryServiceTranscriptRepository limited = repository(keyResolver(), 1);

        assertThatThrownBy(() -> limited.save(acceptedTranscript("objective")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext limit");
        assertThat(service.lastCompare).isNull();
    }

    @Test
    void storedInvalidTranscriptCannotBecomeRestartState() throws Exception {
        Transcript valid = acceptedTranscript("objective");
        repository().save(valid);
        EncryptedRepositoryState original = parseEnvelope();
        Transcript invalid = valid.toBuilder().removeEntries(0).build();

        RepositoryServiceTranscriptRepository writer = repository();
        assertThatThrownBy(() -> writer.save(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThat(parseEnvelope()).isEqualTo(original);
    }

    @Test
    void rejectsInvalidRpcTimeouts() {
        var stub = DocumentServiceGrpc.newBlockingStub(channel);

        assertThatThrownBy(() -> new RepositoryServiceTranscriptRepository(
                stub, DRIVE, OBJECT_KEY, KEY_REF, keyResolver(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
        assertThatThrownBy(() -> new RepositoryServiceTranscriptRepository(
                stub, DRIVE, OBJECT_KEY, KEY_REF, keyResolver(),
                Duration.ofHours(1).plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one hour");
        assertThatThrownBy(() -> new RepositoryServiceTranscriptRepository(
                stub, DRIVE, OBJECT_KEY, KEY_REF, keyResolver(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("rpcTimeout");
    }

    @Test
    void repeatedSaveUsesTheConfirmedEtagAndAdvancesTheOpaqueToken() {
        Transcript transcript = acceptedTranscript("objective");
        RepositoryServiceTranscriptRepository writer = repository();

        writer.save(transcript);
        String first = service.currentEtag;
        writer.save(transcript);

        assertThat(service.lastCompare.hasExpectedEtag()).isTrue();
        assertThat(service.lastCompare.getExpectedEtag()).isEqualTo(first);
        assertThat(service.currentEtag).isNotEqualTo(first);
    }

    @Test
    void staleIfAbsentWriteConflictsPoisonsInstanceAndFreshInstanceRecovers() {
        RepositoryServiceTranscriptRepository first = repository();
        RepositoryServiceTranscriptRepository stale = repository();
        assertThat(first.load()).isEmpty();
        assertThat(stale.load()).isEmpty();
        Transcript transcript = acceptedTranscript("objective");

        first.save(transcript);
        assertThatThrownBy(() -> stale.save(transcript))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.ABORTED));
        assertThatThrownBy(stale::load).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
        assertThat(repository().load()).contains(transcript);
    }

    @Test
    void delayedEarlierWriteCannotOverwriteRecoveryInstanceNewerCommit() throws Exception {
        RepositoryServiceTranscriptRepository earlier = repository();
        RepositoryServiceTranscriptRepository recovery = repository();
        assertThat(earlier.load()).isEmpty();
        assertThat(recovery.load()).isEmpty();
        service.pauseNextCompare = true;
        AtomicReference<Throwable> earlierFailure = new AtomicReference<>();
        Thread earlierWriter = new Thread(() -> {
            try {
                earlier.save(acceptedTranscript("earlier snapshot"));
            } catch (Throwable failure) {
                earlierFailure.set(failure);
            }
        }, "delayed-transcript-writer");
        earlierWriter.start();
        assertThat(service.compareEntered.await(5, TimeUnit.SECONDS)).isTrue();
        Transcript recovered = acceptedTranscript("recovery snapshot");
        recovery.save(recovered);
        service.resumeCompare.countDown();
        earlierWriter.join(5_000);

        assertThat(earlierWriter.isAlive()).isFalse();
        assertThat(earlierFailure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) earlierFailure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.ABORTED);
        assertThat(repository().load()).contains(recovered);
    }

    @Test
    void delayedStaleEtagWriteCannotOverwriteRecoveryAppend() throws Exception {
        Transcript prefix = leasedTranscript("shared prefix");
        repository().save(prefix);
        RepositoryServiceTranscriptRepository earlier = repository();
        RepositoryServiceTranscriptRepository recovery = repository();
        assertThat(earlier.load()).contains(prefix);
        assertThat(recovery.load()).contains(prefix);
        service.pauseNextCompare = true;
        Transcript staleExtension = leasedTranscriptBuilder("shared prefix")
                .progress(TASK, WORKER, 1, 1, "older completion").build();
        Transcript recoveredExtension = leasedTranscriptBuilder("shared prefix")
                .progress(TASK, WORKER, 1, 1, "recovered completion")
                .progress(TASK, WORKER, 1, 2, "recovery continued").build();
        AtomicReference<Throwable> staleFailure = new AtomicReference<>();
        Thread delayed = new Thread(() -> {
            try {
                earlier.save(staleExtension);
            } catch (Throwable failure) {
                staleFailure.set(failure);
            }
        }, "delayed-stale-etag-writer");
        delayed.start();
        assertThat(service.compareEntered.await(5, TimeUnit.SECONDS)).isTrue();
        recovery.save(recoveredExtension);
        service.resumeCompare.countDown();
        delayed.join(5_000);

        assertThat(delayed.isAlive()).isFalse();
        assertThat(staleFailure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) staleFailure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.ABORTED);
        assertThatThrownBy(earlier::load).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
        assertThat(repository().load()).contains(recoveredExtension);
    }

    @Test
    void acknowledgementLostAfterCommitFailsClosedAndNewInstanceRecoversCommittedState() {
        service.loseAcknowledgementAfterCommit = true;
        RepositoryServiceTranscriptRepository writer = repository();
        Transcript transcript = acceptedTranscript("durable despite lost ack");

        assertThatThrownBy(() -> writer.save(transcript))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
        assertThatThrownBy(writer::load).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
        assertThatThrownBy(() -> writer.save(transcript)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
        assertThat(repository().load()).contains(transcript);
    }

    @Test
    void callerCancellationAfterCommittedWriteDoesNotCancelRepositoryAcknowledgement() throws Exception {
        RepositoryServiceTranscriptRepository writer = repository(keyResolver(), 1024 * 1024,
                Duration.ofSeconds(10));
        Transcript transcript = acceptedTranscript("caller canceled after durable commit");
        Gate gate = service.blockNextCompareAfterCommit();
        Context.CancellableContext caller = Context.current().withValue(TEST_CONTEXT, "caller-marker")
                .withCancellation();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Transcript> loadedUnderCancelledContext = new AtomicReference<>();
        AtomicReference<Context> attachedContext = new AtomicReference<>();
        AtomicReference<Context> originalContext = new AtomicReference<>();
        AtomicReference<Context> restoredContext = new AtomicReference<>();
        Thread save = new Thread(() -> {
            Context prior = caller.attach();
            originalContext.set(prior);
            attachedContext.set(Context.current());
            try {
                writer.save(transcript);
                assertThat(Context.current()).isSameAs(attachedContext.get());
                assertThat(TEST_CONTEXT.get()).isEqualTo("caller-marker");
                loadedUnderCancelledContext.set(writer.load().orElseThrow());
                assertThat(Context.current()).isSameAs(attachedContext.get());
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                caller.detach(prior);
                restoredContext.set(Context.current());
            }
        }, "cancelled-caller-transcript-write");
        try {
            save.start();
            assertThat(gate.entered().await(5, TimeUnit.SECONDS))
                    .as("repository bytes committed before caller cancellation").isTrue();
            assertThat(parseStoredTranscript()).isEqualTo(transcript);
            caller.cancel(null);
            gate.release().countDown();
            save.join(5_000);

            assertThat(save.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            assertThat(loadedUnderCancelledContext.get()).isEqualTo(transcript);
            assertThat(restoredContext.get()).isSameAs(originalContext.get());
            int writes = service.compareCalls;
            writer.save(transcript);
            assertThat(service.compareCalls).isEqualTo(writes + 1);
        } finally {
            gate.release().countDown();
            caller.cancel(null);
            save.join(5_000);
        }
    }

    @Test
    void repositoryDeadlineStillPoisonsInstanceWhenCommitAcknowledgementIsLate() throws Exception {
        RepositoryServiceTranscriptRepository writer = repository(keyResolver(), 1024 * 1024,
                Duration.ofMillis(500));
        Transcript transcript = acceptedTranscript("repository call exceeded its own deadline");
        Gate gate = service.blockNextCompareAfterCommit();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread save = new Thread(() -> {
            try {
                writer.save(transcript);
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "timed-out-transcript-write");
        try {
            save.start();
            assertThat(gate.entered().await(5, TimeUnit.SECONDS))
                    .as("server committed before repository deadline").isTrue();
            save.join(5_000);
            assertThat(save.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
            assertThat(((StatusRuntimeException) failure.get()).getStatus().getCode())
                    .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
            assertThatThrownBy(writer::load).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("requires recovery");
            assertThatThrownBy(() -> writer.save(transcript)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("requires recovery");
        } finally {
            gate.release().countDown();
            save.join(5_000);
        }
        assertThat(repository().load()).contains(transcript);
    }

    @Test
    void unsupportedConditionalRpcNeverFallsBackToUnconditionalPut() {
        service.conditionalWriteUnimplemented = true;
        RepositoryServiceTranscriptRepository writer = repository();
        assertThat(writer.load()).isEmpty();

        assertThatThrownBy(() -> writer.save(acceptedTranscript("no fallback")))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNIMPLEMENTED));
        assertThat(service.putCalls).isZero();
        assertThatThrownBy(writer::load).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
    }

    @Test
    void divergentAndTruncatedHistoryAreRejectedBeforeConditionalWrite() {
        Transcript confirmed = acceptedTranscript("confirmed history");
        RepositoryServiceTranscriptRepository writer = repository();
        writer.save(confirmed);
        assertThat(writer.load()).contains(confirmed);
        int writes = service.compareCalls;

        assertThatThrownBy(() -> writer.save(acceptedTranscript("divergent history")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("preserve confirmed history");
        assertThatThrownBy(() -> writer.save(confirmed.toBuilder()
                .removeEntries(confirmed.getEntriesCount() - 1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(service.compareCalls).isEqualTo(writes);
    }

    @Test
    void malformedUnknownAndMisboundReadResponsesFailClosed() {
        repository().save(acceptedTranscript("valid base"));

        service.readWrongKey = true;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
        service.readWrongKey = false;

        service.readUnknownField = true;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown repository protocol fields");
        service.readUnknownField = false;

        service.readMalformedEtag = true;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class);
        service.readMalformedEtag = false;

        service.readWrongDigest = true;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
    }

    @Test
    void malformedOrUnverifiableWriteConfirmationPoisonsTheWriter() {
        for (WriteFault fault : WriteFault.values()) {
            if (fault == WriteFault.NONE) continue;
            service.resetWriteFaults();
            RepositoryServiceTranscriptRepository writer = repository();
            service.writeFault = fault;
            assertThatThrownBy(() -> writer.save(acceptedTranscript("write response " + fault)))
                    .as("fault %s", fault).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(writer::load).as("fault %s poisons writer", fault)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires recovery");
        }
    }

    private RepositoryServiceTranscriptRepository repository() {
        return repository(keyResolver(), 1024 * 1024);
    }

    private RepositoryServiceTranscriptRepository repository(
            RepositoryStateKeyResolver resolver, int maxBytes) {
        return repository(resolver, maxBytes, RepositoryServiceTranscriptRepository.DEFAULT_RPC_TIMEOUT);
    }

    private RepositoryServiceTranscriptRepository repository(
            RepositoryStateKeyResolver resolver, int maxBytes, Duration rpcTimeout) {
        if (rpcTimeout.equals(RepositoryServiceTranscriptRepository.DEFAULT_RPC_TIMEOUT)) {
            return new RepositoryServiceTranscriptRepository(
                    DocumentServiceGrpc.newBlockingStub(channel), DRIVE, OBJECT_KEY,
                    KEY_REF, resolver,
                    Clock.fixed(Instant.parse("2026-08-11T12:00:00Z"), ZoneOffset.UTC),
                    new SecureRandom(), maxBytes);
        }
        return new RepositoryServiceTranscriptRepository(
                DocumentServiceGrpc.newBlockingStub(channel), DRIVE, OBJECT_KEY,
                KEY_REF, resolver, rpcTimeout);
    }

    private static RepositoryStateKeyResolver keyResolver() {
        return ignored -> new SecretKeySpec(KEY, "AES");
    }

    private EncryptedRepositoryState parseEnvelope() {
        try {
            return EncryptedRepositoryState.parseFrom(service.stored);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    private Transcript parseStoredTranscript() throws Exception {
        return Transcript.parseFrom(new EncryptedRepositoryStateCodec(keyResolver(),
                Clock.fixed(Instant.parse("2026-08-11T12:00:00Z"), ZoneOffset.UTC),
                new SecureRandom(), 1024 * 1024).decrypt(parseEnvelope(),
                RepositoryServiceTranscriptRepository.CONTENT_TYPE, DRIVE + "\n" + OBJECT_KEY));
    }

    private record Gate(CountDownLatch entered, CountDownLatch release) {}

    private static EncryptedRepositoryState envelopeFor(byte[] plaintext) {
        return new EncryptedRepositoryStateCodec(keyResolver(),
                Clock.fixed(Instant.parse("2026-08-11T12:00:00Z"), ZoneOffset.UTC),
                new SecureRandom(), 1024 * 1024).encrypt(plaintext,
                RepositoryServiceTranscriptRepository.CONTENT_TYPE,
                acceptedTranscript("unused").getEntriesCount(), KEY_REF, DRIVE + "\n" + OBJECT_KEY);
    }

    private static UnknownFieldSet unknownFields() {
        return UnknownFieldSet.newBuilder().addField(123,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static Transcript deliverableTranscript() {
        var taskSpec = spec("build").toBuilder()
                .setContract(DeliverableFixtures.contract())
                .build();
        var candidate = ai.protomolt.proto.delegation.v1.CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("the review report is written")
                .addEvidence(DelegationFixtures.evidence("build"))
                .addCommits(DelegationFixtures.commit("deliverable"))
                .setResult(DeliverableFixtures.result("a headline long enough", 4))
                .build();
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .candidateWith(TASK, WORKER, 1, candidate)
                .accepted(TASK, WORKER, 1, "verified")
                .build();
    }

    private static Transcript acceptedTranscript(String objective) {
        var taskSpec = spec("build").toBuilder().setObjective(objective).build();
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, taskSpec)
                .accepted(TASK, WORKER, 1, "verified")
                .build();
    }

    private static Transcript leasedTranscript(String objective) {
        return leasedTranscriptBuilder(objective).build();
    }

    private static DelegationFixtures.TranscriptBuilder leasedTranscriptBuilder(String objective) {
        var taskSpec = spec("build").toBuilder().setObjective(objective).build();
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1);
    }

    private static String sha256(ByteString data) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(data.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private enum WriteFault {
        NONE, WRONG_DIGEST, WRONG_SIZE, WRONG_KEY, UNKNOWN_FIELD, MISSING_VERSION, MALFORMED_ETAG
    }

    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private volatile ByteString stored;
        private volatile String currentEtag;
        private volatile CompareAndPutBlobRequest lastCompare;
        private volatile WriteFault writeFault = WriteFault.NONE;
        private volatile long readSizeDelta;
        private volatile boolean readWrongKey;
        private volatile boolean readWrongDigest;
        private volatile boolean readUnknownField;
        private volatile boolean readMalformedEtag;
        private volatile boolean conditionalWriteUnimplemented;
        private volatile boolean loseAcknowledgementAfterCommit;
        private volatile boolean pauseNextCompare;
        private final AtomicReference<Gate> afterCommitGate = new AtomicReference<>();
        private volatile int compareCalls;
        private volatile int putCalls;
        private long generation;
        private CountDownLatch compareEntered = new CountDownLatch(1);
        private CountDownLatch resumeCompare = new CountDownLatch(1);

        private Gate blockNextCompareAfterCommit() {
            Gate gate = new Gate(new CountDownLatch(1), new CountDownLatch(1));
            if (!afterCommitGate.compareAndSet(null, gate)) {
                throw new IllegalStateException("post-commit gate is already armed");
            }
            return gate;
        }

        @Override
        public synchronized void getBlobForUpdate(GetBlobForUpdateRequest request,
                                                  StreamObserver<GetBlobForUpdateResponse> observer) {
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            ConditionalBlobKey responseKey = readWrongKey
                    ? request.getKey().toBuilder().setObjectKey("wrong/key").build()
                    : request.getKey();
            String responseEtag = readMalformedEtag ? "*" : currentEtag;
            ConditionalBlobVersion.Builder version = ConditionalBlobVersion.newBuilder()
                    .setKey(responseKey)
                    .setEtag(responseEtag)
                    .setSizeBytes(stored.size() + readSizeDelta)
                    .setSha256(readWrongDigest ? "0".repeat(64) : sha256(stored));
            GetBlobForUpdateResponse.Builder response = GetBlobForUpdateResponse.newBuilder()
                    .setVersion(version)
                    .setData(stored)
                    .setMimeType(RepositoryServiceTranscriptRepository.MIME_TYPE);
            if (readUnknownField) response.setUnknownFields(unknownFields());
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void compareAndPutBlob(CompareAndPutBlobRequest request,
                                      StreamObserver<CompareAndPutBlobResponse> observer) {
            lastCompare = request;
            compareCalls++;
            if (conditionalWriteUnimplemented) {
                observer.onError(Status.UNIMPLEMENTED.asRuntimeException());
                return;
            }
            boolean pause;
            synchronized (this) {
                pause = pauseNextCompare;
                if (pause) {
                    pauseNextCompare = false;
                    compareEntered.countDown();
                }
            }
            if (pause) {
                try {
                    if (!resumeCompare.await(5, TimeUnit.SECONDS)) {
                        observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    observer.onError(Status.CANCELLED.withCause(e).asRuntimeException());
                    return;
                }
            }
            CompareAndPutBlobResponse writeResponse;
            Gate committedGate;
            synchronized (this) {
                boolean mayWrite = request.hasIfAbsent()
                        ? stored == null
                        : stored != null && request.getExpectedEtag().equals(currentEtag);
                if (!mayWrite) {
                    observer.onError(Status.ABORTED.withDescription("conditional write conflict").asRuntimeException());
                    return;
                }

                stored = request.getData();
                currentEtag = "\"etag-" + (++generation) + "\"";
                ConditionalBlobKey responseKey = writeFault == WriteFault.WRONG_KEY
                        ? request.getKey().toBuilder().setObjectKey("wrong/key").build()
                        : request.getKey();
                ConditionalBlobVersion.Builder version = ConditionalBlobVersion.newBuilder()
                        .setKey(responseKey)
                        .setEtag(writeFault == WriteFault.MALFORMED_ETAG ? "*" : currentEtag)
                        .setSizeBytes(stored.size() + (writeFault == WriteFault.WRONG_SIZE ? 1 : 0))
                        .setSha256(writeFault == WriteFault.WRONG_DIGEST ? "0".repeat(64) : sha256(stored));
                CompareAndPutBlobResponse.Builder response = CompareAndPutBlobResponse.newBuilder();
                if (writeFault != WriteFault.MISSING_VERSION) response.setVersion(version);
                if (writeFault == WriteFault.UNKNOWN_FIELD) response.setUnknownFields(unknownFields());
                writeResponse = response.build();
                committedGate = afterCommitGate.getAndSet(null);
            }
            if (committedGate != null) {
                committedGate.entered().countDown();
                Thread acknowledgement = new Thread(() -> {
                    try {
                        if (!committedGate.release().await(5, TimeUnit.SECONDS)) {
                            observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                        } else if (loseAcknowledgementAfterCommit) {
                            observer.onError(Status.UNAVAILABLE.withDescription(
                                    "simulated lost acknowledgement").asRuntimeException());
                        } else {
                            observer.onNext(writeResponse);
                            observer.onCompleted();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        observer.onError(Status.CANCELLED.withCause(e).asRuntimeException());
                    }
                }, "repository-test-delayed-ack");
                acknowledgement.setDaemon(true);
                acknowledgement.start();
                return;
            }
            if (loseAcknowledgementAfterCommit) {
                observer.onError(Status.UNAVAILABLE.withDescription("simulated lost acknowledgement").asRuntimeException());
                return;
            }
            observer.onNext(writeResponse);
            observer.onCompleted();
        }

        @Override
        public void putBlob(ai.protomolt.proto.repo.v1.PutBlobRequest request,
                            StreamObserver<ai.protomolt.proto.repo.v1.PutBlobResponse> observer) {
            putCalls++;
            observer.onError(Status.UNIMPLEMENTED.asRuntimeException());
        }

        private void resetWriteFaults() {
            writeFault = WriteFault.NONE;
            stored = null;
            currentEtag = null;
            generation = 0;
        }

        private static UnknownFieldSet unknownFields() {
            return UnknownFieldSet.newBuilder().addField(123,
                    UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        }
    }
}
