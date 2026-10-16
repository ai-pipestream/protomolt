package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.repository.EncryptedRepositoryStateCodec;

import ai.protomolt.proto.delegation.repository.RepositoryStateDocuments;

import ai.protomolt.proto.delegation.repository.RepositoryStateKeyResolver;

import ai.protomolt.proto.delegation.repository.RepositoryServiceTranscriptRepository;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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
    void missingDocumentLoadsAsEmpty() {
        assertThat(repository().load()).isEmpty();
    }

    @Test
    void roundTripsEncryptedTranscriptThroughStableRepositoryCoordinates() throws Exception {
        Transcript transcript = acceptedTranscript("private objective marker");

        repository().save(transcript);

        SaveDocumentRequest written = service.lastSave;
        var address = written.getTarget().getAddress();
        assertThat(address.getAccountId()).isEqualTo(DRIVE);
        assertThat(address.getDocId()).isEqualTo(OBJECT_KEY);
        assertThat(address.getGraphAddressId()).isEqualTo(OBJECT_KEY);
        assertThat(address.getGraphId()).isEqualTo("intake:" + DRIVE);
        assertThat(written.getExpected().hasNone()).isTrue();
        assertThat(written.getDocument().getOwnership().getAccountId()).isEqualTo(DRIVE);
        assertThat(written.getDocument().getOwnership().getDatasourceId()).isEqualTo(OBJECT_KEY);
        assertThat(written.getWritesList()).hasSize(1);
        var data = written.getDocument().getStructuredData();
        assertThat(data.getTypeUrl()).isEqualTo(RepositoryStateDocuments.ENVELOPE_TYPE_URL);
        assertThat(data.getValue().toStringUtf8()).doesNotContain("private objective marker");
        EncryptedRepositoryState envelope = EncryptedRepositoryState.parseFrom(data.getValue());
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
        service.writeFault = WriteFault.WRONG_REVISION;

        assertThatThrownBy(() -> repository().save(acceptedTranscript("objective")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
    }

    @Test
    void rejectsRepositoryReadWithAMisboundManifest() {
        repository().save(acceptedTranscript("objective"));
        service.readFault = ReadFault.WRONG_ADDRESS;

        assertThatThrownBy(() -> repository().load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
    }

    @Test
    void rejectsAuthenticatedCiphertextTampering() throws Exception {
        repository().save(acceptedTranscript("objective"));
        EncryptedRepositoryState envelope = parseEnvelope();
        byte[] changed = envelope.getCiphertext().toByteArray();
        changed[0] ^= 1;
        service.stored = stateDocument(envelope.toBuilder()
                .setCiphertext(com.google.protobuf.ByteString.copyFrom(changed))
                .build());

        assertThatThrownBy(() -> repository().load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("authentication failed");
    }

    @Test
    void rejectsUnknownFieldsInAuthenticatedEnvelopeAndTranscript() {
        Transcript transcript = acceptedTranscript("unknown state fields");
        service.seed(stateDocument(envelopeFor(transcript.toByteArray()).toBuilder()
                .setUnknownFields(unknownFields()).build()));
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown");

        Transcript withUnknown = transcript.toBuilder().setUnknownFields(unknownFields()).build();
        service.seed(stateDocument(envelopeFor(withUnknown.toByteArray())));
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
        assertThat(service.lastSave).isNull();
    }

    @Test
    void storedInvalidTranscriptCannotBecomeRestartState() throws Exception {
        Transcript valid = acceptedTranscript("objective");
        repository().save(valid);
        Document original = service.stored;
        Transcript invalid = valid.toBuilder().removeEntries(0).build();

        RepositoryServiceTranscriptRepository writer = repository();
        assertThatThrownBy(() -> writer.save(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThat(service.stored).isEqualTo(original);
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
    void repeatedSaveUsesTheConfirmedRevisionAndAdvancesIt() {
        Transcript transcript = acceptedTranscript("objective");
        RepositoryServiceTranscriptRepository writer = repository();

        writer.save(transcript);
        long first = service.revision;
        writer.save(transcript);

        assertThat(service.lastSave.getExpected().hasRevision()).isTrue();
        assertThat(service.lastSave.getExpected().getRevision()).isEqualTo(first);
        assertThat(service.revision).isEqualTo(first + 1);
    }

    @Test
    void staleCreateWriteConflictsPoisonsInstanceAndFreshInstanceRecovers() {
        RepositoryServiceTranscriptRepository first = repository();
        RepositoryServiceTranscriptRepository stale = repository();
        assertThat(first.load()).isEmpty();
        assertThat(stale.load()).isEmpty();
        Transcript transcript = acceptedTranscript("objective");

        first.save(transcript);
        assertThatThrownBy(() -> stale.save(transcript))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> {
                    assertThat(((StatusRuntimeException) error).getStatus().getCode())
                            .isEqualTo(Status.Code.ABORTED);
                    var status = StatusProto.fromThrowable((StatusRuntimeException) error);
                    assertThat(status).isNotNull();
                    assertThat(status.getDetailsList()).anySatisfy(detail ->
                            assertThat(detail.is(RevisionConflict.class)).isTrue());
                });
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
        service.pauseNextSave = true;
        AtomicReference<Throwable> earlierFailure = new AtomicReference<>();
        Thread earlierWriter = new Thread(() -> {
            try {
                earlier.save(acceptedTranscript("earlier snapshot"));
            } catch (Throwable failure) {
                earlierFailure.set(failure);
            }
        }, "delayed-transcript-writer");
        earlierWriter.start();
        assertThat(service.saveEntered.await(5, TimeUnit.SECONDS)).isTrue();
        Transcript recovered = acceptedTranscript("recovery snapshot");
        recovery.save(recovered);
        service.resumeSave.countDown();
        earlierWriter.join(5_000);

        assertThat(earlierWriter.isAlive()).isFalse();
        assertThat(earlierFailure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) earlierFailure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.ABORTED);
        assertThat(repository().load()).contains(recovered);
    }

    @Test
    void delayedStaleRevisionWriteCannotOverwriteRecoveryAppend() throws Exception {
        Transcript prefix = leasedTranscript("shared prefix");
        repository().save(prefix);
        RepositoryServiceTranscriptRepository earlier = repository();
        RepositoryServiceTranscriptRepository recovery = repository();
        assertThat(earlier.load()).contains(prefix);
        assertThat(recovery.load()).contains(prefix);
        service.pauseNextSave = true;
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
        }, "delayed-stale-revision-writer");
        delayed.start();
        assertThat(service.saveEntered.await(5, TimeUnit.SECONDS)).isTrue();
        recovery.save(recoveredExtension);
        service.resumeSave.countDown();
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
        Gate gate = service.blockNextSaveAfterCommit();
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
            int writes = service.saveCalls;
            writer.save(transcript);
            assertThat(service.saveCalls).isEqualTo(writes + 1);
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
        Gate gate = service.blockNextSaveAfterCommit();
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
    void unsupportedSaveRpcIsNeverRetriedThroughAnotherRpc() {
        service.saveUnimplemented = true;
        RepositoryServiceTranscriptRepository writer = repository();
        assertThat(writer.load()).isEmpty();
        int reads = service.getCalls;

        assertThatThrownBy(() -> writer.save(acceptedTranscript("no fallback")))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNIMPLEMENTED));
        assertThat(service.saveCalls).isEqualTo(1);
        assertThat(service.getCalls).isEqualTo(reads);
        assertThatThrownBy(writer::load).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires recovery");
    }

    @Test
    void divergentAndTruncatedHistoryAreRejectedBeforeConditionalWrite() {
        Transcript confirmed = acceptedTranscript("confirmed history");
        RepositoryServiceTranscriptRepository writer = repository();
        writer.save(confirmed);
        assertThat(writer.load()).contains(confirmed);
        int writes = service.saveCalls;

        assertThatThrownBy(() -> writer.save(acceptedTranscript("divergent history")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("preserve confirmed history");
        assertThatThrownBy(() -> writer.save(confirmed.toBuilder()
                .removeEntries(confirmed.getEntriesCount() - 1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(service.saveCalls).isEqualTo(writes);
    }

    @Test
    void malformedUnknownAndMisboundReadResponsesFailClosed() {
        repository().save(acceptedTranscript("valid base"));

        service.readFault = ReadFault.WRONG_ADDRESS;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
        service.readFault = ReadFault.NONE;

        service.readFault = ReadFault.UNKNOWN_FIELD;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown repository protocol fields");
        service.readFault = ReadFault.NONE;

        service.readFault = ReadFault.ZERO_REVISION;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
        service.readFault = ReadFault.NONE;

        service.readFault = ReadFault.WRONG_DOC_ID;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm");
        service.readFault = ReadFault.NONE;

        service.readFault = ReadFault.WRONG_TYPE_URL;
        assertThatThrownBy(() -> repository().load()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unexpected media type");
        service.readFault = ReadFault.NONE;
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
            return EncryptedRepositoryState.parseFrom(service.stored.getStructuredData().getValue());
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

    private static Document stateDocument(EncryptedRepositoryState envelope) {
        return Document.newBuilder().setDocId(OBJECT_KEY)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(DRIVE).setDatasourceId(OBJECT_KEY))
                .setStructuredData(Any.pack(envelope)).build();
    }

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

    private enum WriteFault {
        NONE, WRONG_REVISION, WRONG_ADDRESS, WRONG_OPERATION, REPLAYED, UNKNOWN_FIELD
    }

    private enum ReadFault {
        NONE, WRONG_ADDRESS, UNKNOWN_FIELD, ZERO_REVISION, WRONG_DOC_ID, WRONG_TYPE_URL
    }

    /**
     * The v2 DocumentService contract over one in-memory document: current-revision
     * reads, and compare-and-set saves that answer ABORTED with a RevisionConflict
     * detail on a stale expectation. Fault hooks misbind or corrupt responses;
     * gates commit first and hold the acknowledgement, or hold the compare-and-set
     * decision, so concurrency and lost-acknowledgement scenarios are deterministic.
     */
    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private volatile Document stored;
        private volatile long revision;
        private volatile SaveDocumentRequest lastSave;
        private volatile WriteFault writeFault = WriteFault.NONE;
        private volatile ReadFault readFault = ReadFault.NONE;
        private volatile boolean saveUnimplemented;
        private volatile boolean loseAcknowledgementAfterCommit;
        private volatile boolean pauseNextSave;
        private final AtomicReference<Gate> afterCommitGate = new AtomicReference<>();
        private volatile int saveCalls;
        private volatile int getCalls;
        private CountDownLatch saveEntered = new CountDownLatch(1);
        private CountDownLatch resumeSave = new CountDownLatch(1);

        private void seed(Document document) {
            stored = document;
            revision = 1;
        }

        private Gate blockNextSaveAfterCommit() {
            Gate gate = new Gate(new CountDownLatch(1), new CountDownLatch(1));
            if (!afterCommitGate.compareAndSet(null, gate)) {
                throw new IllegalStateException("post-commit gate is already armed");
            }
            return gate;
        }

        @Override
        public synchronized void getDocument(GetDocumentRequest request,
                                             StreamObserver<GetDocumentResponse> observer) {
            getCalls++;
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            var address = readFault == ReadFault.WRONG_ADDRESS
                    ? request.getAddress().toBuilder().setDocId("wrong/document").build()
                    : request.getAddress();
            var manifest = DocumentManifest.newBuilder().setAddress(address)
                    .setRevision(readFault == ReadFault.ZERO_REVISION ? 0 : revision)
                    .addParts(PartManifestEntry.newBuilder()
                            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                            .setState(PartState.PART_STATE_PRESENT).setWrittenInRevision(1)
                            .setOwner(SaveOwner.newBuilder().setIntake(
                                    IntakeOrigin.newBuilder().setDatasourceId("seed"))));
            var document = stored;
            if (readFault == ReadFault.WRONG_DOC_ID) {
                document = document.toBuilder().setDocId("wrong/document").build();
            }
            if (readFault == ReadFault.WRONG_TYPE_URL) {
                document = document.toBuilder().setStructuredData(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/wrong.Type")
                        .setValue(document.getStructuredData().getValue())).build();
            }
            var response = GetDocumentResponse.newBuilder().setDocument(document).setManifest(manifest);
            if (readFault == ReadFault.UNKNOWN_FIELD) response.setUnknownFields(unknownFields());
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void saveDocument(SaveDocumentRequest request,
                                 StreamObserver<SaveDocumentResponse> observer) {
            lastSave = request;
            saveCalls++;
            if (saveUnimplemented) {
                observer.onError(Status.UNIMPLEMENTED.asRuntimeException());
                return;
            }
            boolean pause;
            synchronized (this) {
                pause = pauseNextSave;
                if (pause) {
                    pauseNextSave = false;
                    saveEntered.countDown();
                }
            }
            if (pause) {
                try {
                    if (!resumeSave.await(5, TimeUnit.SECONDS)) {
                        observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    observer.onError(Status.CANCELLED.withCause(e).asRuntimeException());
                    return;
                }
            }
            SaveDocumentResponse writeResponse;
            Gate committedGate;
            synchronized (this) {
                long expected = request.getExpected().hasNone() ? 0 : request.getExpected().getRevision();
                if (stored == null ? expected != 0 : revision != expected) {
                    observer.onError(abortedWithConflict(request, revision));
                    return;
                }

                stored = request.getDocument();
                revision++;
                var accession = Accession.newBuilder()
                        .setAddress(writeFault == WriteFault.WRONG_ADDRESS
                                ? request.getTarget().getAddress().toBuilder().setDocId("wrong/document").build()
                                : request.getTarget().getAddress())
                        .setRevision(writeFault == WriteFault.WRONG_REVISION ? revision + 1 : revision)
                        .setPreviousRevision(expected);
                var response = SaveDocumentResponse.newBuilder()
                        .setOperationId(writeFault == WriteFault.WRONG_OPERATION
                                ? "tampered-operation" : request.getOperationId())
                        .setAccession(accession)
                        .setReplayed(writeFault == WriteFault.REPLAYED);
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

        private static StatusRuntimeException abortedWithConflict(SaveDocumentRequest request, long current) {
            var detail = RevisionConflict.newBuilder().setAddress(request.getTarget().getAddress())
                    .setExpected(request.getExpected()).setCurrentRevision(current).build();
            return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.ABORTED.value()).setMessage("conditional write conflict")
                    .addDetails(Any.pack(detail)).build());
        }

        private void resetWriteFaults() {
            writeFault = WriteFault.NONE;
            stored = null;
            revision = 0;
        }

        private static UnknownFieldSet unknownFields() {
            return UnknownFieldSet.newBuilder().addField(123,
                    UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        }
    }
}
