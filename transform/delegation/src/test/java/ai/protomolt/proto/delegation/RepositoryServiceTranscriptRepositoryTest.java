package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
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

        assertThat(service.lastPut.getDriveName()).isEqualTo(DRIVE);
        assertThat(service.lastPut.getObjectKey()).isEqualTo(OBJECT_KEY);
        assertThat(service.lastPut.getMimeType())
                .isEqualTo(RepositoryServiceTranscriptRepository.MIME_TYPE);
        assertThat(service.lastPut.getData().toStringUtf8())
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
        service.wrongWriteDigest = true;

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
        assertThat(service.lastPut).isNull();
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

    private RepositoryServiceTranscriptRepository repository() {
        return repository(keyResolver(), 1024 * 1024);
    }

    private RepositoryServiceTranscriptRepository repository(
            RepositoryStateKeyResolver resolver, int maxBytes) {
        return new RepositoryServiceTranscriptRepository(
                DocumentServiceGrpc.newBlockingStub(channel), DRIVE, OBJECT_KEY,
                KEY_REF, resolver,
                Clock.fixed(Instant.parse("2026-08-11T12:00:00Z"), ZoneOffset.UTC),
                new SecureRandom(), maxBytes);
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

    private static String sha256(ByteString data) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(data.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private ByteString stored;
        private PutBlobRequest lastPut;
        private boolean wrongWriteDigest;
        private long readSizeDelta;

        @Override
        public void putBlob(PutBlobRequest request,
                            StreamObserver<PutBlobResponse> observer) {
            lastPut = request;
            stored = request.getData();
            observer.onNext(PutBlobResponse.newBuilder()
                    .setStorageRef(FileStorageReference.newBuilder()
                            .setDriveName(request.getDriveName())
                            .setObjectKey(request.getObjectKey()))
                    .setSizeBytes(stored.size())
                    .setSha256(wrongWriteDigest ? "0".repeat(64) : sha256(stored))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getBlob(GetBlobRequest request,
                            StreamObserver<GetBlobResponse> observer) {
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            observer.onNext(GetBlobResponse.newBuilder()
                    .setData(stored)
                    .setSizeBytes(stored.size() + readSizeDelta)
                    .setMimeType(RepositoryServiceTranscriptRepository.MIME_TYPE)
                    .build());
            observer.onCompleted();
        }
    }
}
