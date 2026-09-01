package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.delegation.EncryptedRepositoryStateCodec;
import ai.pipestream.proto.delegation.RepositoryStateKeyResolver;
import ai.pipestream.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.pipestream.proto.mesh.cluster.v1.DirectoryCheckpoint;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
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

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RepositoryServiceClusterEventRepositoryTest {

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
    void missingEventLogLoadsAsEmpty() {
        assertThat(repository().load(ClusterFixtures.cluster())).isEmpty();
    }

    @Test
    void roundTripsCiphertextWithoutExposingAdvertisementMetadata() throws Exception {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("private-node-name"));

        repository().save(ClusterFixtures.cluster(),
                ClusterEventRepository.StoredDirectory.of(source.events()));

        assertThat(service.lastPut.getDriveName()).isEqualTo("protomolt");
        assertThat(service.lastPut.getObjectKey())
                .isEqualTo("cluster/cluster-a/events.pb.enc");
        assertThat(service.lastPut.getMimeType())
                .isEqualTo(EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE);
        assertThat(service.stored.toStringUtf8()).doesNotContain("private-node-name");
        EncryptedRepositoryState envelope = EncryptedRepositoryState.parseFrom(service.stored);
        assertThat(envelope.getContentType())
                .isEqualTo(RepositoryServiceClusterEventRepository.CONTENT_TYPE);
        assertThat(repository().load(ClusterFixtures.cluster()))
                .get()
                .satisfies(loaded -> {
                    assertThat(loaded.events()).isEqualTo(source.events());
                    assertThat(loaded.compacted()).isFalse();
                    assertThat(loaded.firstSeq()).isEqualTo(1);
                });
    }

    @Test
    void roundTripsACompactedDirectoryAndItsFencingTombstones() {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("node-1"));
        source.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        DirectoryCheckpoint checkpoint = source.checkpoint();

        repository().save(ClusterFixtures.cluster(),
                new ClusterEventRepository.StoredDirectory(checkpoint, List.of()));

        assertThat(repository().load(ClusterFixtures.cluster()))
                .get()
                .satisfies(loaded -> {
                    assertThat(loaded.compacted()).isTrue();
                    assertThat(loaded.checkpoint()).isEqualTo(checkpoint);
                    assertThat(loaded.events()).isEmpty();
                    assertThat(loaded.firstSeq())
                            .isEqualTo(checkpoint.getState().getSnapshotSeq() + 1);
                });
        assertThat(service.stored.toStringUtf8()).doesNotContain("node-1");
    }

    @Test
    void rejectsCiphertextTamperingAndClusterSubstitution() throws Exception {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("node-1"));
        repository().save(ClusterFixtures.cluster(),
                ClusterEventRepository.StoredDirectory.of(source.events()));
        ByteString original = service.stored;
        EncryptedRepositoryState envelope = EncryptedRepositoryState.parseFrom(original);
        byte[] ciphertext = envelope.getCiphertext().toByteArray();
        ciphertext[0] ^= 1;
        service.stored = envelope.toBuilder()
                .setCiphertext(ByteString.copyFrom(ciphertext)).build().toByteString();

        assertThatThrownBy(() -> repository().load(ClusterFixtures.cluster()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("authentication failed");

        service.stored = original;
        var otherBuilder = ClusterFixtures.clusterBuilder().setClusterId("other-cluster");
        var other = otherBuilder.setFingerprint(
                ClusterValidation.descriptorFingerprint(otherBuilder.build())).build();
        assertThatThrownBy(() -> repository().load(other))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aRepositoryThatNeverAnswersFailsLoudlyInsteadOfStalling() {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("node-1"));
        service.stall = true;
        RepositoryServiceClusterEventRepository repository =
                repository(Duration.ofMillis(250));

        assertThatThrownBy(() -> repository.save(ClusterFixtures.cluster(),
                ClusterEventRepository.StoredDirectory.of(source.events())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("within the deadline")
                .hasMessageContaining("refuses to report membership it cannot persist");
        assertThatThrownBy(() -> repository.load(ClusterFixtures.cluster()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("within the deadline");
    }

    @Test
    void aTimeoutMustBePositive() {
        assertThatThrownBy(() -> repository(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout must be positive");
    }

    private RepositoryServiceClusterEventRepository repository() {
        return new RepositoryServiceClusterEventRepository(
                DocumentServiceGrpc.newBlockingStub(channel), "protomolt",
                "cluster/cluster-a/events.pb.enc", "env:PROTOMOLT_STATE_KEY", keys());
    }

    private RepositoryServiceClusterEventRepository repository(Duration timeout) {
        return new RepositoryServiceClusterEventRepository(
                DocumentServiceGrpc.newBlockingStub(channel), "protomolt",
                "cluster/cluster-a/events.pb.enc", "env:PROTOMOLT_STATE_KEY",
                new EncryptedRepositoryStateCodec(keys()), timeout);
    }

    private static RepositoryStateKeyResolver keys() {
        return ignored -> new SecretKeySpec(
                "0123456789abcdef0123456789abcdef"
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII), "AES");
    }

    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private ByteString stored;
        private PutBlobRequest lastPut;
        /** Accepts the call and never answers it, the way a wedged repository behaves. */
        private boolean stall;

        @Override
        public void putBlob(PutBlobRequest request,
                            StreamObserver<PutBlobResponse> observer) {
            if (stall) {
                return;
            }
            lastPut = request;
            stored = request.getData();
            observer.onNext(PutBlobResponse.newBuilder()
                    .setStorageRef(FileStorageReference.newBuilder()
                            .setDriveName(request.getDriveName())
                            .setObjectKey(request.getObjectKey()))
                    .setSizeBytes(stored.size())
                    .setSha256(EncryptedRepositoryStateCodec.sha256Hex(stored.toByteArray()))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getBlob(GetBlobRequest request,
                            StreamObserver<GetBlobResponse> observer) {
            if (stall) {
                return;
            }
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            observer.onNext(GetBlobResponse.newBuilder()
                    .setData(stored)
                    .setSizeBytes(stored.size())
                    .setMimeType(EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE)
                    .build());
            observer.onCompleted();
        }
    }
}
