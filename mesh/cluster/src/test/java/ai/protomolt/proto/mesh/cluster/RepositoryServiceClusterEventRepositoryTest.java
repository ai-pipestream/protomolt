package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.delegation.repository.EncryptedRepositoryStateCodec;
import ai.protomolt.proto.delegation.repository.RepositoryStateDocuments;
import ai.protomolt.proto.delegation.repository.RepositoryStateKeyResolver;
import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.mesh.cluster.v1.DirectoryCheckpoint;
import ai.protomolt.proto.repo.v1.Document;
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
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
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

    private static final String DRIVE = "protomolt";
    private static final String OBJECT_KEY = "cluster/cluster-a/events.pb.enc";

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

        SaveDocumentRequest written = service.lastSave;
        var address = written.getTarget().getAddress();
        assertThat(address.getAccountId()).isEqualTo(DRIVE);
        assertThat(address.getDocId()).isEqualTo(OBJECT_KEY);
        assertThat(written.getDocument().getOwnership().getAccountId()).isEqualTo(DRIVE);
        assertThat(written.getDocument().getOwnership().getDatasourceId()).isEqualTo(OBJECT_KEY);
        var data = written.getDocument().getStructuredData();
        assertThat(data.getTypeUrl()).isEqualTo(RepositoryStateDocuments.ENVELOPE_TYPE_URL);
        assertThat(data.getValue().toStringUtf8()).doesNotContain("private-node-name");
        EncryptedRepositoryState envelope = EncryptedRepositoryState.parseFrom(data.getValue());
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
        assertThat(service.stored.getStructuredData().getValue().toStringUtf8()).doesNotContain("node-1");
    }

    @Test
    void rejectsCiphertextTamperingAndClusterSubstitution() throws Exception {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("node-1"));
        repository().save(ClusterFixtures.cluster(),
                ClusterEventRepository.StoredDirectory.of(source.events()));
        Document original = service.stored;
        EncryptedRepositoryState envelope = EncryptedRepositoryState.parseFrom(
                original.getStructuredData().getValue());
        byte[] ciphertext = envelope.getCiphertext().toByteArray();
        ciphertext[0] ^= 1;
        service.stored = original.toBuilder().setStructuredData(Any.pack(envelope.toBuilder()
                .setCiphertext(ByteString.copyFrom(ciphertext)).build())).build();

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

    @Test
    void aCompetingWriterSurfacesAConflictAndTheCommittedLogSurvives() {
        var cluster = ClusterFixtures.cluster();
        ClusterDirectory first = new ClusterDirectory(cluster,
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        first.register(ClusterFixtures.node("node-first"));
        ClusterDirectory second = new ClusterDirectory(cluster,
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        second.register(ClusterFixtures.node("node-second"));
        RepositoryServiceClusterEventRepository winner = repository();
        RepositoryServiceClusterEventRepository competitor = repository();
        assertThat(winner.load(cluster)).isEmpty();
        assertThat(competitor.load(cluster)).isEmpty();
        winner.save(cluster, ClusterEventRepository.StoredDirectory.of(first.events()));

        // The competitor's candidate was derived from the same empty baseline; committing
        // it would drop the winner's event, so the conflict must surface instead.
        assertThatThrownBy(() -> competitor.save(cluster,
                ClusterEventRepository.StoredDirectory.of(second.events())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicting write")
                .hasMessageContaining("fresh load");
        assertThat(service.saveCalls).isEqualTo(2);
        assertThat(service.revision).isEqualTo(1);

        // The conflicted instance stays conflicted rather than retrying its stale
        // candidate; only a fresh load recovers.
        assertThatThrownBy(() -> competitor.save(cluster,
                ClusterEventRepository.StoredDirectory.of(second.events())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicting write");
        assertThat(service.saveCalls).isEqualTo(3);

        // The winner's confirmed baseline advanced, so its next save compare-and-sets on
        // revision 1, and the committed log keeps every event it ever wrote.
        first.register(ClusterFixtures.node("node-first-2"));
        winner.save(cluster, ClusterEventRepository.StoredDirectory.of(first.events()));
        assertThat(service.lastSave.getExpected().getRevision()).isEqualTo(1);
        assertThat(service.revision).isEqualTo(2);
        assertThat(repository().load(cluster)).get()
                .satisfies(loaded -> assertThat(loaded.events()).isEqualTo(first.events()));
    }

    @Test
    void aConflictSurfacesWithoutRetryingTheStaleCandidate() {
        ClusterDirectory source = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        source.register(ClusterFixtures.node("node-1"));
        service.alwaysConflict = true;

        assertThatThrownBy(() -> repository().save(ClusterFixtures.cluster(),
                ClusterEventRepository.StoredDirectory.of(source.events())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicting write")
                .satisfies(error -> {
                    var cause = (io.grpc.StatusRuntimeException) error.getCause();
                    assertThat(cause.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
                    var status = StatusProto.fromThrowable(cause);
                    assertThat(status).isNotNull();
                    assertThat(status.getDetailsList()).anySatisfy(detail ->
                            assertThat(detail.is(RevisionConflict.class)).isTrue());
                });
        assertThat(service.saveCalls).isEqualTo(1);
    }

    private RepositoryServiceClusterEventRepository repository() {
        return new RepositoryServiceClusterEventRepository(
                DocumentServiceGrpc.newBlockingStub(channel), DRIVE, OBJECT_KEY,
                "env:PROTOMOLT_STATE_KEY", keys());
    }

    private RepositoryServiceClusterEventRepository repository(Duration timeout) {
        return new RepositoryServiceClusterEventRepository(
                DocumentServiceGrpc.newBlockingStub(channel), DRIVE, OBJECT_KEY,
                "env:PROTOMOLT_STATE_KEY", new EncryptedRepositoryStateCodec(keys()), timeout);
    }

    private static RepositoryStateKeyResolver keys() {
        return ignored -> new SecretKeySpec(
                "0123456789abcdef0123456789abcdef"
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII), "AES");
    }

    /**
     * The v2 DocumentService contract over one in-memory document: current-revision
     * reads and compare-and-set saves answering ABORTED with a RevisionConflict detail
     * on a stale expectation. Fault hooks stall answers or conflict every write.
     */
    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private volatile Document stored;
        private volatile long revision;
        private volatile SaveDocumentRequest lastSave;
        private volatile int saveCalls;
        /** Accepts the call and never answers it, the way a wedged repository behaves. */
        private volatile boolean stall;
        private volatile boolean alwaysConflict;

        @Override
        public synchronized void getDocument(GetDocumentRequest request,
                                             StreamObserver<GetDocumentResponse> observer) {
            if (stall) {
                return;
            }
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            var manifest = DocumentManifest.newBuilder().setAddress(request.getAddress())
                    .setRevision(revision)
                    .addParts(PartManifestEntry.newBuilder()
                            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                            .setState(PartState.PART_STATE_PRESENT).setWrittenInRevision(1)
                            .setOwner(SaveOwner.newBuilder().setIntake(
                                    IntakeOrigin.newBuilder().setDatasourceId("seed"))));
            observer.onNext(GetDocumentResponse.newBuilder().setDocument(stored).setManifest(manifest).build());
            observer.onCompleted();
        }

        @Override
        public synchronized void saveDocument(SaveDocumentRequest request,
                                              StreamObserver<SaveDocumentResponse> observer) {
            lastSave = request;
            saveCalls++;
            if (stall) {
                return;
            }
            long expected = request.getExpected().hasNone() ? 0 : request.getExpected().getRevision();
            if (alwaysConflict || (stored == null ? expected != 0 : revision != expected)) {
                var detail = RevisionConflict.newBuilder().setAddress(request.getTarget().getAddress())
                        .setExpected(request.getExpected()).setCurrentRevision(revision).build();
                observer.onError(StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value()).setMessage("conditional write conflict")
                        .addDetails(Any.pack(detail)).build()));
                return;
            }
            stored = request.getDocument();
            revision++;
            observer.onNext(SaveDocumentResponse.newBuilder().setOperationId(request.getOperationId())
                    .setAccession(Accession.newBuilder().setAddress(request.getTarget().getAddress())
                            .setRevision(revision).setPreviousRevision(expected))
                    .build());
            observer.onCompleted();
        }
    }
}
