package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.MeshDigest;
import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryPayloadStoreTest {

    @TempDir
    Path temporary;

    @Test
    void repositoryBytesAndProtobufLeaseLedgerSurviveAdapterRestart() throws Exception {
        FakeRepository repository = new FakeRepository();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(repository).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        Path ledger = temporary.resolve("payload-ledger.wal");
        byte[] bytes = RawInput.newBuilder().setText("repository payload").build().toByteArray();
        String lease = UUID.randomUUID().toString();
        ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity identity;
        try {
            try (var store = new RepositoryPayloadStore(
                    DocumentServiceGrpc.newBlockingStub(channel), "mesh", "payloads",
                    ledger, clock)) {
                identity = store.put(new PayloadStore.Put("scope/with/slash", "profile",
                        RawInput.getDescriptor().getFullName(),
                        EntityEnvelopes.schemaOf(RawInput.getDefaultInstance())
                                .getDescriptorFingerprint(), bytes, MeshDigest.sha256(bytes)))
                        .getIdentity();
                store.acquire(identity, "run-owner", lease,
                        ProcessorChannelFixtures.NOW.plusSeconds(60));
                assertThat(repository.keys()).singleElement()
                        .asString().doesNotContain("scope/with/slash");
            }

            try (var restored = new RepositoryPayloadStore(
                    DocumentServiceGrpc.newBlockingStub(channel), "mesh", "payloads",
                    ledger, clock)) {
                assertThat(restored.head(identity).getActiveLeases()).isEqualTo(1);
                assertThat(restored.get(identity, 0, bytes.length)).isEqualTo(bytes);
                restored.release(identity, "run-owner", lease);
                restored.markEligible(identity, ProcessorChannelFixtures.NOW, "", "");
                assertThat(restored.purge(identity, "retention elapsed",
                        ProcessorChannelFixtures.NOW).getPurged()).isTrue();
            }
            assertThat(repository.keys()).isEmpty();
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class FakeRepository
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private final Map<String, ByteString> blobs = new LinkedHashMap<>();

        @Override
        public synchronized void putBlob(
                PutBlobRequest request, StreamObserver<PutBlobResponse> response) {
            blobs.put(request.getObjectKey(), request.getData());
            response.onNext(PutBlobResponse.newBuilder()
                    .setStorageRef(reference(request.getDriveName(), request.getObjectKey()))
                    .setSizeBytes(request.getData().size())
                    .setSha256(MeshDigest.sha256(request.getData().toByteArray()))
                    .build());
            response.onCompleted();
        }

        @Override
        public synchronized void getBlob(
                GetBlobRequest request, StreamObserver<GetBlobResponse> response) {
            ByteString bytes = blobs.get(request.getStorageRef().getObjectKey());
            if (bytes == null) {
                response.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            response.onNext(GetBlobResponse.newBuilder()
                    .setData(bytes).setSizeBytes(bytes.size())
                    .setMimeType("application/x-protobuf").build());
            response.onCompleted();
        }

        @Override
        public synchronized void deleteBlob(
                DeleteBlobRequest request, StreamObserver<DeleteBlobResponse> response) {
            blobs.remove(request.getStorageRef().getObjectKey());
            response.onNext(DeleteBlobResponse.getDefaultInstance());
            response.onCompleted();
        }

        private synchronized java.util.Set<String> keys() {
            return java.util.Set.copyOf(blobs.keySet());
        }

        private static FileStorageReference reference(String drive, String key) {
            return FileStorageReference.newBuilder()
                    .setDriveName(drive).setObjectKey(key).build();
        }
    }
}
