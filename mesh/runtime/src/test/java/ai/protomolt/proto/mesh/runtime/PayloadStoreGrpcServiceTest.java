package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.v1.GetPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.HeadPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.MarkPayloadEligibleRequest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseRequest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadStoreServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.PurgePayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadHeader;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadStoreGrpcServiceTest {

    @Test
    void protobufRpcStreamsPutRangeGetLeasesEligibilityAndPurge() throws Exception {
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        InMemoryPayloadStore store = new InMemoryPayloadStore(clock, 10, 1_000_000);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new PayloadStoreGrpcService(store, clock)).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            byte[] body = RawInput.newBuilder().setText("streamed payload").build()
                    .toByteArray();
            AtomicReference<PutPayloadResponse> put = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch complete = new CountDownLatch(1);
            StreamObserver<PutPayloadRequest> requests = PayloadStoreServiceGrpc
                    .newStub(channel).putPayload(new StreamObserver<>() {
                        @Override
                        public void onNext(PutPayloadResponse response) {
                            put.set(response);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            failure.set(throwable);
                            complete.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            complete.countDown();
                        }
                    });
            requests.onNext(PutPayloadRequest.newBuilder()
                    .setHeader(PutPayloadHeader.newBuilder()
                            .setNamespace("scope-a")
                            .setProfile("payload-test")
                            .setPayloadTypeName(RawInput.getDescriptor().getFullName())
                            .setDescriptorFingerprint(EntityEnvelopes
                                    .schemaOf(RawInput.getDefaultInstance())
                                    .getDescriptorFingerprint())
                            .setExpectedSizeBytes(body.length)
                            .setExpectedSha256(ai.protomolt.proto.mesh.MeshDigest.sha256(body)))
                    .build());
            requests.onNext(PutPayloadRequest.newBuilder()
                    .setData(ByteString.copyFrom(body)).build());
            requests.onCompleted();
            assertThat(complete.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();

            var identity = put.get().getMetadata().getIdentity();
            var blocking = PayloadStoreServiceGrpc.newBlockingStub(channel);
            assertThat(blocking.headPayload(HeadPayloadRequest.newBuilder()
                    .setIdentity(identity).build()).getIdentity()).isEqualTo(identity);

            ByteArrayOutputStream fetched = new ByteArrayOutputStream();
            blocking.getPayload(GetPayloadRequest.newBuilder()
                            .setIdentity(identity).setLength(body.length).build())
                    .forEachRemaining(chunk -> fetched.writeBytes(chunk.getData().toByteArray()));
            assertThat(fetched.toByteArray()).isEqualTo(body);

            String lease = UUID.randomUUID().toString();
            PayloadLeaseRequest leaseRequest = PayloadLeaseRequest.newBuilder()
                    .setIdentity(identity).setOwnerId("test-owner").setLeaseId(lease)
                    .setExpiresAt(RemoteValidation.timestamp(
                            ProcessorChannelFixtures.NOW.plusSeconds(60)))
                    .build();
            assertThat(blocking.acquireLease(leaseRequest).getMetadata().getActiveLeases())
                    .isEqualTo(1);
            blocking.markEligibleForDeletion(MarkPayloadEligibleRequest.newBuilder()
                    .setIdentity(identity)
                    .setNotBefore(RemoteValidation.timestamp(ProcessorChannelFixtures.NOW))
                    .build());
            blocking.releaseLease(leaseRequest);
            assertThat(blocking.purgePayload(PurgePayloadRequest.newBuilder()
                    .setIdentity(identity).setReason("test cleanup").build()).getPurged())
                    .isTrue();
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
