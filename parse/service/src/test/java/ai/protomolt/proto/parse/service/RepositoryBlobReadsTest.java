package ai.protomolt.proto.parse.service;

import static org.assertj.core.api.Assertions.*;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RepositoryBlobReadsTest {
    private static final ByteString PAYLOAD = ByteString.copyFromUtf8("abc");
    private static final String SHA = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("a").setDocId("d")
            .setGraphId("g").setGraphAddressId("n").build();
    private static ReadBlobResponse header(long revision, String sha) {
        return ReadBlobResponse.newBuilder().setHeader(BlobHeader.newBuilder().setRevision(revision)
                .setTotalLength(3).setSha256(sha)).build();
    }
    private byte[] read(List<ReadBlobResponse> frames, Status failure, AtomicReference<ReadBlobRequest> request) throws Exception {
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor().addService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void readBlob(ReadBlobRequest r, StreamObserver<ReadBlobResponse> out) {
                request.set(r);
                frames.forEach(out::onNext);
                if (failure == null) out.onCompleted(); else out.onError(failure.asRuntimeException());
            }
        }).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            return RepositoryBlobReads.read(DocumentServiceGrpc.newBlockingStub(channel), ADDRESS, 7,
                    Blob.newBuilder().setBlobId("b").setSizeBytes(3).build());
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }
    @Test void pinsTheAddressRevisionAndBlobAndVerifiesTheWholeBody() throws Exception {
        var request = new AtomicReference<ReadBlobRequest>();
        assertThat(read(List.of(header(7, SHA), ReadBlobResponse.newBuilder().setData(PAYLOAD.substring(0,1)).build(),
                ReadBlobResponse.newBuilder().setData(PAYLOAD.substring(1)).build()), null, request))
                .isEqualTo(PAYLOAD.toByteArray());
        assertThat(request.get().getAddress()).isEqualTo(ADDRESS);
        assertThat(request.get().getRevision().getRevision()).isEqualTo(7);
        assertThat(request.get().getBlobId()).isEqualTo("b");
    }
    @Test void refusesMalformedTruncatedCorruptAndWrongRevisionStreams() {
        var data = ReadBlobResponse.newBuilder().setData(PAYLOAD).build();
        for (var frames : List.of(List.<ReadBlobResponse>of(), List.of(data), List.of(header(7,SHA)),
                List.of(header(8,SHA),data), List.of(header(7,"0".repeat(64)),data),
                List.of(header(7,SHA),data,data), List.of(header(7,SHA),header(7,SHA)))) {
            assertThatThrownBy(() -> read(frames,null,new AtomicReference<>()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
        }
    }
    @Test void completeBodyFollowedByTransportFailureIsNotSuccess() {
        assertThatThrownBy(() -> read(List.of(header(7,SHA),ReadBlobResponse.newBuilder().setData(PAYLOAD).build()),
                Status.UNAVAILABLE,new AtomicReference<>())).isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
    }
}
