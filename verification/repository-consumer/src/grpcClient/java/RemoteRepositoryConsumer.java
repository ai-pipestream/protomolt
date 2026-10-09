import ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;

/** Compile the public client surface using only published client dependencies. */
public final class RemoteRepositoryConsumer {
    public static BlobStore connect(DocumentServiceGrpc.DocumentServiceBlockingStub stub, String drive) {
        return new RemoteBlobStore(stub, drive);
    }
}
