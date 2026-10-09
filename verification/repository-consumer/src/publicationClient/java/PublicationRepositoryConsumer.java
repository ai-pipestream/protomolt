import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.publication.grpc.RemoteDocumentPublicationRepository;
import ai.protomolt.proto.repo.spi.DocumentPublicationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationServiceGrpc;
import ai.protomolt.proto.repo.v1.PublishDocumentRequest;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;
import java.time.Duration;

/** Compile the complete publication client surface with only its published artifact. */
public final class PublicationRepositoryConsumer {
    public static DocumentPublicationRepository connect(RepositoryCaller identity,
            DocumentPublicationServiceGrpc.DocumentPublicationServiceFutureStub authenticatedStub) {
        return new RemoteDocumentPublicationRepository(identity,authenticatedStub,
                new PayloadBudget(32L*1024*1024),Duration.ofSeconds(30),4);
    }

    public static PublishDocumentResponse publish(DocumentPublicationRepository repository, RepositoryCaller identity,
            PublishDocumentRequest request, RepositoryReadControl control) {
        return repository.publishDocument(identity,request,control);
    }
}
