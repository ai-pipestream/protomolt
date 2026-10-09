package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.*;
import com.google.protobuf.*;
import java.lang.reflect.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Contract rejection through real storage and retained descriptor validation. */
final class BoundedDocumentRejectionProbe {
    private final AtomicInteger puts=new AtomicInteger(),reads=new AtomicInteger();
    BlobStore wrap(BlobStore actual) {
        return (BlobStore)Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},
                (proxy,method,args) -> {
                    final Object result;
                    try { result=method.invoke(actual,args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                    if (method.getName().equals("put")) puts.incrementAndGet();
                    if (method.getName().equals("getBounded")) reads.incrementAndGet();
                    return result;
                });
    }
    static Descriptors.Descriptor constrainedType() throws Exception {
        var proto=StringValue.getDescriptor().getFile().toProto().toBuilder()
                .addDependency(ValidateProto.getDescriptor().getName());
        for (var type:proto.getMessageTypeBuilderList()) if (type.getName().equals("StringValue"))
            type.setOptions(type.getOptions().toBuilder().setExtension(ValidateProto.message,
                    MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("bounded-fixture-value")
                            .setExpression("this != 'contract-invalid'")).build()));
        return Descriptors.FileDescriptor.buildFrom(proto.build(),new Descriptors.FileDescriptor[]{ValidateProto.getDescriptor()})
                .findMessageTypeByName("StringValue");
    }
    PublishDocumentResponse reject(RepoServices host,Tx tx,RepositoryCaller caller,PublishDocumentRequest request) throws Exception {
        DocumentPublicationInput.validate(request,RepositoryReadControl.NONE);
        int beforePuts=puts.get(),beforeReads=reads.get();
        var response=host.publicationRepository().publishDocument(caller,request,RepositoryReadControl.NONE);
        require(response.hasRejected(),"contract-invalid candidate has durable rejection");
        var rejection=response.getRejected();
        require(rejection.getReason()==DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_ADMISSION_REJECTED
                && rejection.hasAssessment(),"runtime admission rejection retains assessment identity");
        UUID.fromString(rejection.getAssessment().getAssessmentId());
        DocumentPublicationResponseValidator.requireValid(new DocumentPublicationCommand(request.getIntent()),caller.principalName(),response);
        requireRuleEvidence(tx,request,response);
        require(puts.get()>beforePuts && reads.get()>beforeReads,"rejection exercised real Redis upload and read-back");
        requireNoCurrent(host,tx,request);
        requireOneRejection(tx,request);
        return response;
    }
    void replay(RepoServices host,Tx tx,RepositoryCaller caller,PublishDocumentRequest request,PublishDocumentResponse expected)
            throws Exception {
        int beforePuts=puts.get(),beforeReads=reads.get();
        require(host.publicationRepository().publishDocument(caller,request,RepositoryReadControl.NONE).equals(expected),
                "local rejection replay after resolver close");
        String name="bounded-rejection-"+UUID.randomUUID();
        host.startInProcess(name,"bounded-rejection-token",null);
        var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var headers=new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token",io.grpc.Metadata.ASCII_STRING_MARSHALLER),"bounded-rejection-token");
            var client=DocumentPublicationServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
            require(client.withDeadlineAfter(10,TimeUnit.SECONDS).publishDocument(request).equals(expected),
                    "authenticated rejection replay after resolver close");
        } finally {
            channel.shutdownNow();
            require(channel.awaitTermination(10,TimeUnit.SECONDS),"rejection channel stopped");
        }
        require(puts.get()==beforePuts && reads.get()==beforeReads,"replay performs no provider upload or read");
        requireNoCurrent(host,tx,request);
        requireOneRejection(tx,request);
        System.out.println("BOUNDED_DOCUMENT_TYPED_REJECTION_OK");
    }
    private static void requireNoCurrent(RepoServices host,Tx tx,PublishDocumentRequest request) {
        var row=host.documentLedger().findByReference(request.getIntent().getMembers(0).getDestination().getAddress());
        if (row.isPresent()) {
            long count=tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_current WHERE node_id=:node")
                    .setParameter("node",row.orElseThrow().nodeId).getSingleResult()).longValue());
            require(count==0,"rejected candidate has no current revision");
        }
    }
    private static void requireOneRejection(Tx tx,PublishDocumentRequest request) {
        long count=tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:operation")
                .setParameter("operation",UUID.fromString(request.getIntent().getOperationId())).getSingleResult()).longValue());
        require(count==1,"one durable rejection across replays");
        for (String table:java.util.List.of("repository_operation_success","document_revision_commits")) {
            long accepted=tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:operation")
                    .setParameter("operation",UUID.fromString(request.getIntent().getOperationId())).getSingleResult()).longValue());
            require(accepted==0,"rejected operation has no success or revision commits");
        }
    }
    private static void requireRuleEvidence(Tx tx,PublishDocumentRequest request,PublishDocumentResponse response) throws Exception {
        var binding=response.getRejected().getAssessment();
        var row=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT manifest_bytes,manifest_sha256,sealed FROM document_assessment_owners WHERE assessment_id=:assessment
                """).setParameter("assessment",UUID.fromString(binding.getAssessmentId())).getSingleResult());
        require(Boolean.TRUE.equals(row[2]),"retained assessment sealed");
        String digest=java.util.HexFormat.of().formatHex((byte[])row[1]);
        require(digest.equals(binding.getManifestSha256()),"receipt binds exact retained manifest");
        var budget=new ai.protomolt.proto.repo.blob.spi.PayloadBudget(64L*1024*1024);
        var manifest=ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec.decode(
                binding.getManifestCodec(),binding.getManifestEncodingVersion(),ByteString.copyFrom((byte[])row[0]),digest,
                bytes -> { var lease=budget.reserve(bytes); return lease::close; },() -> {});
        var failure=manifest.getFirstFailure();
        var member=request.getIntent().getMembers(0);
        require(failure.getRuleId().equals("bounded-fixture-value") && failure.getMemberId().equals(member.getMemberId()),
                "the intended CEL rule rejected this member");
        int ordinal=failure.getRoot().getOrdinal();
        require(ordinal>=0 && ordinal<member.getPartsCount() && member.getParts(ordinal).getSlot().getPart()==DocumentPart.DOCUMENT_PART_CORE,
                "failure refers to the CORE typed root");
        require(manifest.getMembersList().stream().anyMatch(evidence -> evidence.getMemberId().equals(member.getMemberId())
                && evidence.hasTyped() && evidence.getTyped().getRootsList().contains(failure.getRoot())),
                "failure binds a retained typed root");
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
