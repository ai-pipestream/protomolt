package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.intake.v1.IngestDocumentRequest;
import ai.protomolt.proto.intake.v1.IngestDocumentResponse;
import ai.protomolt.proto.intake.v1.IngestMetadata;
import ai.protomolt.proto.intake.v1.IngestStreamRequest;
import ai.protomolt.proto.intake.v1.IngestStreamResponse;
import ai.protomolt.proto.intake.v1.IntakeServiceGrpc;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.DocIdDerivation;
import ai.protomolt.proto.repo.v1.DocIdDerivationMethod;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

/**
 * The only authenticated way in: ingest into repo-service.
 *
 * <p>Both gRPC lanes land here. The handler narrows every request within the
 * call's {@link IntakeScope} (established by
 * {@link ApiKeyServerInterceptor}), wraps raw payloads into a repository
 * {@link Document}, and saves through the {@code DocumentService} wire
 * contract — intake never touches object storage itself. The wrap-and-save
 * core is {@link RawIngests}, shared with the HTTP lane
 * ({@link IntakeHttpServer}).
 *
 * <p>Save shape (the intake arm of repo's save contract): {@code
 * use_datasource_id=true}, {@code graph_id = "intake:" + account_id}, the
 * document owned by the scope's account and the request's datasource. The
 * scope's ownership always wins over caller-supplied ownership fields.
 *
 * <p>Receipt integrity: {@code sha256} is computed by intake over the raw
 * payload bytes on the raw and streaming lanes, and over the canonical
 * serialized {@link Document} on the typed-document lane (where the document
 * itself is the payload).
 */
public final class IntakeGrpcService extends IntakeServiceGrpc.IntakeServiceImplBase {

    /** The connector identity intake stamps on documents it wraps itself. */
    public static final String CONNECTOR_ID = "intake";

    /** The contract's default drive when a request names none. */
    public static final String DEFAULT_DRIVE = "intake";

    /** Graph-id prefix of the intake save arm ({@code "intake:" + account_id}). */
    public static final String INTAKE_GRAPH_PREFIX = "intake:";

    private final RawIngests ingests;

    /**
     * @param documents the repo-service document stub every save goes through
     * @param maxPayloadBytes the service-wide payload cap for the in-memory
     *        gRPC lanes; must be positive (bulk payloads beyond it belong on
     *        the HTTP lane)
     */
    public IntakeGrpcService(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents, long maxPayloadBytes) {
        this.ingests = new RawIngests(documents, maxPayloadBytes);
    }

    @Override
    public void ingestDocument(
            IngestDocumentRequest request, StreamObserver<IngestDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> ingest(request));
    }

    @Override
    public StreamObserver<IngestStreamRequest> ingestStream(
            StreamObserver<IngestStreamResponse> observer) {
        IntakeScope scope = ApiKeyServerInterceptor.currentScope();
        return new IngestStreamHandler(scope, observer);
    }

    private IngestDocumentResponse ingest(IngestDocumentRequest request) {
        IntakeScope scope = ApiKeyServerInterceptor.currentScope();
        RawIngests.Targeting targeting = RawIngests.Targeting.of(scope, request.getDatasourceId(),
                request.hasDrive() ? request.getDrive() : null);
        RawIngests.Saved saved =
                switch (request.getContentCase()) {
                    case DOCUMENT -> saveTypedDocument(
                            scope, targeting, request.getDocument(), request.getMetadataMap());
                    case RAW -> ingests.saveRawPayload(
                            scope,
                            targeting,
                            request.getRaw().getData(),
                            request.getRaw().getFilename(),
                            request.getRaw().getMimeType(),
                            request.getMetadataMap());
                    case CONTENT_NOT_SET -> throw GrpcErrors.invalidArgument(
                            "content is required: set document or raw");
                };
        return IngestDocumentResponse.newBuilder()
                .setDocId(saved.response().getAddress().getDocId())
                .setNodeId(saved.response().getNodeId())
                .setAddress(saved.response().getAddress())
                .setDrive(saved.response().getDrive())
                .setSizeBytes(saved.payloadSize())
                .setSha256(saved.sha256())
                .setDeduplicated(saved.response().getDeduplicated())
                .build();
    }

    private RawIngests.Saved saveTypedDocument(
            IntakeScope scope,
            RawIngests.Targeting targeting,
            Document caller,
            Map<String, String> metadata) {
        long inlineBytes = totalInlineBlobBytes(caller);
        ingests.requirePayloadWithinCaps(scope, inlineBytes);
        requireBlobMimeTypesWithinScope(scope, caller);

        Document.Builder document = caller.toBuilder();
        OwnershipContext.Builder ownership =
                caller.hasOwnership() ? caller.getOwnership().toBuilder() : OwnershipContext.newBuilder();
        // The key's scope dictates ownership; whatever the caller supplied loses.
        ownership.setAccountId(scope.accountId()).setDatasourceId(targeting.datasourceId());
        document.setOwnership(ownership);
        if (!caller.getDocId().isBlank() && !caller.hasDocIdDerivation()) {
            document.setDocIdDerivation(
                    DocIdDerivation.newBuilder()
                            .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CALLER_PROVIDED));
        }
        Document toSave = document.build();
        byte[] canonical = toSave.toByteArray();
        return ingests.save(
                scope, targeting, toSave, metadata, RawIngests.sha256Hex(canonical), canonical.length);
    }

    private static void requireBlobMimeTypesWithinScope(IntakeScope scope, Document document) {
        if (scope.mimeTypes().isEmpty()) {
            return;
        }
        for (Blob blob : blobsOf(document)) {
            String mime = blob.hasMimeType() ? blob.getMimeType() : null;
            if (!scope.allowsMimeType(mime)) {
                throw GrpcErrors.permissionDenied(
                        "blob mime_type '" + (mime == null ? "" : mime)
                                + "' is outside the API key's content-type restrictions");
            }
        }
    }

    private static Iterable<Blob> blobsOf(Document document) {
        BlobBag bag = document.getBlobBag();
        return switch (bag.getBlobDataCase()) {
            case BLOB -> List.of(bag.getBlob());
            case BLOBS -> bag.getBlobs().getBlobList();
            case BLOBDATA_NOT_SET -> List.of();
        };
    }

    private static long totalInlineBlobBytes(Document document) {
        long total = 0;
        for (Blob blob : blobsOf(document)) {
            if (blob.getContentCase() == Blob.ContentCase.DATA) {
                total += blob.getData().size();
            }
        }
        return total;
    }

    /**
     * The streaming lane. Enforces the frame discipline — exactly one leading
     * metadata frame, then data frames — and assembles the payload with the
     * caps checked as frames arrive, not after the memory is spent.
     */
    private final class IngestStreamHandler implements StreamObserver<IngestStreamRequest> {

        private final IntakeScope scope;
        private final StreamObserver<IngestStreamResponse> observer;
        private final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        private IngestMetadata metadata;
        private RawIngests.Targeting targeting;
        private boolean failed;

        private IngestStreamHandler(IntakeScope scope, StreamObserver<IngestStreamResponse> observer) {
            this.scope = scope;
            this.observer = observer;
        }

        @Override
        public void onNext(IngestStreamRequest frame) {
            if (failed) {
                return;
            }
            try {
                switch (frame.getFrameCase()) {
                    case METADATA -> acceptMetadata(frame.getMetadata());
                    case DATA -> acceptData(frame.getData());
                    case FRAME_NOT_SET -> throw GrpcErrors.invalidArgument(
                            "frame is required: set metadata or data");
                }
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void acceptMetadata(IngestMetadata first) {
            if (metadata != null) {
                throw GrpcErrors.invalidArgument(
                        "metadata frame after the first frame; only the first frame carries metadata");
            }
            metadata = first;
            targeting = RawIngests.Targeting.of(scope, first.getDatasourceId(),
                    first.hasDrive() ? first.getDrive() : null);
            RawIngests.requireDeclaredMimeTypeWithinScope(scope, first.getMimeType());
        }

        private void acceptData(ByteString data) {
            if (metadata == null) {
                throw GrpcErrors.invalidArgument(
                        "the first frame must carry metadata, not data");
            }
            ingests.requirePayloadWithinCaps(scope, (long) payload.size() + data.size());
            payload.writeBytes(data.toByteArray());
        }

        @Override
        public void onError(Throwable t) {
            // The client cancelled or the transport failed; nothing was saved.
            failed = true;
        }

        @Override
        public void onCompleted() {
            if (failed) {
                return;
            }
            try {
                if (metadata == null) {
                    throw GrpcErrors.invalidArgument(
                            "the stream closed without a metadata frame");
                }
                RawIngests.Saved saved = ingests.saveRawPayload(
                        scope,
                        targeting,
                        ByteString.copyFrom(payload.toByteArray()),
                        metadata.getFilename(),
                        metadata.getMimeType(),
                        metadata.getMetadataMap());
                observer.onNext(
                        IngestStreamResponse.newBuilder()
                                .setDocId(saved.response().getAddress().getDocId())
                                .setNodeId(saved.response().getNodeId())
                                .setAddress(saved.response().getAddress())
                                .setDrive(saved.response().getDrive())
                                .setSizeBytes(saved.payloadSize())
                                .setSha256(saved.sha256())
                                .setDeduplicated(saved.response().getDeduplicated())
                                .build());
                observer.onCompleted();
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void fail(Throwable t) {
            failed = true;
            observer.onError(GrpcErrors.map(t));
        }
    }
}
