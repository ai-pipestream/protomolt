package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryResponse;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.util.function.Supplier;

/**
 * {@code ArchiveService} over v2 documents: the archive-definition, stats,
 * classification, bridging, and entry reads and writes of
 * {@link DocumentArchive}, {@link RenditionReads}, and {@link RenditionUploads}.
 */
public final class DocumentArchiveGrpcService extends ArchiveServiceGrpc.ArchiveServiceImplBase {

    private final DocumentArchive archive;
    private final RenditionReads reads;
    private final RenditionUploads uploads;

    /**
     * Constructs a {@link DocumentArchiveGrpcService} with
     * {@link DocumentArchive#DEFAULT_MAX_UNARY_GET_BYTES} and {@link BridgeEngine#standard()}.
     *
     * @param documents the channel to the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     */
    public DocumentArchiveGrpcService(Channel documents, ArchiveBindings bindings, Clock clock) {
        this(documents, bindings, clock, DocumentArchive.DEFAULT_MAX_UNARY_GET_BYTES, BridgeEngine.standard());
    }

    /**
     * Constructs a {@link DocumentArchiveGrpcService} with an explicit {@link BridgeEngine}
     * and {@link DocumentArchive#DEFAULT_MAX_UNARY_GET_BYTES}.
     *
     * @param documents the channel to the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     * @param bridgeEngine the bridge engine used by {@code BridgeEntry}
     */
    public DocumentArchiveGrpcService(
            Channel documents, ArchiveBindings bindings, Clock clock, BridgeEngine bridgeEngine) {
        this(documents, bindings, clock, DocumentArchive.DEFAULT_MAX_UNARY_GET_BYTES, bridgeEngine);
    }

    /**
     * Constructs a {@link DocumentArchiveGrpcService} with an explicit unary {@code GetEntry}
     * byte limit and {@link BridgeEngine#standard()}.
     *
     * @param documents the channel to the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     * @param maxUnaryGetBytes maximum total rendition bytes assembled in one unary {@code GetEntry}
     */
    public DocumentArchiveGrpcService(
            Channel documents, ArchiveBindings bindings, Clock clock, long maxUnaryGetBytes) {
        this(documents, bindings, clock, maxUnaryGetBytes, BridgeEngine.standard());
    }

    /**
     * Constructs a {@link DocumentArchiveGrpcService} with an explicit unary {@code GetEntry}
     * byte limit and {@link BridgeEngine}.
     *
     * @param documents the channel to the v2 document service the archive is stored through
     * @param bindings the archive definitions
     * @param clock stamps {@code created_at} on an entry's creating save
     * @param maxUnaryGetBytes maximum total rendition bytes assembled in one unary {@code GetEntry}
     * @param bridgeEngine the bridge engine used by {@code BridgeEntry}
     */
    public DocumentArchiveGrpcService(
            Channel documents,
            ArchiveBindings bindings,
            Clock clock,
            long maxUnaryGetBytes,
            BridgeEngine bridgeEngine) {
        this.archive = new DocumentArchive(documents, bindings, clock, maxUnaryGetBytes, bridgeEngine);
        this.reads = new RenditionReads(archive);
        this.uploads = new RenditionUploads(archive, documents);
    }

    /**
     * Returns the underlying {@link DocumentArchive} engine backing this gRPC service.
     *
     * @return the {@link DocumentArchive} instance
     */
    public DocumentArchive archive() {
        return archive;
    }

    @Override
    public void putEntry(PutEntryRequest request, StreamObserver<PutEntryResponse> responses) {
        unary(() -> archive.putEntry(request), responses);
    }

    @Override
    public StreamObserver<UploadRenditionRequest> uploadRendition(StreamObserver<UploadRenditionResponse> responses) {
        return uploads.upload(responses);
    }

    @Override
    public void getEntry(GetEntryRequest request, StreamObserver<GetEntryResponse> responses) {
        unary(() -> archive.getEntry(request), responses);
    }

    @Override
    public void readRendition(ReadRenditionRequest request, StreamObserver<ReadRenditionResponse> responses) {
        reads.read(request, responses);
    }

    @Override
    public void getEntryManifest(GetEntryManifestRequest request, StreamObserver<GetEntryManifestResponse> responses) {
        unary(() -> archive.getEntryManifest(request), responses);
    }

    @Override
    public void listEntries(ListEntriesRequest request, StreamObserver<ListEntriesResponse> responses) {
        unary(() -> archive.listEntries(request), responses);
    }

    @Override
    public void listVersions(ListVersionsRequest request, StreamObserver<ListVersionsResponse> responses) {
        unary(() -> archive.listVersions(request), responses);
    }

    @Override
    public void createArchive(CreateArchiveRequest request, StreamObserver<CreateArchiveResponse> responses) {
        unary(() -> archive.createArchive(request), responses);
    }

    @Override
    public void getArchive(GetArchiveRequest request, StreamObserver<GetArchiveResponse> responses) {
        unary(() -> archive.getArchive(request), responses);
    }

    @Override
    public void listArchives(ListArchivesRequest request, StreamObserver<ListArchivesResponse> responses) {
        unary(() -> archive.listArchives(request), responses);
    }

    @Override
    public void getArchiveStats(GetArchiveStatsRequest request, StreamObserver<GetArchiveStatsResponse> responses) {
        unary(() -> archive.getArchiveStats(request), responses);
    }

    @Override
    public void classifyEntry(ClassifyEntryRequest request, StreamObserver<ClassifyEntryResponse> responses) {
        unary(() -> archive.classifyEntry(request), responses);
    }

    @Override
    public void bridgeEntry(BridgeEntryRequest request, StreamObserver<BridgeEntryResponse> responses) {
        unary(() -> archive.bridgeEntry(request), responses);
    }

    private static <T> void unary(Supplier<T> call, StreamObserver<T> responses) {
        T response;
        try {
            response = call.get();
        } catch (StatusRuntimeException failure) {
            responses.onError(failure);
            return;
        }
        responses.onNext(response);
        responses.onCompleted();
    }
}
