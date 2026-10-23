package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import ai.protomolt.proto.projection.SourceResolver;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersionSummary;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.RevisionSummary;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * The archive view of v2 reads: one projection per view message, compiled from
 * the {@code (sources)} and {@code (from)} options on the archive {@code .proto},
 * and a validate.v1 check of every projected message before it is returned.
 *
 * <p>The stored revision passed v2 validation, so a projection that fails or
 * produces a view its own rules refuse is {@code DATA_LOSS} naming the message,
 * field and rule: the view must not invent a different truth. Three fields are
 * host-filled, each for the reason its {@code .proto} comment names:
 * {@code EntryInfo.metadata}, {@code VersionManifest.total_bytes} and, in
 * {@code metadata_snapshot}, the same metadata map.</p>
 */
public final class ArchiveViews {

    private final MessageProjection manifests;
    private final MessageProjection entries;
    private final MessageProjection summaries;

    /** Compiles the view projections; fails fast when a view rule compiles against no source. */
    public ArchiveViews() {
        SourceResolver sources = SourceResolver.of(
                GetDocumentResponse.getDescriptor(),
                DocumentSummary.getDescriptor(),
                RevisionSummary.getDescriptor(),
                Blob.getDescriptor(),
                PartManifestEntry.getDescriptor(),
                NodeAddress.getDescriptor());
        this.manifests = compile(VersionManifest.getDescriptor(), sources);
        this.entries = compile(EntryInfo.getDescriptor(), sources);
        this.summaries = compile(VersionSummary.getDescriptor(), sources);
    }

    private static MessageProjection compile(Descriptor target, SourceResolver sources) {
        return MessageProjection.forTarget(target, sources).orElseThrow(() -> new IllegalStateException(
                target.getFullName() + " declares no projection sources"));
    }

    /**
     * The manifest of the revision {@code read} returned: its raw blobs in stored
     * order, then its PARSED part when present, with {@code metadata_snapshot}
     * projected from the same revision's CORE.
     *
     * @throws io.grpc.StatusRuntimeException {@code FAILED_PRECONDITION} when the
     *         document's BLOBS part uses the singular {@code blob} arm, which the
     *         archive never writes; {@code DATA_LOSS} when the view cannot be built
     *         or breaks its own rules
     */
    public VersionManifest manifest(GetDocumentResponse read) {
        if (read.getDocument().getBlobBag().hasBlob()) {
            throw ArchiveErrors.failedPrecondition("Document " + read.getManifest().getAddress().getDocId()
                    + " stores its BLOBS part in the singular blob_bag.blob arm; it was not written by the"
                    + " archive and is not an archive entry");
        }
        VersionManifest.Builder manifest = project(manifests, read, VersionManifest.parser()).toBuilder();
        long total = 0;
        for (RenditionManifestEntry row : manifest.getRenditionsList()) {
            if (row.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                total = Math.addExact(total, row.getSizeBytes());
            }
        }
        manifest.setTotalBytes(total);
        if (manifest.hasMetadataSnapshot()) {
            manifest.getMetadataSnapshotBuilder()
                    .putAllMetadata(read.getDocument().getSearchMetadata().getMetadataMap());
        }
        return validated(manifest.build());
    }

    /** The entry metadata of a document read: the revision's CORE and its "original" blob's filename. */
    public EntryInfo entryInfo(GetDocumentResponse read) {
        EntryInfo info = project(entries, read, EntryInfo.parser()).toBuilder()
                .putAllMetadata(read.getDocument().getSearchMetadata().getMetadataMap())
                .build();
        return validated(info);
    }

    /** The entry metadata of a listing row, through its {@code DocumentLookup}. Listing rows carry no filename. */
    public EntryInfo entryInfo(DocumentSummary row) {
        EntryInfo info = project(entries, row, EntryInfo.parser()).toBuilder()
                .putAllMetadata(row.getLookup().getMetadataMap())
                .build();
        return validated(info);
    }

    /** One {@code ListVersions} row from one revision's history summary. */
    public VersionSummary summary(RevisionSummary revision) {
        return validated(project(summaries, revision, VersionSummary.parser()));
    }

    private static <T extends Message> T project(MessageProjection projection, Message source, Parser<T> parser) {
        try {
            return parser.parseFrom(projection.project(source).toByteString());
        } catch (ProjectionException | InvalidProtocolBufferException e) {
            throw ArchiveErrors.dataLoss("Cannot project " + source.getDescriptorForType().getFullName()
                    + " into " + projection.targetType().getFullName() + ": " + e.getMessage(), e);
        }
    }

    private static <T extends Message> T validated(T view) {
        DocumentArchive.requireValidView(view);
        return view;
    }
}
