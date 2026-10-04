package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Exact candidate checks for the forthcoming atomic managed publication boundary. */
final class DocumentPartPublication {
    private DocumentPartPublication() {}

    record VerifiedPart(int ordinal, DocumentPart part, String subKey, String key,
            long size, String sha256, String version, String etag) {}

    /**
     * Called after locking the attempt and document revisions in the publication
     * transaction. It performs no I/O and does not publish or grant authorization.
     * Provider versions for every part remain in the attempt ledger; the existing
     * manifest contract has no per-part provider identity fields.
     */
    static void validate(DocumentRecord candidate, List<VerifiedPart> parts) {
        if (!DocumentStatus.AVAILABLE.equals(candidate.status) || candidate.pendingPurgeId != null)
            refuse("New document body must be available without a pending purge");
        if (candidate.partManifest == null) refuse("Document publication requires a manifest");
        var parsed = DocumentManifest.newBuilder();
        try {
            // Managed publication must not silently discard unknown persisted fields.
            com.google.protobuf.util.JsonFormat.parser().merge(candidate.partManifest, parsed);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new DocumentPartAttemptLedger.FenceException("Document publication manifest is invalid", malformed);
        }
        validate(candidate, parts, parsed.build());
    }

    /** Reuses a strictly parsed stored manifest without another JSON allocation under locks. */
    static void validate(DocumentRecord candidate, List<VerifiedPart> parts, DocumentManifest manifest) {
        if (!DocumentStatus.AVAILABLE.equals(candidate.status) || candidate.pendingPurgeId != null)
            refuse("New document body must be available without a pending purge");
        Objects.requireNonNull(manifest, "manifest");
        if (!manifest.hasAddress() || manifest.getDocVersion() <= 0)
            refuse("Document publication requires an addressed, versioned manifest");
        var address = manifest.getAddress();
        if (!Objects.equals(candidate.docId, address.getDocId())
                || !Objects.equals(candidate.accountId, address.getAccountId())
                || !Objects.equals(candidate.graphId, address.getGraphId())
                || !Objects.equals(candidate.graphAddressId, address.getGraphAddressId()))
            refuse("Manifest address differs from document identity");
        var slots = new HashSet<Map.Entry<DocumentPart, String>>();
        int present = 0;
        long size = 0;
        for (var entry : manifest.getPartsList()) {
            if (entry.getPart() == DocumentPart.UNRECOGNIZED || entry.getPart() == DocumentPart.DOCUMENT_PART_UNSPECIFIED
                    || entry.getState() == PartState.UNRECOGNIZED || entry.getState() == PartState.PART_STATE_UNSPECIFIED
                    || entry.getSizeBytes() < 0
                    || (entry.getPart() != DocumentPart.DOCUMENT_PART_CHUNKS && !entry.getSubKey().isEmpty())
                    || !slots.add(Map.entry(entry.getPart(), entry.getSubKey())))
                refuse("Invalid or duplicate manifest slot");
            if (entry.getState() == PartState.PART_STATE_EMPTY) {
                if (!entry.getObjectKey().isEmpty() || !entry.getSha256().isEmpty() || entry.getSizeBytes() != 0
                        || !entry.getDeletedReason().isEmpty())
                    refuse("Empty manifest slot claims stored content");
                continue;
            }
            if (entry.getState() == PartState.PART_STATE_DELETED) {
                if (entry.getPart() == DocumentPart.DOCUMENT_PART_CORE || entry.getDeletedReason().isBlank())
                    refuse("Invalid deleted manifest slot");
                continue; // Historical tombstones are not live byte references.
            }
            if (!entry.getDeletedReason().isEmpty() || present >= parts.size())
                refuse("Manifest contains an unplanned PRESENT object");
            var expected = parts.get(present);
            if (expected.ordinal() != present || !matches(expected, entry))
                refuse("Manifest PRESENT object differs from ordered verified plan");
            if (expected.part() == DocumentPart.DOCUMENT_PART_CORE
                    && (!Objects.equals(candidate.versionId, expected.version())
                    || !Objects.equals(candidate.etag, expected.etag() == null ? "" : expected.etag())))
                refuse("Document CORE provider identity differs from verified plan");
            try { size = Math.addExact(size, expected.size()); }
            catch (ArithmeticException overflow) { refuse("Document part size exceeds supported range"); }
            present++;
        }
        if (present != parts.size()) refuse("Manifest omits verified objects");
        if (!Objects.equals(candidate.sizeBytes, size)) refuse("Document size differs from verified plan");
        if (!Objects.equals(candidate.checksum, DocumentPartCodec.rootChecksumFromManifest(manifest)))
            refuse("Document checksum differs from verified plan");
    }

    private static boolean matches(VerifiedPart expected, PartManifestEntry actual) {
        return expected.part() == actual.getPart() && expected.subKey().equals(actual.getSubKey())
                && expected.key().equals(actual.getObjectKey()) && expected.size() == actual.getSizeBytes()
                && expected.sha256().equals(actual.getSha256());
    }
    private static void refuse(String message) { throw new DocumentPartAttemptLedger.FenceException(message); }
}
