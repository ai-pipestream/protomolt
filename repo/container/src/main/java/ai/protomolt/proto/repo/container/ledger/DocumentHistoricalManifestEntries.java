package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Borrowed historical provenance. Source Uses and transaction authorization remain caller-owned. */
final class DocumentHistoricalManifestEntries {
    private record Retained(DocumentHistoricalReferenceAdmission.Prepared source, PartManifestEntry entry) {}
    private final Map<PublicationHistoricalReuse, Retained> entries;

    private DocumentHistoricalManifestEntries(Map<PublicationHistoricalReuse, Retained> entries) {
        this.entries = Map.copyOf(entries);
    }

    /** Captures exact selected entries outside write locks; never aliases revisions by node/slot. */
    static DocumentHistoricalManifestEntries prepare(DocumentPublicationCommand command,
            List<DocumentHistoricalReferenceAdmission.Prepared> sources, Runnable control) {
        sources = DocumentHistoricalReferenceAdmission.requireComplete(command, sources, control);
        var entries = new HashMap<PublicationHistoricalReuse, Retained>();
        for (var source : sources) {
            var plan = source.plan();
            for (var selector : source.selectors()) {
                control.run();
                if (!selector.getSource().equals(plan.address()) || !selector.getRevisionId().equals(plan.revision().toString())
                        || selector.getRevisionOrdinal() < 0 || selector.getRevisionOrdinal() >= plan.manifest().getPartsCount())
                    throw conflict();
                var entry = plan.manifest().getParts(selector.getRevisionOrdinal());
                var object = selector.getObject();
                if (entry.getState() != PartState.PART_STATE_PRESENT || entry.getPart() != selector.getSourceSlot().getPart()
                        || !entry.getSubKey().equals(selector.getSourceSlot().getSubKey()) || !entry.getObjectKey().equals(object.getObjectKey())
                        || entry.getSizeBytes() != object.getSizeBytes() || !entry.getSha256().equals(object.getSha256())) throw conflict();
                var previous = entries.putIfAbsent(selector, new Retained(source, entry));
                if (previous != null && !previous.entry().equals(entry)) throw conflict();
            }
        }
        sources.forEach(DocumentHistoricalReferenceAdmission.Prepared::plan);
        return new DocumentHistoricalManifestEntries(entries);
    }

    /** Compares the selected declaration to the transaction's physical binding; grants no lock proof. */
    PartManifestEntry select(PublicationHistoricalReuse selector, DocumentCommitParts.Physical physical, Runnable control) {
        control.run();
        var retained = entries.get(selector); var object = selector.getObject();
        if (retained == null) throw conflict();
        retained.source().plan();
        var entry = retained.entry();
        if (physical == null || !UUID.fromString(object.getObjectId()).equals(physical.id())
                || selector.getSourceSlot().getPartValue() != physical.part() || !selector.getSourceSlot().getSubKey().equals(physical.subKey())
                || !object.getObjectKey().equals(physical.key()) || object.getSizeBytes() != physical.size()
                || !object.getSha256().equals(physical.sha256()) || !object.getContentType().equals(physical.contentType())
                || !object.getBackendGeneration().equals(physical.generation()) || !object.getStorageRealm().equals(physical.realm())
                || !object.getNamespace().equals(physical.namespace())
                || !Objects.equals(object.hasProviderVersion() ? object.getProviderVersion() : null, physical.version())) throw conflict();
        control.run();
        retained.source().plan();
        return entry;
    }

    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Historical manifest entry differs from retained physical identity");
    }
}
