package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

/** Strict replay of every supported root in one exact fragment; no revision authorization. */
final class DocumentFragmentSchemaReplay {
    record Limits(DocumentAnyRootInventory.Limits fragment, DocumentPayloadCheck.Limits payload,
            DocumentSchemaOccurrences.Limits occurrences, DocumentSchemaReplay.Limits replay,
            long maxEvidenceBytes) {
        Limits {
            java.util.Objects.requireNonNull(fragment); java.util.Objects.requireNonNull(payload);
            java.util.Objects.requireNonNull(occurrences); java.util.Objects.requireNonNull(replay);
            if (maxEvidenceBytes < 1) throw new IllegalArgumentException("positive fragment evidence byte limit required");
        }
    }
    record CheckedRoot(DocumentSchemaRootLocator root, DocumentPayloadCheck.AssetResult checked) {}
    private DocumentFragmentSchemaReplay() {}

    /**
     * The host supplies authorized immutable slot/bytes and recorded metadata. Persisted
     * evidence must pass its canonical codec before entry. Inventory parses the fragment
     * once, before any schema resolution. No partial list escapes if a later root fails.
     * The payload byte limit is shared across all roots and nested Any decodes. Other
     * payload/evidence limits apply per root, with root count bounded by fragment limits.
     * This is not opaque materialization or coverage of unsupported document Any locations.
     * A historical reader must classify inconsistency in authenticated retained inputs
     * as data loss; this primitive also serves candidate checks, where mismatch is rejection.
     * Keep the retained reader scoped to this authorization attempt, including after failure.
     */
    static List<CheckedRoot> check(DocumentSchemaBinding container, DocumentPublicationSlot expectedSlot,
            ByteString fragment, String expectedDocumentId, List<DocumentRootSchemaEvidence> bundles,
            Map<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset> metadata,
            DocumentRetainedSchemaAssets retained, ProtoValidator validator, Limits limits, Runnable control)
            throws InvalidProtocolBufferException {
        active(control);
        if (bundles.size() > limits.fragment().maxRoots())
            throw new IllegalArgumentException("fragment evidence root count exceeds limit");
        var byRoot = new HashMap<DocumentSchemaRootLocator, DocumentRootSchemaEvidence>();
        long evidenceBytes = 0;
        for (var bundle : bundles) {
            active(control);
            if (bundle.getSerializedSize() > limits.maxEvidenceBytes() - evidenceBytes)
                throw new IllegalArgumentException("aggregate fragment evidence bytes exceed limit");
            var canonical = DocumentRootSchemaEvidenceCodec.encode(bundle, () -> active(control));
            evidenceBytes += canonical.bytes().size();
            if (!bundle.getRoot().getSlot().equals(expectedSlot))
                throw new IllegalArgumentException("evidence slot differs from selected revision fragment");
            if (byRoot.putIfAbsent(bundle.getRoot(), bundle) != null)
                throw new IllegalArgumentException("duplicate fragment root evidence");
        }
        var inventory = DocumentAnyRootInventory.inspect(container, expectedSlot, fragment, expectedDocumentId,
                limits.fragment(), () -> active(control));
        if (inventory.roots().size() != byRoot.size())
            throw new IllegalArgumentException("evidence does not cover every discovered fragment root");
        // Complete locator matching precedes registry-independent descriptor loading.
        var ordered = new ArrayList<DocumentRootSchemaEvidence>();
        for (var root : inventory.roots()) {
            var locator = DocumentSchemaRootProjection.project(inventory, root, () -> active(control));
            var bundle = byRoot.get(locator);
            if (bundle == null) throw new IllegalArgumentException("evidence root differs from exact fragment inventory");
            ordered.add(bundle);
        }
        var results = new ArrayList<CheckedRoot>();
        long remaining = limits.payload().maxBytes();
        for (int i = 0; i < ordered.size(); i++) {
            active(control);
            if (remaining < 1) throw new IllegalArgumentException("aggregate fragment decoded bytes exceed limit");
            var perRoot = new DocumentPayloadCheck.Limits((int) remaining, limits.payload().maxWireValues(),
                    limits.payload().maxDepth(), limits.payload().maxSchemaTypes(), limits.payload().maxSchemaFields());
            var bundle = ordered.get(i);
            var checked = DocumentSchemaReplay.check(inventory.roots().get(i).envelope(), bundle.getOccurrencesList(),
                    metadata, retained, validator, perRoot, limits.occurrences(), limits.replay(), () -> active(control));
            remaining -= checked.payload().decodedBytes();
            results.add(new CheckedRoot(bundle.getRoot(), checked));
        }
        active(control);
        return List.copyOf(results);
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("fragment schema replay interrupted");
        control.run();
    }
}
