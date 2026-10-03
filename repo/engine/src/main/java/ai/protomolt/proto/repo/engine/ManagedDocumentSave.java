package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Full-save composition after authorization and locked dedupe; borrowed writer owns staging/publication. */
final class ManagedDocumentSave {
    private ManagedDocumentSave() {}

    static DocumentRecord full(DocumentAttemptWriter writer, String generation, RepositoryCaller caller,
            SaveResolution.Resolved resolved, SaveDocumentRequest request, DriveRecord drive, UUID node,
            String basePrefix, List<PartObject> payloads, DocumentRecord existing, long version,
            ManagedRawBindings.Plan bindings, JdbcEventOutbox events, RepositoryOperationControl control) {
        UUID attempt = UUID.randomUUID();
        String prefix = basePrefix + "/attempts/" + attempt + "/";
        var planned = payloads.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length, p.sha256(),
                DocumentOperations.PART_CONTENT_TYPE)).toList();
        var plan = new DocumentPartAttemptLedger.Plan(attempt,
                new DocumentPartAttemptLedger.Location(node, resolved.address().getAccountId(), generation, drive.bucket),
                existing == null ? 0 : existing.mutationRevision, Map.of(), planned);
        return writer.write(plan, resolved.address(), drive, payloads, Duration.ofMinutes(1), SaveResolution.s3Metadata(resolved),
                verified -> {
                    var manifest = DocumentManifest.newBuilder().setAddress(resolved.address()).setDocVersion(version);
                    var now = DocumentRequests.timestampNow();
                    var present = java.util.EnumSet.noneOf(DocumentPart.class);
                    for (var part : verified) {
                        present.add(part.part());
                        var entry = PartManifestEntry.newBuilder().setPart(part.part()).setSubKey(part.subKey())
                                .setState(PartState.PART_STATE_PRESENT).setObjectKey(part.key()).setSizeBytes(part.size())
                                .setSha256(part.sha256()).setUpdatedAt(now);
                        if (request.hasWrittenBy()) entry.setWrittenBy(request.getWrittenBy());
                        manifest.addParts(entry);
                    }
                    for (var part : List.of(DocumentPart.DOCUMENT_PART_BLOBS, DocumentPart.DOCUMENT_PART_CHUNKS,
                            DocumentPart.DOCUMENT_PART_PARSED)) {
                        if (present.contains(part)) continue;
                        var entry = PartManifestEntry.newBuilder().setPart(part).setState(PartState.PART_STATE_EMPTY).setUpdatedAt(now);
                        if (request.hasWrittenBy()) entry.setWrittenBy(request.getWrittenBy());
                        manifest.addParts(entry);
                    }
                    var complete = manifest.build();
                    var core = verified.stream().filter(p -> p.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
                    return DocumentSaveCandidate.build(caller, resolved, request, drive, node, basePrefix, complete,
                            DocumentPartCodec.rootChecksumFromManifest(complete), verified.stream().mapToLong(DocumentPublicationLedger.Part::size).sum(),
                            core.etag(), core.providerVersion(), existing);
                }, control::check, (em, saved) -> {
                    control.check();
                    bindings.publish(em, saved);
                    if (events != null) events.enqueue(em, DocumentEventFactory.saved(saved, saved.updatedAt));
                    control.check();
                });
    }
}
