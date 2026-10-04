package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Synthetic SQL verification evidence, not a provider adapter or proof of uploaded bytes. */
record ManagedDocumentFixture(DocumentRecord row, UUID attempt,
        List<DocumentPublicationSlot> slots, List<PublicationObjectIdentity> identities) {
    static ManagedDocumentFixture publish(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, DocumentSecurity policy,
            int parts, long coreSize, String version) {
        return publish(tx, drive, generation, profile, address, policy, parts, coreSize, version, false);
    }

    static ManagedDocumentFixture publish(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, DocumentSecurity policy,
            int parts, long coreSize, String version, boolean sparseManifest) {
        UUID node = DocumentIds.nodeId(address); UUID id = UUID.randomUUID();
        var documents = new DocumentLedger(tx);
        var prior = documents.findByNodeId(node).orElse(null);
        String prefix = RepositoryNamespaces.under(drive.prefix, "documents/" + address.getAccountId() + "/" + node + "/attempts/" + id + "/");
        var objects = new ArrayList<DocumentPartAttemptLedger.PlannedObject>();
        var slots = new ArrayList<DocumentPublicationSlot>();
        for (int i = 0; i < parts; i++) {
            var slot = DocumentPublicationSlot.newBuilder().setPart(i == 0 ? DocumentPart.DOCUMENT_PART_CORE : DocumentPart.DOCUMENT_PART_CHUNKS)
                    .setSubKey(i == 0 ? "" : "chunk-" + node + "-" + i).build();
            slots.add(slot);
            objects.add(new DocumentPartAttemptLedger.PlannedObject(slot.getPart(), slot.getSubKey(), prefix + "part-" + i,
                    i == 0 ? coreSize : 1, "a".repeat(64), "application/protobuf"));
        }
        var attempts = new DocumentPartAttemptLedger(tx);
        var attempt = attempts.begin(new DocumentPartAttemptLedger.Plan(id,
                new DocumentPartAttemptLedger.Location(node, address.getAccountId(), generation, drive.bucket),
                prior == null ? 0 : prior.mutationRevision, Map.of(), objects), Duration.ofMinutes(5));
        // Exercise real SQL guards with explicitly synthetic observations; no successful SDK response is fabricated.
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_part_attempt_objects SET verified=true,provider_version=:version,etag='fixture' WHERE attempt_id=:id")
                    .setParameter("version", version).setParameter("id", id).executeUpdate();
            em.createNativeQuery("UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id").setParameter("id", id).executeUpdate();
        });
        var row = new DocumentRecord(); row.nodeId = node; row.accountId = address.getAccountId(); row.docId = address.getDocId();
        row.graphId = address.getGraphId(); row.graphAddressId = address.getGraphAddressId(); row.rowKind = DocumentRowKind.PIPELINE;
        row.datasourceId = "source"; row.driveName = drive.name; row.objectKey = prefix;
        row.versionId = version; row.etag = "fixture"; row.sizeBytes = Math.addExact(coreSize, parts - 1L);
        row.createdAt = Instant.now(); row.updatedAt = row.createdAt; row.writeSecurity(policy);
        var manifest = DocumentManifest.newBuilder().setAddress(address)
                .setDocVersion(prior == null ? 1 : Math.addExact(prior.readManifest().getDocVersion(), 1));
        if (sparseManifest) manifest.addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)
                .setState(PartState.PART_STATE_EMPTY));
        for (var object : objects) {
            manifest.addParts(PartManifestEntry.newBuilder().setPart(object.part()).setSubKey(object.subKey())
                    .setState(PartState.PART_STATE_PRESENT).setObjectKey(object.objectKey()).setSizeBytes(object.size()).setSha256(object.sha256()));
            if (sparseManifest && object.part() == DocumentPart.DOCUMENT_PART_CORE)
                manifest.addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)
                        .setState(PartState.PART_STATE_DELETED).setDeletedReason("Synthetic tombstone"));
        }
        row.writeManifest(manifest.build()); row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest.build());
        row = documents.saveVerifiedAttempt(row, prior == null ? null : prior.mutationRevision, Map.of(), id, attempt.token(),
                new DocumentPublicationTarget(new DriveLedger(tx), drive, generation, profile.identity()), (em, saved) -> {});
        var identities = tx.readOnly(em -> {
            var result = new ArrayList<PublicationObjectIdentity>();
            for (var values : em.unwrap(org.hibernate.Session.class).createNativeQuery(
                    "SELECT physical_object_id,ordinal FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal", Object[].class)
                    .setParameter("id", id).getResultList()) {
                var object = objects.get(((Number) values[1]).intValue());
                var identity = PublicationObjectIdentity.newBuilder().setObjectId(values[0].toString())
                        .setBackendGeneration(generation).setStorageRealm(profile.storageRealm()).setNamespace(drive.bucket)
                        .setObjectKey(object.objectKey()).setSizeBytes(object.size()).setSha256(object.sha256()).setContentType(object.contentType());
                if (version != null) identity.setProviderVersion(version);
                result.add(identity.build());
            }
            return List.copyOf(result);
        });
        return new ManagedDocumentFixture(row, id, List.copyOf(slots), identities);
    }
}
