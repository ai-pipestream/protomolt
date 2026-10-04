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
        return publish(tx,drive,generation,profile,address,policy,parts,coreSize,version,sparseManifest,null);
    }

    static ManagedDocumentFixture publish(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, DocumentSecurity policy,
            int parts, long coreSize, String version, boolean sparseManifest, WriteProvenance provenance) {
        return publish(tx, drive, generation, profile, address, policy, parts, coreSize, version, sparseManifest, provenance, false);
    }

    /** Populate a pre-V59 database using its historical SQL protocol, never today's Java writer. */
    static ManagedDocumentFixture publishBeforePolicyFence(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, int parts, long coreSize, String version) {
        return publishBeforePolicyFence(tx, drive, generation, profile, address, parts, coreSize, version, false);
    }

    static ManagedDocumentFixture publishBeforePolicyFence(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, int parts, long coreSize, String version, boolean sparse) {
        return publish(tx, drive, generation, profile, address, DocumentSecurity.getDefaultInstance(),
                parts, coreSize, version, sparse, null, true);
    }

    private static ManagedDocumentFixture publish(Tx tx, DriveRecord drive, String generation,
            ManagedBackendLedger.Profile profile, NodeAddress address, DocumentSecurity policy,
            int parts, long coreSize, String version, boolean sparseManifest, WriteProvenance provenance, boolean historical) {
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
            var entry=PartManifestEntry.newBuilder().setPart(object.part()).setSubKey(object.subKey())
                    .setState(PartState.PART_STATE_PRESENT).setObjectKey(object.objectKey()).setSizeBytes(object.size()).setSha256(object.sha256());
            if (provenance!=null) entry.setWrittenBy(provenance);
            manifest.addParts(entry);
            if (sparseManifest && object.part() == DocumentPart.DOCUMENT_PART_CORE)
                manifest.addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)
                        .setState(PartState.PART_STATE_DELETED).setDeletedReason("Synthetic tombstone"));
        }
        row.writeManifest(manifest.build()); row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest.build());
        var target = new DocumentPublicationTarget(new DriveLedger(tx), drive, generation, profile.identity());
        row = historical ? saveBeforePolicyFence(tx, row, prior, id, attempt.token(), target)
                : documents.saveVerifiedAttempt(row, prior == null ? null : prior.mutationRevision, Map.of(), id, attempt.token(),
                        target, (em, saved) -> {});
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

    private static DocumentRecord saveBeforePolicyFence(Tx tx, DocumentRecord candidate, DocumentRecord prior,
            UUID attemptId, UUID token, DocumentPublicationTarget target) {
        return tx.inTransaction(em -> {
            var nodes = java.util.Set.of(candidate.nodeId);
            var locked = DocumentRevisionLocks.lock(em, nodes, java.util.Set.of());
            Long revision = prior == null ? null : prior.mutationRevision;
            DocumentLedger.requireRevision(locked.get(candidate.nodeId), revision);
            target.lock(em);
            var origins = DocumentPublicationLocks.lockOrigins(em, nodes, java.util.Set.of(attemptId));
            var attempt = DocumentPartAttemptLedger.requirePublishable(em, attemptId, token, candidate, revision, Map.of());
            target.requireMatches(em, candidate, attempt);
            DocumentPublicationLocks.lockRetention(em, origins);
            var saved = em.merge(candidate); em.flush(); em.refresh(saved);
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    SELECT :attempt,node_id,mutation_revision,document_publication_body(documents)
                    FROM documents WHERE node_id=:node
                    """).setParameter("attempt", attemptId).setParameter("node", saved.nodeId).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_part_publications(node_id,attempt_id) VALUES(:node,:attempt)
                    ON CONFLICT(node_id) DO UPDATE SET attempt_id=EXCLUDED.attempt_id
                    """).setParameter("node", saved.nodeId).setParameter("attempt", attemptId).executeUpdate();
            return saved;
        });
    }
}
