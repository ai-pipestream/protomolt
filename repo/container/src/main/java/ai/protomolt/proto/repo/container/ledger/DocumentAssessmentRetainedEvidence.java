package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Integrity of historical evidence; never claims fresh observation or candidate validation. */
final class DocumentAssessmentRetainedEvidence {
    private DocumentAssessmentRetainedEvidence() {}
    private record Root(String member, int ordinal, String codec, int version, String sha) {}
    private record Fragment(long size, String sha) {}

    /** Caller holds current read authorization and the retained owner lock. */
    static void verify(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity, DocumentPublicationCommand command,
            ByteString bytes, int expectedArtifacts, int expectedRoots, PayloadBudget budget, Runnable control) {
        final ai.protomolt.proto.repo.v1.DocumentPublicationAssessmentManifest manifest;
        try {
            manifest = DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC, DocumentAssessmentManifestCodec.VERSION,
                    bytes, identity.manifestSha256(), amount -> { var lease = budget.reserve(amount); return lease::close; }, control);
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            throw new IllegalStateException("Retained assessment manifest is invalid", invalid);
        }
        if (!manifest.getOperationId().equals(identity.key().operationId().toString())
                || !manifest.getAccountId().equals(identity.key().account()) || !manifest.getPrincipal().equals(identity.key().principal())
                || manifest.getOwnerGeneration() != identity.generation() || !manifest.getCommandSha256().equals(command.sha256())
                || !manifest.getCommandCodec().equals(DocumentPublicationCommand.CODEC)
                || manifest.getCommandVersion() != DocumentPublicationCommand.ENCODING_VERSION) throw conflict();
        var members = command.intent().getMembersList().stream().collect(Collectors.toMap(m -> m.getMemberId(), m -> m));
        var seen = new HashSet<String>();
        var artifacts = new HashSet<String>();
        var roots = new HashMap<Root, Fragment>();
        for (var member : manifest.getMembersList()) {
            control.run(); var declaration = members.get(member.getMemberId());
            if (declaration == null || !seen.add(member.getMemberId())) throw conflict();
            if (!member.hasTyped()) { if (!member.getOpaque()) throw conflict(); continue; }
            add(member.getTyped().getContainer(), artifacts);
            for (var schema : member.getTyped().getPayloadSchemasList()) { control.run(); add(schema, artifacts); }
            for (var root : member.getTyped().getRootsList()) {
                control.run();
                if (root.getOrdinal() < 0 || root.getOrdinal() >= declaration.getPartsCount()) throw conflict();
                var part = declaration.getParts(root.getOrdinal());
                Fragment fragment;
                if (part.hasUpload()) fragment = new Fragment(part.getUpload().getSizeBytes(), part.getUpload().getSha256());
                else if (part.hasReuse()) fragment = new Fragment(part.getReuse().getObject().getSizeBytes(), part.getReuse().getObject().getSha256());
                else if (part.hasHistoricalReuse()) fragment = new Fragment(part.getHistoricalReuse().getObject().getSizeBytes(),
                        part.getHistoricalReuse().getObject().getSha256());
                else throw conflict();
                if (roots.put(new Root(member.getMemberId(), root.getOrdinal(), root.getCodec(), root.getVersion(), root.getSha256()), fragment) != null)
                    throw conflict();
            }
        }
        if (!seen.equals(members.keySet()) || artifacts.size() != expectedArtifacts || roots.size() != expectedRoots) throw conflict();
        verifyArtifacts(em, identity.assessment(), artifacts, control);
        var rows = em.createNativeQuery("""
                SELECT member_id,revision_ordinal,evidence_codec,evidence_version,encode(evidence_sha256,'hex'),
                    encode(fragment_sha256,'hex'),fragment_size,evidence_size,encode(sha256(evidence_bytes),'hex')
                FROM document_assessment_roots WHERE assessment_id=:id LIMIT 4097
                """).setParameter("id", identity.assessment()).getResultList();
        if (rows.size() != roots.size()) throw conflict();
        long total = 0;
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            var key = new Root((String) row[0], ((Number) row[1]).intValue(), (String) row[2], ((Number) row[3]).intValue(), (String) row[4]);
            var fragment = roots.remove(key); long size = ((Number) row[7]).longValue();
            if (fragment == null || !fragment.sha().equals(row[5]) || fragment.size() != ((Number) row[6]).longValue()
                    || size < 1 || size > 4194304 || !key.sha().equals(row[8]) || (total += size) > 67108864) throw conflict();
        }
        control.run();
    }

    private static void verifyArtifacts(EntityManager em, UUID assessment, Set<String> expected, Runnable control) {
        var rows = em.createNativeQuery("""
                SELECT encode(r.artifact_sha256,'hex'),a.size_bytes,encode(sha256(a.artifact_bytes),'hex')
                FROM document_assessment_artifacts r JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
                WHERE r.assessment_id=:id LIMIT 65
                """).setParameter("id", assessment).getResultList();
        if (rows.size() != expected.size()) throw conflict();
        var remaining = new HashSet<>(expected); long total = 0;
        for (Object value : rows) {
            control.run(); var row = (Object[]) value; long size = ((Number) row[1]).longValue();
            if (!remaining.remove((String) row[0]) || !row[0].equals(row[2]) || size < 1 || size > 16777216 || (total += size) > 67108864)
                throw conflict();
        }
    }
    private static void add(RepositorySchemaAssetReference schema, Set<String> artifacts) {
        artifacts.add(schema.getDescriptorSha256()); artifacts.add(schema.getMetadataSha256());
        if (schema.hasSourceSha256()) artifacts.add(schema.getSourceSha256());
    }
    private static IllegalStateException conflict() { return new IllegalStateException("Retained assessment evidence differs from original manifest"); }
}
