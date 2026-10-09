package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationAssessmentManifest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Internal borrowed replay inputs. The caller must finish all use before closing this scope. */
final class DocumentAssessmentReplayInputs implements AutoCloseable {
    record Root(String member, int ordinal, String locatorSha, String fragmentSha, long fragmentSize,
                DocumentSchemaAdmission.EncodedEvidence evidence) {}
    record Snapshot(DocumentPublicationCommand command, DocumentPublicationAssessmentManifest manifest,
                    DocumentAdmissionPolicy policy, List<Root> roots, Map<String, ByteString> artifacts) {
        Snapshot { roots = List.copyOf(roots); artifacts = Map.copyOf(artifacts); }
    }
    private final DocumentReadLedger.PinnedRead<DocumentAssessmentReadPlan>.Use use;
    private final List<PayloadBudget.Lease> leases = new ArrayList<>();
    private Snapshot snapshot;
    private boolean closed;

    private DocumentAssessmentReplayInputs(DocumentReadLedger.PinnedRead<DocumentAssessmentReadPlan>.Use use) {
        this.use = use;
    }

    /** Only the ledger supplies the command bound to the capture; no caller-selected storage identities. */
    static DocumentAssessmentReplayInputs load(Tx tx, DocumentReadLedger.PinnedAssessment capture,
            DocumentPublicationCommand command, PayloadBudget budget, RepositoryReadControl control) {
        Objects.requireNonNull(budget); Objects.requireNonNull(control);
        var active = new RepositoryReadControl() {
            @Override public boolean isCancelled() {
                if (Thread.currentThread().isInterrupted()) return true;
                return control.isCancelled() || Thread.currentThread().isInterrupted();
            }
            @Override public long remainingNanos() { return control.remainingNanos(); }
        };
        active.check();
        var result = new DocumentAssessmentReplayInputs(capture.use());
        boolean delivered = false;
        try {
            capture.authorizeDelivery(result.use, active);
            // Bounded views apply SQL limits transaction-locally. Load the retained
            // inputs in one unit; provider reads and CEL evaluation occur after it ends.
            result.snapshot = tx.inTransaction(em -> { return result.read(em, command, budget, active); });
            capture.authorizeDelivery(result.use, active);
            delivered = true;
            return result;
        } catch (java.util.concurrent.CancellationException cancelled) {
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Assessment input loading cancelled");
        } catch (RuntimeException failure) {
            if (failure instanceof RepositoryException repository
                    && (repository.code() == RepositoryException.Code.CANCELLED
                        || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
                throw new RepositoryException(repository.code(), "Assessment input loading cancelled or expired");
            active.check();
            // Authorization failure replaces private storage/decoding diagnostics.
            capture.authorizeDelivery(result.use, active);
            throw failure;
        } finally {
            if (!delivered) result.close();
        }
    }

    synchronized Snapshot snapshot() {
        if (closed) throw new IllegalStateException("Assessment replay inputs are closed");
        use.plan();
        return snapshot;
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        snapshot = null;
        leases.forEach(PayloadBudget.Lease::close);
        use.close();
    }

    private Snapshot read(EntityManager em, DocumentPublicationCommand command, PayloadBudget budget, RepositoryReadControl control) {
        var stage = use.plan().stage(); UUID id = stage.assessment();
        long manifestSize = ((Number) em.createNativeQuery(
                "SELECT octet_length(manifest_bytes) FROM document_assessment_owners WHERE assessment_id=:id")
                .setParameter("id", id).getSingleResult()).longValue();
        bounded(manifestSize, 4_194_304); reserve(budget, 2 * manifestSize); control.check();
        byte[] manifestBytes = (byte[]) em.createNativeQuery("""
                SELECT CASE WHEN octet_length(manifest_bytes)=:size THEN manifest_bytes END
                FROM document_assessment_owners WHERE assessment_id=:id
                """).setParameter("id", id).setParameter("size", manifestSize).getSingleResult();
        var encoded = checked(manifestBytes, manifestSize, stage.manifestSha256());
        final DocumentPublicationAssessmentManifest manifest;
        try {
            manifest = DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC,
                    DocumentAssessmentManifestCodec.VERSION, encoded, stage.manifestSha256(), amount -> {
                        var lease = lease(budget, amount); return lease::close;
                    }, control::check);
        } catch (InvalidProtocolBufferException invalid) { throw invalid("Assessment manifest cannot be decoded", invalid); }
        if (!manifest.getCommandSha256().equals(command.sha256())
                || !manifest.getAccountId().equals(command.intent().getAccountId())
                || !manifest.getOperationId().equals(command.operationId().toString()))
            throw invalid("Assessment manifest differs from captured command");
        control.check();
        var policy = policy(em, command.intent().getAccountId(), manifest.getPolicySha256(), budget, control);
        var roots = roots(em, id, budget, control);
        var artifacts = artifacts(em, id, command.intent().getAccountId(), budget, control);
        verifySet(command, manifest, roots, artifacts, budget, control);
        return new Snapshot(command, manifest, policy, roots, artifacts);
    }

    private record RootKey(String member, int ordinal, String codec, int version, String sha) {}
    private static void verifySet(DocumentPublicationCommand command, DocumentPublicationAssessmentManifest manifest,
            List<Root> roots, Map<String,ByteString> artifacts, PayloadBudget budget, RepositoryReadControl control) {
        var members = new HashMap<String,ai.protomolt.proto.repo.v1.DocumentPublicationMember>();
        command.intent().getMembersList().forEach(member -> members.put(member.getMemberId(), member));
        var expectedRoots = new java.util.HashSet<RootKey>();
        var expectedAssets = new java.util.HashSet<String>();
        for (var member : manifest.getMembersList()) {
            control.check();
            if (members.remove(member.getMemberId()) == null) throw invalid("Assessment member set differs");
            if (!member.hasTyped()) continue;
            var references = new ArrayList<>(member.getTyped().getPayloadSchemasList());
            references.add(member.getTyped().getContainer());
            for (var reference : references) {
                expectedAssets.add(reference.getDescriptorSha256()); expectedAssets.add(reference.getMetadataSha256());
                if (reference.hasSourceSha256()) expectedAssets.add(reference.getSourceSha256());
            }
            for (var root : member.getTyped().getRootsList())
                if (!expectedRoots.add(new RootKey(member.getMemberId(), root.getOrdinal(), root.getCodec(), root.getVersion(), root.getSha256())))
                    throw invalid("Duplicate assessment root reference");
        }
        if (!members.isEmpty() || !expectedAssets.equals(artifacts.keySet())) throw invalid("Assessment retained set differs");
        for (var root : roots) {
            control.check(); var evidence = root.evidence();
            if (!expectedRoots.remove(new RootKey(root.member(), root.ordinal(), evidence.codec(), evidence.version(), evidence.sha256())))
                throw invalid("Assessment retained root differs");
            var member = command.intent().getMembersList().stream().filter(value -> value.getMemberId().equals(root.member()))
                    .findFirst().orElseThrow();
            if (root.ordinal() < 0 || root.ordinal() >= member.getPartsCount()) throw invalid("Assessment root ordinal differs");
            var part = member.getParts(root.ordinal());
            try {
                var decoded = DocumentSchemaAdmission.decodeRootEvidence(root.ordinal(), evidence, amount -> {
                    var reservation = lease(budget, amount); return reservation::close;
                }, control::check);
                if (!decoded.locatorSha256().equals(root.locatorSha()) || !decoded.locator().getSlot().equals(part.getSlot()))
                    throw invalid("Assessment root locator differs from retained slot");
            } catch (InvalidProtocolBufferException invalid) { throw invalid("Assessment root cannot be decoded", invalid); }
            String sha = switch (part.getContentCase()) {
                case UPLOAD -> part.getUpload().getSha256();
                case REUSE -> part.getReuse().getObject().getSha256();
                case HISTORICAL_REUSE -> part.getHistoricalReuse().getObject().getSha256();
                default -> throw invalid("Retained root has no payload declaration");
            };
            long size = switch (part.getContentCase()) {
                case UPLOAD -> part.getUpload().getSizeBytes();
                case REUSE -> part.getReuse().getObject().getSizeBytes();
                case HISTORICAL_REUSE -> part.getHistoricalReuse().getObject().getSizeBytes();
                default -> throw invalid("Retained root has no payload declaration");
            };
            if (!root.fragmentSha().equals(sha) || root.fragmentSize() != size) throw invalid("Assessment root fragment differs");
        }
        if (!expectedRoots.isEmpty()) throw invalid("Assessment retained root is missing");
    }

    private DocumentAdmissionPolicy policy(EntityManager em, String account, String digest, PayloadBudget budget, RepositoryReadControl control) {
        Object[] header = (Object[]) em.createNativeQuery("""
                SELECT policy_codec,policy_version,octet_length(policy_bytes) FROM document_schema_policies
                WHERE account_id=:account AND encode(policy_sha256,'hex')=:sha
                """).setParameter("account", account).setParameter("sha", digest).getSingleResult();
        long size = ((Number) header[2]).longValue(); bounded(size, 524_288); reserve(budget, 2 * size); control.check();
        byte[] bytes = (byte[]) em.createNativeQuery("""
                SELECT CASE WHEN octet_length(policy_bytes)=:size THEN policy_bytes END FROM document_schema_policies
                WHERE account_id=:account AND encode(policy_sha256,'hex')=:sha
                """).setParameter("account", account).setParameter("sha", digest).setParameter("size", size).getSingleResult();
        try {
            var policy = DocumentAdmissionPolicy.decode((String) header[0], ((Number) header[1]).intValue(),
                    checked(bytes, size, digest), digest, control::check);
            if (!policy.definition().getAccountId().equals(account)) throw invalid("Retained policy account differs");
            return policy;
        } catch (InvalidProtocolBufferException invalid) { throw invalid("Assessment policy cannot be decoded", invalid); }
    }

    private List<Root> roots(EntityManager em, UUID id, PayloadBudget budget, RepositoryReadControl control) {
        var sizes = em.createNativeQuery("""
                SELECT evidence_size FROM document_assessment_roots WHERE assessment_id=:id LIMIT 4097
                """).setParameter("id", id).getResultList();
        long total = total(sizes, 4096, 4_194_304); reserve(budget, 2 * total); control.check();
        var rows = em.createNativeQuery("""
                SELECT member_id,revision_ordinal,encode(root_locator_sha256,'hex'),encode(fragment_sha256,'hex'),
                    fragment_size,evidence_codec,evidence_version,encode(evidence_sha256,'hex'),evidence_size,
                    CASE WHEN evidence_size BETWEEN 1 AND 4194304 THEN evidence_bytes END
                FROM document_assessment_roots WHERE assessment_id=:id
                  AND (SELECT COALESCE(sum(evidence_size),0) FROM document_assessment_roots WHERE assessment_id=:id)=:total
                ORDER BY member_id COLLATE "C",revision_ordinal,root_locator_sha256 LIMIT 4097
                """).setParameter("id", id).setParameter("total", total).getResultList();
        if (rows.size() != sizes.size()) throw invalid("Retained root set changed");
        var result = new ArrayList<Root>();
        for (Object value : rows) {
            control.check(); var row = (Object[]) value;
            result.add(new Root((String) row[0], ((Number) row[1]).intValue(), (String) row[2], (String) row[3],
                    ((Number) row[4]).longValue(), new DocumentSchemaAdmission.EncodedEvidence((String) row[5],
                    ((Number) row[6]).intValue(), checked((byte[]) row[9], ((Number) row[8]).longValue(), (String) row[7]), (String) row[7])));
        }
        return result;
    }

    private Map<String, ByteString> artifacts(EntityManager em, UUID id, String account, PayloadBudget budget, RepositoryReadControl control) {
        var sizes = em.createNativeQuery("""
                SELECT a.size_bytes FROM document_assessment_artifacts r JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
                WHERE r.assessment_id=:id AND r.account_id=:account LIMIT 65
                """).setParameter("id", id).setParameter("account", account).getResultList();
        long total = total(sizes, 64, 16_777_216); reserve(budget, 2 * total); control.check();
        var rows = em.createNativeQuery("""
                SELECT encode(a.artifact_sha256,'hex'),a.size_bytes,
                    CASE WHEN a.size_bytes BETWEEN 1 AND 16777216 THEN a.artifact_bytes END
                FROM document_assessment_artifacts r JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
                WHERE r.assessment_id=:id AND r.account_id=:account
                  AND (SELECT COALESCE(sum(a2.size_bytes),0) FROM document_assessment_artifacts r2
                       JOIN repository_schema_artifacts a2 USING(account_id,artifact_sha256)
                       WHERE r2.assessment_id=:id AND r2.account_id=:account)=:total
                ORDER BY a.artifact_sha256 LIMIT 65
                """).setParameter("id", id).setParameter("account", account).setParameter("total", total).getResultList();
        if (rows.size() != sizes.size()) throw invalid("Retained artifact set changed");
        var result = new HashMap<String,ByteString>();
        for (Object value : rows) {
            control.check(); var row = (Object[]) value;
            if (result.put((String) row[0], checked((byte[]) row[2], ((Number) row[1]).longValue(), (String) row[0])) != null)
                throw invalid("Duplicate retained artifact");
        }
        return result;
    }

    private static long total(List<?> sizes, int maxCount, long maxSize) {
        if (sizes.size() > maxCount) throw invalid("Retained input count exceeds bound");
        long total = 0;
        for (var value : sizes) {
            long size = ((Number) value).longValue(); bounded(size, maxSize); total += size;
            if (total > 67_108_864) throw invalid("Retained input bytes exceed bound");
        }
        return total;
    }
    private void reserve(PayloadBudget budget, long size) {
        leases.add(lease(budget, size));
    }
    private static PayloadBudget.Lease lease(PayloadBudget budget, long size) {
        try { return budget.reserve(size); }
        catch (PayloadBudget.CapacityExceededException exhausted) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Assessment replay capacity exhausted");
        }
    }
    private static void bounded(long size, long max) {
        if (size < 1 || size > max) throw invalid("Retained input size exceeds bound");
    }
    private static ByteString checked(byte[] bytes, long size, String digest) {
        if (bytes == null || bytes.length != size || !DocumentPartCodec.sha256Hex(bytes).equals(digest))
            throw invalid("Retained input size or digest differs");
        return ByteString.copyFrom(bytes);
    }
    private static RepositoryException invalid(String message) { return invalid(message, null); }
    private static RepositoryException invalid(String message, Throwable cause) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message, cause);
    }
}
