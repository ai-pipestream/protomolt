package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Internal SQL staging only. The qualified composition supplies sampled physical
 * placement and authenticated caller. Current policy and revisions are checked
 * here, along with retained current source bindings. Provider qualification for
 * physical reuse, schema validation, provider retry reconciliation and provider I/O remain
 * separate, unimplemented boundaries.
 */
final class DocumentOperationUploadAdmission {
    private final Tx tx;
    private final DriveLedger drives;

    DocumentOperationUploadAdmission(Tx tx, DriveLedger drives) {
        this.tx = Objects.requireNonNull(tx);
        this.drives = Objects.requireNonNull(drives);
    }

    /** Only prepare() can couple a canonical command with its executable subset. */
    static final class Prepared {
        private final DocumentUploadPlan.Prepared plan;
        private final Duration lease;
        private final List<UploadMember> uploads;
        private final List<DocumentUploadPlan.Placement> placements;
        private final DocumentAdmissionAuthorization.Prepared authorization;
        private final DocumentReuseAdmission.Prepared reuse;
        private final String selections;

        private Prepared(DocumentUploadPlan.Prepared plan, Duration lease) {
            this.plan = plan;
            this.authorization = DocumentAdmissionAuthorization.prepare(plan);
            this.reuse = DocumentReuseAdmission.prepare(plan);
            this.selections = DocumentOperationSelection.encode(plan);
            this.lease = lease;
            this.uploads = plan.members().stream().filter(member -> member.attempt().isPresent())
                    .map(member -> new UploadMember(member, UUID.randomUUID())).toList();
            this.placements = plan.members().stream().map(DocumentUploadPlan.Member::placement).distinct()
                    .sorted(Comparator.comparing(placement -> placement.drive().id())).toList();
        }

        DocumentUploadPayloads preparePayloads(Set<String> members,
                Map<DocumentUploadPayloads.Key, PartObject> supplied, PayloadBudget budget) {
            return DocumentUploadPayloads.prepare(plan, members, supplied, budget);
        }

        DocumentUploadPayloads.Use claimPayloads(DocumentUploadPayloads payloads, Set<String> members) {
            return payloads.claim(plan, members);
        }

        List<DocumentUploadPlan.Member> members() { return plan.members(); }
        Duration lease() { return lease; }
    }

    private record EncodedMember(DocumentUploadPlan.Member member, UUID token, DocumentAttemptPlanEncoding encoded) {}
    private record UploadMember(DocumentUploadPlan.Member member, UUID token) {}

    static Prepared prepare(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements,
            Map<String, UUID> attempts, Duration lease) {
        Objects.requireNonNull(lease);
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Upload admission lease requires one second to one day");
        return new Prepared(DocumentUploadPlan.prepare(command, placements, attempts), lease);
    }

    /** All rows commit together; duplicate attempt identity fails without adopting existing bytes. */
    List<DocumentPartAttemptLedger.Attempt> admit(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, Prepared prepared) {
        return stage(caller, owner, prepared, Map.of());
    }

    /** Replaces only named uploading members; all command authorization is checked again. */
    List<DocumentPartAttemptLedger.Attempt> retry(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            Prepared prepared, Map<String, DocumentOperationSelection.Expected> expected) {
        var selections = Map.copyOf(expected);
        if (selections.isEmpty()) throw new IllegalArgumentException("Retry requires at least one uploading member");
        var uploads = prepared.uploads.stream().filter(upload -> selections.containsKey(upload.member.intent().getMemberId())).toList();
        if (uploads.size() != selections.size()) throw new IllegalArgumentException("Retry member is not an uploading command member");
        for (var upload : uploads) {
            if (upload.member.attempt().orElseThrow().id().equals(selections.get(upload.member.intent().getMemberId()).attempt()))
                throw new IllegalArgumentException("Retry requires a new attempt identity");
        }
        return stage(caller, owner, prepared, selections);
    }

    private List<DocumentPartAttemptLedger.Attempt> stage(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            Prepared prepared, Map<String, DocumentOperationSelection.Expected> replacements) {
        Objects.requireNonNull(owner); Objects.requireNonNull(prepared);
        var command = prepared.plan.command();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Upload command differs from operation scope");
        // Encode only the selected subset, before acquiring any SQL locks.
        var uploads = prepared.uploads.stream().filter(upload -> replacements.isEmpty()
                        || replacements.containsKey(upload.member.intent().getMemberId()))
                .map(upload -> new EncodedMember(upload.member, upload.token, DocumentAttemptPlanEncoding.prepare(upload.member)))
                .toList();
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            requireCommand(em, owner.key(), command);
            DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, prepared.plan, prepared.authorization);
            DocumentReuseAdmission.requireBoundSources(em, prepared.reuse);
            for (var placement : prepared.placements) {
                placement.drive().lock(em, drives);
                var actual = ManagedBackendLedger.find(em, placement.generation())
                        .orElseThrow(() -> new IllegalArgumentException("Selected backend generation is not registered"));
                if (!actual.equals(placement.profile()))
                    throw new IllegalArgumentException("Selected backend profile differs from its immutable registration");
            }
            var admitted = new ArrayList<DocumentPartAttemptLedger.Attempt>(uploads.size());
            for (var upload : uploads) {
                var member = upload.member;
                var attempt = member.attempt().orElseThrow();
                var location = attempt.location();
                var realm = member.placement().profile().storageRealm();
                em.createNativeQuery("""
                        INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                            storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state,
                            plan_kind,operation_principal,operation_id,operation_generation,member_id,drive_id)
                        VALUES (:id,:node,:account,:revision,:backend,:realm,:namespace,:count,:sources,:token,
                            clock_timestamp()+(:millis * interval '1 millisecond'),'PLANNING','NEW_CONTENT',
                            :principal,:operation,:generation,:member,:drive)
                        """).setParameter("id", attempt.id()).setParameter("node", member.nodeId())
                        .setParameter("account", location.accountId())
                        .setParameter("revision", member.intent().getDestination().getExpectedMutationRevision())
                        .setParameter("backend", location.backendGeneration()).setParameter("realm", realm)
                        .setParameter("namespace", location.namespace()).setParameter("count", attempt.uploads().size())
                        .setParameter("sources", member.sources().size()).setParameter("token", upload.token)
                        .setParameter("millis", prepared.lease.toMillis()).setParameter("principal", owner.key().principal())
                        .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                        .setParameter("member", member.intent().getMemberId()).setParameter("drive", member.placement().drive().id())
                        .executeUpdate();
                DocumentPartAttemptLedger.insertEncodedRows(em, upload.encoded, attempt.id(), realm, location.namespace());
                em.createNativeQuery("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=:id")
                        .setParameter("id", attempt.id()).executeUpdate();
                admitted.add(DocumentPartAttemptLedger.read(em, attempt.id(), false).orElseThrow());
            }
            if (!admitted.isEmpty()) {
                boolean live = (Boolean) em.createNativeQuery("""
                        SELECT count(*)=:count AND bool_and(state='STAGING') AND min(lease_until)>clock_timestamp()
                        FROM document_part_attempts WHERE attempt_id IN (:attempts)
                        """).setParameter("count", admitted.size())
                        .setParameter("attempts", admitted.stream().map(DocumentPartAttemptLedger.Attempt::id).toList()).getSingleResult();
                if (!live) throw new DocumentPartAttemptLedger.FenceException("New-content attempt expired before admission completed");
            }
            if (replacements.isEmpty()) {
                DocumentOperationSelection.insert(em, owner, prepared.selections, prepared.plan.members().size());
            } else {
                for (var upload : uploads) DocumentOperationSelection.replace(em, owner,
                        upload.member.intent().getMemberId(), replacements.get(upload.member.intent().getMemberId()),
                        upload.member.attempt().orElseThrow().id());
            }
            // Includes reuse-only commands and lease expiry during drive lock waits.
            em.createNativeQuery("SELECT require_repository_operation_write_fence(:account,:principal,:operation,:generation)")
                    .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).getSingleResult();
            return List.copyOf(admitted);
        });
    }

    private static void requireCommand(EntityManager em, RepositoryOperationLedger.Key key, DocumentPublicationCommand expected) {
        var rows = em.createNativeQuery("""
                SELECT command_codec,command_version,command,command_sha256 FROM repository_operations
                WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                """).setParameter("account", key.account()).setParameter("principal", key.principal())
                .setParameter("operation", key.operationId()).getResultList();
        if (rows.size() != 1) throw new RepositoryOperationLedger.CommandConflictException();
        Object[] row = (Object[]) rows.getFirst();
        if (!DocumentPublicationCommand.CODEC.equals(row[0])
                || DocumentPublicationCommand.ENCODING_VERSION != ((Number) row[1]).intValue()
                || !expected.canonical().equals(ByteString.copyFrom((byte[]) row[2]))
                || !expected.sha256().equals(HexFormat.of().formatHex((byte[]) row[3])))
            throw new RepositoryOperationLedger.CommandConflictException();
    }
}
