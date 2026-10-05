package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Real uploads, commits, lease expiry and takeover; controlled SQL changes only source access policy. */
public final class PromotedAssessmentCommitProbe {
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final DocumentRevisionAssembly.Limits LIMITS =
            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);

    static DocumentSchemaPolicies.Selection run(Tx tx, AssessmentProviderProbe provider,
            AssessmentMixedReuseProbe.Source source, DocumentSchemaPolicies.Selection policy) throws Exception {
        var current = policy;
        long committedRevision = 0;
        for (int scenario : new int[]{0, 4, 5, 2, 3, 1}) {
            var original = source.candidate();
            var destination = original.getDestination().toBuilder()
                    .clearExpectedMutationRevision().setIfAbsent(true)
                    .setAddress(original.getDestination().getAddress().toBuilder()
                            .setGraphId("promoted-commit-" + (scenario == 1 ? 0 : scenario)));
            if (scenario == 1) destination.clearIfAbsent().setExpectedMutationRevision(committedRevision);
            var member = original.toBuilder().setDestination(destination).build();
            var command = AssessmentMixedReuseProbe.command(member);
            var budget = new PayloadBudget(128_000_000);
            var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
            final RepositoryOperationLedger.Owner owner;
            final AssessmentMixedReuseProbe.Uploads uploaded;
            if (scenario >= 4) {
                var saved = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(source.placement().drive().id(), source.placement()), Duration.ofMinutes(5), 0);
                var claim = new RepositoryExecutionClaimLedger(tx).acquire(key, command, UUID.randomUUID(), Duration.ofMinutes(5));
                new DocumentPublicationPreparationJournal(tx, budget).save(CALLER, claim, saved, RepositoryReadControl.NONE);
                new DocumentPublicationModesJournal(tx, budget).bind(CALLER, claim, 0,
                        Map.of("a", scenario == 4 ? DocumentPublicationCandidate.Mode.OPAQUE : DocumentPublicationCandidate.Mode.TYPED), RepositoryReadControl.NONE);
                owner = new RepositoryOperationLedger(tx).admit(key, command, saved.seeds().ownerNonce(), Duration.ofMinutes(5), claim).owner().orElseThrow();
                uploaded = AssessmentMixedReuseProbe.upload(tx, provider, owner, saved.prepare(), source.fragments());
            } else {
                owner = new RepositoryOperationLedger(tx).admit(key, command, UUID.randomUUID(),
                        scenario == 2 ? Duration.ofSeconds(15) : Duration.ofMinutes(5)).owner().orElseThrow();
                uploaded = AssessmentMixedReuseProbe.upload(tx, provider, command, owner, source.placement(), source.fragments());
            }
            var calls = new AtomicInteger();
            var definition = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
            var commit = new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false);
            try (var assessment = DocumentPublicationAssessment.prepare(command, current,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED), Map.of("a", source.fragments()),
                    Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())), (selected, occurrence) -> {
                        calls.incrementAndGet(); return definition;
                    }, budget, LIMITS, Instant.now(), () -> {});
                    var candidate = assessment.promoteAccepted(() -> {})) {
                candidate.schemas().stage(new RepositorySchemaArtifacts(tx), owner, () -> {});
                Runnable publish = () -> commit.commit(CALLER, owner, uploaded.prepared(), candidate.opaque(),
                        uploaded.selected(), candidate.schemas(), () -> {});
                if (scenario == 4) {
                    try { publish.run(); throw new AssertionError("Direct commit bypassed fixed publication modes"); }
                    catch (RepositoryException expected) {
                        require(expected.code() == RepositoryException.Code.FAILED_PRECONDITION
                                && expected.getMessage().equals("Committed publication modes differ from fixed modes"), "direct mode mismatch refusal");
                    }
                    System.out.println("JOURNALED_DIRECT_COMMIT_MISMATCH_OK");
                } else if (scenario == 1) {
                    var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
                    tx.inTransaction(em -> {
                        DocumentAdmissionAuthorization.lockAndAuthorize(em, scoped,
                                uploaded.prepared().plan(), DocumentAdmissionAuthorization.prepare(uploaded.prepared().plan()));
                    });
                    try {
                        sourceAccess(tx, source.node(), "ACCESS_DENY");
                        try {
                            commit.commit(scoped, owner, uploaded.prepared(), candidate.opaque(), uploaded.selected(),
                                    candidate.schemas(), () -> {});
                            throw new AssertionError("Denied source was published");
                        }
                        catch (RepositoryException expected) {
                            require(expected.code() == RepositoryException.Code.NOT_FOUND, "source access checked at commit");
                        }
                        require(outcomes(tx, command) == 0, "source refusal cannot write a terminal outcome");
                    } finally { sourceAccess(tx, source.node(), "ACCESS_READ"); }
                    try { publish.run(); throw new AssertionError("Restoring access revived a stale source revision"); }
                    catch (DocumentLedger.RevisionConflictException expected) { /* A new command must sample the new revision. */ }
                } else if (scenario == 2) {
                    awaitExpiry(tx, owner);
                    var replacement = new RepositoryOperationLedger(tx).takeOver(owner.key(), command,
                            owner.generation(), UUID.randomUUID(), Duration.ofMinutes(5));
                    require(replacement.generation() > owner.generation(), "takeover advances actual owner generation");
                    try { publish.run(); throw new AssertionError("Replaced owner published"); }
                    catch (RepositoryOperationLedger.OwnerFencedException expected) { /* Exact fence refusal. */ }
                } else if (scenario == 3) {
                    current = new DocumentSchemaPolicies(tx).activate(current.policy(), current.revision(), () -> {});
                    require(current.revision() > candidate.schemas().policy().revision(), "policy revision advances after promotion");
                    try { publish.run(); throw new AssertionError("Stale policy published"); }
                    catch (DocumentSchemaPolicies.StalePolicy expected) { /* Even the same policy bytes need a current revision. */ }
                }
                if (scenario == 0 || scenario == 5) {
                    var result = commit.commit(CALLER, owner, uploaded.prepared(), candidate.opaque(), uploaded.selected(),
                            candidate.schemas(), () -> {});
                    require(result.getMembersCount() == 1, "promoted candidate committed");
                    require(new DocumentPublicationReplay(tx).observe(CALLER, command).result().orElseThrow().equals(result),
                            "exact committed result replays without resolution");
                    require(outcomes(tx, command) == 1, "exactly one success outcome");
                    var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getDestination().getAddress());
                    var row = new DocumentLedger(tx).findByNodeId(node).orElseThrow();
                    if (scenario == 0) committedRevision = row.mutationRevision;
                    else System.out.println("JOURNALED_DIRECT_COMMIT_MATCH_OK");
                    var publication = new DocumentPublicationLedger(tx).findForRead(row).orElseThrow();
                    long reused = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM document_revision_parts WHERE revision_id=:revision AND object_id=:object
                            """).setParameter("revision", publication.revisionId()).setParameter("object", source.object())
                            .getSingleResult()).longValue());
                    require(reused == 1, "committed revision retains the exact reused source object");
                } else {
                    require(outcomes(tx, command) == 0, "fenced promotion has no terminal outcome");
                    var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getDestination().getAddress());
                    var row = new DocumentLedger(tx).findByNodeId(node);
                    if (scenario == 1) require(row.orElseThrow().mutationRevision == committedRevision,
                            "source refusal leaves previous destination revision unchanged");
                    else require(row.isEmpty(), "failed transaction leaves no destination");
                }
                require(calls.get() == 1, "promotion, staging, commit and replay never revisit registry");
            }
            require(budget.reservedBytes() == 0, "all promotion bytes released after commit or refusal");
        }
        System.out.println("PROMOTED_ASSESSMENT_COMMIT_OK");
        return current;
    }

    private static long outcomes(Tx tx, DocumentPublicationCommand command) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT (SELECT count(*) FROM repository_operation_success WHERE operation_id=:op)
                     + (SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op)
                """).setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    private static void awaitExpiry(Tx tx, RepositoryOperationLedger.Owner owner) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (true) {
            boolean expired = tx.readOnly(em -> (Boolean) em.createNativeQuery("""
                    SELECT lease_until<=clock_timestamp() FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:op
                    """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("op", owner.key().operationId()).getSingleResult());
            if (expired) return;
            require(System.nanoTime() < deadline, "real owner lease expires within fixture deadline");
            Thread.sleep(50);
        }
    }
    private static void sourceAccess(Tx tx, UUID node, String access) {
        tx.inTransaction(em -> {
            require(em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("node", node).executeUpdate() == 1, "source access policy changed");
        });
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
