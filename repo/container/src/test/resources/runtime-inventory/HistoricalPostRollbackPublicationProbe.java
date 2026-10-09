package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.*;
import javax.sql.DataSource;

/** Publish the same command through fresh provider reads after the old transaction rolls back. */
final class HistoricalPostRollbackPublicationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord original, RepositorySuccessorInstall.Plan failedPlan,
            RepositoryCoordinatorReservation.ExpiredUnquiesced reservation,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            DocumentRevisionAssembly.Limits limits, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation, DataSource database) throws Exception {
        var plan = RepositorySuccessorInstall.prepare(reservation, failedPlan.next(), Duration.ofMinutes(2), failedPlan.modes());
        var command = plan.next().command();
        require(plan.previous().equals(failedPlan.next())
                && plan.next().predecessorGeneration() == failedPlan.next().predecessorGeneration() + 1,
                "installation advances exact failed predecessor");
        require(reservation.predecessor().token().equals(failedPlan.reservation().successorToken())
                && !reservation.successorToken().equals(reservation.predecessor().token())
                && !reservation.successorIncarnation().equals(reservation.predecessor().incarnation()),
                "successor advances claim token and process incarnation");
        require(!plan.next().seeds().ownerNonce().equals(failedPlan.next().seeds().ownerNonce())
                && Collections.disjoint(plan.next().seeds().attempts().values(), failedPlan.next().seeds().attempts().values())
                && Collections.disjoint(plan.next().seeds().uploadTokens().values(), failedPlan.next().seeds().uploadTokens().values()),
                "successor has fresh owner, attempt and upload-token identities");
        long before = budget.reservedBytes();
        var attempts = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
        try (var prepared = new HistoricalInstalledOwnerProbe.Prepared(attempts, plan, budget, before, coordinator, null)) {
            try (var pending = attempts.beginInstalled(caller, plan, original)) {
                require(pending.identity() != null, "successor capacity reserved before installation");
            }
            RepositorySuccessorInstall.install(tx, budget, coordinator, plan, RepositoryReadControl.NONE);
            var selector = command.intent().getMembers(0).getPartsList().stream().filter(p -> p.hasHistoricalReuse())
                    .findFirst().orElseThrow().getHistoricalReuse();
            var reads = new DocumentReadLedger(tx, UUID.randomUUID());
            var history = reads.captureHistorical(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()));
            try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                 var accepted = sources.work()) {
                var fresh = new HashMap<Integer, ByteString>();
                try (var use = history.use()) {
                    for (var entry : use.plan().entries()) {
                        var part = entry.part();
                        var bytes = ByteString.copyFrom(provider.store().getBounded(part.binding().namespace(),
                                part.part().key(), part.part().providerVersion(), Math.toIntExact(part.part().size())).data());
                        for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                            var declaration = command.intent().getMembers(0).getParts(ordinal);
                            if (declaration.hasHistoricalReuse()
                                    && declaration.getHistoricalReuse().getObject().getObjectId().equals(entry.objectId().toString())) {
                                require(bytes.equals(fragments.get(ordinal)), "successor rereads exact retained provider version");
                                fresh.put(ordinal, bytes);
                            }
                        }
                    }
                }
                for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                    var part = command.intent().getMembers(0).getParts(ordinal);
                    if (part.hasUpload()) {
                        var bytes = Objects.requireNonNull(fragments.get(ordinal));
                        require(bytes.size() == part.getUpload().getSizeBytes()
                                && ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes.toByteArray()).equals(part.getUpload().getSha256()),
                                "caller resubmits exact declared upload");
                        fresh.put(ordinal, bytes);
                    }
                }
                require(fresh.size() == fragments.size(), "fresh capture and caller supply complete candidate");
                HistoricalInstalledOwnerProbe.run(tx, provider, caller, coordinator, original, prepared, sources, accepted,
                        policy, fresh, container, resolver, limits, budget, observation, null, database,
                        HistoricalInstalledOwnerProbe.Check.RECOVERED_PUBLICATION);
            } finally {
                history.close();
                require(history.awaitDrained(Duration.ofSeconds(1)), "successor history drains before release");
                history.release(); reads.fence(); reads.attestLocalQuiescence();
            }
        }
        require(budget.reservedBytes() == before, "successor returns retained memory");
        var result = new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow();
        require(result.getOwnerGeneration() == plan.next().predecessorGeneration() + 1
                && result.getCommandSha256().equals(command.sha256()), "receipt belongs to exact successor command and generation");
        require(count(tx, command, "repository_coordinator_expirations") == 2
                && count(tx, command, "repository_successor_installs") == 2, "exactly two reservations and installs");
        var priorPins = pins(tx, command, failedPlan.reservation().successorToken());
        var successorPins = pins(tx, command, reservation.successorToken());
        require(!priorPins.isEmpty() && !successorPins.isEmpty() && Collections.disjoint(priorPins, successorPins),
                "successor uses distinct capture pins from failed publisher");
        System.out.println("HISTORICAL_POST_ROLLBACK_SUCCESSOR_PUBLICATION_OK");
    }
    private static long count(Tx tx, DocumentPublicationCommand command, String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    private static Set<UUID> pins(Tx tx, DocumentPublicationCommand command, UUID token) {
        return tx.readOnly(em -> {
            var result = new HashSet<UUID>();
            for (Object value : em.createNativeQuery("""
                    SELECT p.pin_id FROM repository_preparation_source_pins p JOIN repository_preparation_pin_owners o
                    USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                    WHERE p.operation_id=:op AND o.claim_token=:token
                    """).setParameter("op", command.operationId()).setParameter("token", token).getResultList())
                result.add((UUID) value);
            return Set.copyOf(result);
        });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
