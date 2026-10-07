package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real provider mixed publication with assessment ownership spanning separate client calls. */
final class HistoricalInstalledOwnerProbe {
    enum Check {
        ORDINARY, REVOKED, EXPIRED, SELF_SUPERSESSION, OVERLAP, COMMIT_WINS, COMMIT_WINS_OLD_FIRST, CLAIM_EXPIRES;
        boolean commitWinner() { return this == COMMIT_WINS || this == COMMIT_WINS_OLD_FIRST; }
    }
    record Prepared(RepositoryInstalledHistoricalAttempts attempts, RepositorySuccessorInstall.Plan plan,
            PayloadBudget budget, long before, RepositoryCaller coordinator, HistoricalGenerationOverlapProbe overlap) implements AutoCloseable {
        @Override public void close() throws Exception {
            closeOwned(attempts, coordinator, overlap, budget, before);
        }
    }

    private static void closeOwned(RepositoryInstalledHistoricalAttempts attempts, RepositoryCaller coordinator,
            HistoricalGenerationOverlapProbe overlap, PayloadBudget budget, long before) throws Exception {
        Throwable primary = null;
        try {
            if (overlap != null) overlap.releaseWorker();
            attempts.close();
            require(attempts.detachClosed(Duration.ofSeconds(1), ignored -> coordinator, RepositoryReadControl.NONE),
                    "proposed owner drains on every exit path");
        } catch (Exception | Error failure) { primary = failure; }
        try {
            if (overlap != null) overlap.close();
        } catch (Exception | Error cleanup) {
            if (primary == null) primary = cleanup;
            else if (cleanup != primary) primary.addSuppressed(cleanup);
        }
        try {
            require(budget.reservedBytes() == before, "proposed owner returns retained preparation bytes");
        } catch (Exception | Error cleanup) {
            if (primary == null) primary = cleanup;
            else if (cleanup != primary) primary.addSuppressed(cleanup);
        }
        if (primary instanceof Exception failure) throw failure;
        if (primary instanceof Error failure) throw failure;
    }

    /** One owner is installed before the caller captures fresh historical sources. */
    static Prepared prepare(Tx tx, RepositoryCaller caller, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord original, Map<Integer, ByteString> fragments,
            PayloadBudget budget, Check check) throws Exception {
        long before = budget.reservedBytes();
        var ownerTx = check == Check.EXPIRED ? tx.withTimeouts(new SqlTimeouts(Duration.ofSeconds(35), Duration.ofSeconds(45))) : tx;
        var attempts = new RepositoryInstalledHistoricalAttempts(ownerTx, budget, new DriveLedger(tx), (check == Check.OVERLAP || check.commitWinner()) ? 2 : 1);
        HistoricalGenerationOverlapProbe overlap = null;
        try {
            var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
            var command = original.command();
            var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts)
                    .inspect(coordinator, original.key(), command.sha256(), RepositoryReadControl.NONE);
            require(observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND,
                    "provider owner begins from actual expired predecessor discovery");
            var bodies = new HashMap<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject>();
            require(command.intent().getMembersCount() == 1, "fixture has one mixed member");
            var member = command.intent().getMembers(0);
            for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                var part = member.getParts(ordinal);
                if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(), ordinal),
                        new ai.protomolt.proto.repo.codec.PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                Objects.requireNonNull(fragments.get(ordinal)).toByteArray(), part.getUpload().getSha256()));
            }
            require(bodies.size() == 1, "mixed fixture resubmits one fresh payload");
            var modes = Map.of(member.getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
            try (var request = attempts.beginProposed(caller, original, modes, observed, (check == Check.SELF_SUPERSESSION || check == Check.OVERLAP || check.commitWinner() || check == Check.CLAIM_EXPIRES) ? Duration.ofSeconds(30) : Duration.ofMinutes(2), timeouts)) {
                try {
                    request.advancePreparation(coordinator, modes, Map.of(), RepositoryReadControl.NONE);
                    throw new AssertionError("Missing resubmitted bytes reserved historical recovery");
                } catch (IllegalArgumentException refused) {
                    require(refused.getMessage().contains("complete command upload payloads"), "missing bytes refused before reservation");
                }
                var uploadKey = bodies.keySet().iterator().next();
                var valid = bodies.get(uploadKey); var corrupt = valid.bytes().clone(); corrupt[0] ^= 1;
                try {
                    request.advancePreparation(coordinator, modes, Map.of(uploadKey,
                            new ai.protomolt.proto.repo.codec.PartObject(valid.part(), valid.subKey(), corrupt, valid.sha256())),
                            RepositoryReadControl.NONE);
                    throw new AssertionError("Corrupt resubmitted bytes reserved historical recovery");
                } catch (IllegalArgumentException refused) {
                    require(refused.getMessage().contains("checksum differs"), "actual bytes are checked before reservation");
                }
                require(count(tx, "repository_coordinator_expirations", command.operationId()) == 0
                        && count(tx, "repository_successor_installs", command.operationId()) == 0,
                        "bad payloads perform no reservation or installation writes");
                require(request.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                        == RepositoryHistoricalAttemptPreparation.Phase.RESERVED, "first call reserves one proposal");
            }
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 1)), "proposal survives first call");
            RepositorySuccessorInstall.Plan plan;
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                require(request.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                        == RepositoryHistoricalAttemptPreparation.Phase.INSTALLED, "second call confirms retained installation");
                plan = request.installedPlan(coordinator, RepositoryReadControl.NONE);
            }
            require(count(tx, "repository_coordinator_expirations", command.operationId()) == 1
                    && count(tx, "repository_successor_installs", command.operationId()) == 1,
                    "one exact reservation and installation before any new history capture");
            if (check == Check.SELF_SUPERSESSION) {
                var previous = plan;
                tx.readOnly(em -> em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:op
                        """).setParameter("op", command.operationId()).getSingleResult());
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    require(request.reconcileUnactivated(coordinator, modes, bodies, RepositoryReadControl.NONE),
                            "same retained owner replaces its own expired installed claim");
                    try {
                        request.installedPlan(coordinator, RepositoryReadControl.NONE);
                        throw new AssertionError("Replacement exposed the old installed plan");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.CONFLICT, "replacement still requires installation");
                    }
                }
                require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 1)),
                        "same entry survives replacement request");
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    require(request.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                            == RepositoryHistoricalAttemptPreparation.Phase.INSTALLED, "replacement installs on a later request");
                    plan = request.installedPlan(coordinator, RepositoryReadControl.NONE);
                }
                require(plan.reservation().predecessor().epoch() == previous.reservation().predecessor().epoch() + 1
                        && !plan.reservation().successorToken().equals(previous.reservation().successorToken())
                        && !plan.reservation().successorIncarnation().equals(previous.reservation().successorIncarnation())
                        && !plan.next().seeds().ownerNonce().equals(previous.next().seeds().ownerNonce()),
                        "replacement advances epoch and mints distinct claim, process and owner identities");
                require(count(tx, "repository_coordinator_supersessions", command.operationId()) == 1
                        && count(tx, "repository_successor_installs", command.operationId()) == 2
                        && count(tx, "repository_historical_activations", command.operationId()) == 0,
                        "one replacement and two installs precede fresh capture or activation");
                System.out.println("SCOPED_HISTORICAL_SELF_SUPERSESSION_INSTALLED_OK");
            }
            if (check == Check.OVERLAP) {
                overlap = new HistoricalGenerationOverlapProbe(tx, caller, coordinator, attempts, command);
                plan = overlap.takeOver(plan, modes, bodies, timeouts);
            }
            System.out.println("SCOPED_HISTORICAL_PROPOSED_OWNER_INSTALLED_OK");

            return new Prepared(attempts, plan, budget, before, coordinator, overlap);
        } catch (Exception | Error failure) {
            try {
                closeOwned(attempts, coordinator, overlap, budget, before);
            } catch (Exception | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord original, Prepared prepared,
            DocumentHistoricalAssessmentSources sources, DocumentHistoricalAssessmentSources.Work accepted,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            DocumentRevisionAssembly.Limits limits, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation, HistoricalCreateCommitFault fault,
            javax.sql.DataSource database, Check check) throws Exception {
        require(!caller.processAuthority(), "installed owner uses scoped execution caller");
        var plan = prepared.plan();
        var command = plan.next().command();
        long before = prepared.before();
        var attempts = prepared.attempts();
        var runtime = new DocumentPublicationScopeCalls();
        boolean transferred = false;
        HistoricalPublicationLosingSuccessorProbe losing = null;
        Throwable primary = null;
        try {
            DocumentAssessmentStartJournal.Started started;
            DocumentOperationUploadAdmission.Admission admitted;
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                request.attachSources(sources, accepted, RepositoryReadControl.NONE); transferred = true;
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                admitted = request.admitUploads(RepositoryReadControl.NONE);
                started = request.start(check == Check.EXPIRED ? Duration.ofSeconds(20) : Duration.ofMinutes(1), RepositoryReadControl.NONE);
                request.prepareAssessment(policy, Map.of("a", fragments), container, resolver, limits,
                        Instant.now(), RepositoryReadControl.NONE);
            }
            require(runtime.isIdle(), "first client call releases runtime barrier while assessment stays retained");
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, check == Check.OVERLAP ? 2 : 1)), "entry survives first call");
            sources.close(); // Subsequent requests must use the retained Work and assessment.
            var owner = tx.inTransaction(em -> {
                var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), command.sha256(),
                        plan.reservation().predecessor().epoch()+1, plan.reservation().successorToken());
                return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration()+1,
                        plan.next().seeds().ownerNonce(), Optional.of(claim));
            });
            require(admitted.attempts().size() == 1, "mixed owner admits exactly one upload");
            var upload = admitted.attempts().getFirst();
            require(upload.id().equals(plan.next().seeds().attempts().get("a"))
                    && !upload.id().equals(original.seeds().attempts().get("a")), "fresh attempt belongs to successor");
            require(upload.token().equals(plan.next().seeds().uploadTokens().get("a"))
                    && !upload.token().equals(original.seeds().uploadTokens().get("a")), "fresh token belongs to successor");
            var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, upload.id(), upload.token());
            var selections = Map.of("a", selected);
            DocumentAssessmentCreation.Created created = null;
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                require(request.start(check == Check.EXPIRED ? Duration.ofSeconds(20) : Duration.ofMinutes(1), RepositoryReadControl.NONE).equals(started), "START identity survives call boundary");
                try {
                    request.prepareAssessment(policy, Map.of("a", fragments), container, resolver, limits,
                            Instant.now(), RepositoryReadControl.NONE);
                    throw new AssertionError("Retained assessment was replaced");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.CONFLICT, "assessment replacement refused");
                }
                var physical = request.withAssessment(assessment -> assessment.preparePhysical(plan.next().placements(),
                        plan.next().seeds().attempts(), plan.next().lease(), plan.next().seeds().uploadTokens(),
                        RepositoryReadControl.NONE), RepositoryReadControl.NONE);
                var member = physical.plan().members().getFirst();
                var measured = member.attempt().orElseThrow().uploads().stream().map(part -> {
                    var object = part.object();
                    var actual = DocumentPartTransfer.upload(provider.store(), member.placement().drive().namespace(), object,
                            fragments.get(part.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), actual.version(), actual.etag());
                }).toList();
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
                if (fault == null) {
                    created = request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started,
                            RepositoryReadControl.NONE);
                } else {
                    fault.arm(started.assessment());
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Installed owner CREATE did not lose its reply");
                    } catch (RuntimeException failure) { fault.requireFailure(failure); }
                }
            }
            require(runtime.isIdle(), "CREATE request releases runtime barrier");
            if (fault != null) require(count(tx, "document_assessment_owners", command.operationId()) == 1,
                    "lost CREATE reply leaves exactly one committed assessment");
            if (check == Check.REVOKED) {
                require(fault != null, "revocation checks an uncertain committed CREATE");
                new RepositoryCredentialAuthorities(tx).revoke(coordinator, caller.credentialBinding().orElseThrow(), caller.principalName());
                try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                    try {
                        request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE);
                        throw new AssertionError("Revoked caller adopted an assessment");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.UNAUTHENTICATED,
                                "revoked credential must refuse reconciliation as unauthenticated: " + refused.code());
                    }
                }
                require(count(tx, "document_revision_commits", command.operationId()) == 0, "revoked reconciliation publishes nothing");
                System.out.println("HISTORICAL_RECONCILIATION_REVOKED_OK");
                return;
            }
            if (check == Check.EXPIRED) {
                require(fault != null, "expiry checks an uncertain committed CREATE");
                HistoricalReconciliationExpiryProbe.run(database, started, () -> {
                    try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                        request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE);
                    }
                });
                require(count(tx, "document_revision_commits", command.operationId()) == 0, "expired reconciliation publishes nothing");
                System.out.println("HISTORICAL_RECONCILIATION_EXPIRED_OK");
                require(tx.inTransaction(em -> (Boolean) em.createNativeQuery(
                        "SELECT release_expired_document_assessment(:account,:principal,:operation,:assessment)")
                        .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                        .setParameter("operation", command.operationId()).setParameter("assessment", started.assessment())
                        .getSingleResult()), "real recovery releases the expired assessment");
                require(count(tx, "document_assessment_owners", command.operationId()) == 0,
                        "released assessment is no longer retained");
                try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                    require(request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE).isEmpty(),
                            "released stage cannot be adopted");
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Released assessment permitted another CREATE");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                                && refused.getMessage().contains("requires reconciliation"),
                                "release must preserve the original sticky CREATE boundary");
                    }
                }
                require(count(tx, "document_assessment_owners", command.operationId()) == 0
                        && count(tx, "document_revision_commits", command.operationId()) == 0,
                        "release reconciliation recreates and publishes nothing");
                System.out.println("HISTORICAL_RECONCILIATION_RELEASED_OK");
                return;
            }
            if (fault != null) {
                try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Lost CREATE reply permitted another CREATE");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION, "uncertain CREATE stays sticky across calls");
                    }
                    try {
                        request.reconcileAssessment(Map.of(), observation, RepositoryReadControl.NONE);
                        throw new AssertionError("Reconciliation accepted changed upload selections");
                    } catch (IllegalArgumentException refused) {
                        require(refused.getMessage().contains("selections differ"), "exact CREATE selections required");
                    }
                    created = request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE).orElseThrow();
                    require(created.assessment().equals(started.assessment()) && created.retainUntil().equals(started.retainUntil()),
                            "reconciled stage has original acknowledged START coordinates");
                }
                require(runtime.isIdle(), "reconciliation request releases runtime barrier");
            }
            var found = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
            require(found.stage().equals(created), "separate request CREATE has exact persisted identity");
            ai.protomolt.proto.repo.v1.DocumentPublicationResult result;
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                if (check == Check.CLAIM_EXPIRES) {
                    HistoricalPublicationClaimExpiryProbe.run(database, command, created, tx, coordinator, owner, plan,
                            publicationTx -> request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                                    new DocumentPublicationCommit(publicationTx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE));
                    require(request.retireFenced(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                            == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "expired publisher retires after confirmed takeover");
                    require(budget.reservedBytes() == before, "expired publisher returns retained bytes");
                    System.out.println("SCOPED_HISTORICAL_EXPIRED_PUBLISHER_RETIRED_OK");
                    return;
                } else if (check.commitWinner()) {
                    losing = new HistoricalPublicationLosingSuccessorProbe(tx, attempts, caller, coordinator, command,
                            request.identity(), fragments, accepted.fork());
                    result = HistoricalPublicationCommitWinnerProbe.run(database, tx, coordinator, owner, plan,
                            publicationTx -> request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                                    new DocumentPublicationCommit(publicationTx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE), losing::selectWhilePublicationWaits);
                } else {
                    result = request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                }
            }
            require(runtime.isIdle(), "publication request releases runtime barrier");
            HistoricalClaimedMixedPublicationProbe.verify(tx, provider, caller, command, owner, selections, fragments, result);
            if (prepared.overlap() != null) prepared.overlap().verifyAndRetire();
            if (losing != null) {
                losing.verifyAndRetire(check == Check.COMMIT_WINS_OLD_FIRST);
                require(budget.reservedBytes() == before, "both generation byte reservations returned");
                require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                        "receipt preserved after losing proposal and publisher retirement");
                System.out.println("SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
                return;
            }
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                try {
                    request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                    throw new AssertionError("A later call repeated publication");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && refused.getMessage().contains("reconciliation"), "sticky publication flag survives call boundary");
                }
            }
            require(runtime.isIdle(), "repeat refusal releases runtime barrier");
            require(count(tx, "repository_publication_assessment_starts", command.operationId()) == 2,
                    "original and successor START only");
            require(count(tx, "document_assessment_owners", command.operationId()) == 1, "exactly one CREATE");
            require(count(tx, "document_revision_commits", command.operationId()) == 1, "exactly one publication");
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                require(request.retireTerminal(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                        == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED,
                        "committed publication retires its exact retained owner during normal operation");
            }
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 0)),
                    "retirement removes entry and decrements the borrowed call once");
            require(budget.reservedBytes() == before, "normal retirement releases owned byte reservations before shutdown");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                    "authorized committed receipt remains replayable after local resource retirement");
            System.out.println("SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK");
        } catch (Exception | Error failure) {
            primary = failure; throw failure;
        } finally {
            try {
                if (losing != null) losing.close();
                if (prepared.overlap() != null) prepared.overlap().releaseWorker();
                runtime.close(); attempts.close();
                require(runtime.awaitIdle(Duration.ofSeconds(1)), "client requests drained before owner shutdown");
                require(attempts.detachClosed(Duration.ofSeconds(1), ignored -> coordinator, RepositoryReadControl.NONE),
                        "retained owner closes assessment and execution then drains its exact capture");
                if (transferred) require(budget.reservedBytes() == before, "owner returns every retained byte reservation");
            } catch (Exception | Error cleanup) {
                if (primary == null) throw cleanup;
                if (primary != cleanup) primary.addSuppressed(cleanup);
            }
        }
        if (check == Check.SELF_SUPERSESSION) System.out.println("SCOPED_HISTORICAL_SELF_SUPERSESSION_PUBLICATION_OK");
        System.out.println(fault == null ? "SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK"
                : "SCOPED_INSTALLED_HISTORICAL_CREATE_RECONCILED_PUBLICATION_OK");
    }
    private static long count(Tx tx, String table, UUID operation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", operation).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
