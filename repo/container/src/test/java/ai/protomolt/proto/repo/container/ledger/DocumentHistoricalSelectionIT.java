package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.UnknownFieldSet;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL retention/authorization; synthetic provider observations, no restore publication proof. */
@Testcontainers
class DocumentHistoricalSelectionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("reader", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void physicalBinderCarriesHistoricalObjectsAndTransactionProofAlongsideCurrentReuseOrUploads(boolean upload) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var original = publish(c, f, Fault.NONE, em -> {});
            advance(c, f, advance(c, f, original));
            var row = new DocumentLedger(c.tx()).findByNodeId(f.sources().getFirst().row().nodeId).orElseThrow();
            var member = f.command().intent().getMembers(0).toBuilder();
            var condition = member.getDestination().toBuilder().setExpectedMutationRevision(row.mutationRevision).build();
            member.setDestination(condition);
            for (int i = 0; i < member.getPartsCount(); i++) member.setParts(i, member.getParts(i).toBuilder()
                    .setReuse(member.getParts(i).getReuse().toBuilder().setSource(condition)));
            if (upload) {
                var object = member.getParts(1).getReuse().getObject();
                member.setParts(1, member.getParts(1).toBuilder().setUpload(PublicationUpload.newBuilder()
                        .setSizeBytes(object.getSizeBytes()).setSha256(object.getSha256()).setContentType(object.getContentType())));
            }
            // Admit an ordinary operation to exercise its real physical selection
            // rows. Historical command admission and CREATE remain independently gated.
            var ordinary = new DocumentPublicationCommand(f.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).setMembers(0, member).build());
            var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", ordinary.operationId()),
                    ordinary, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            var drive = new DriveLedger(c.tx()).findById(UUID.fromString(member.getDriveId())).orElseThrow();
            var placements = Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test", new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow()));
            Map<String, UUID> attempts = upload ? Map.of("member-0", UUID.randomUUID()) : Map.of();
            var caller = new RepositoryCaller("principal", true);
            var admitted = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(caller, owner,
                    DocumentOperationUploadAdmission.prepare(ordinary, placements, attempts, Duration.ofMinutes(5)));
            var selected = new java.util.HashMap<String, DocumentSelectedAttemptLedger.Selected>();
            if (upload) {
                var attempt = admitted.getFirst();
                var selection = new DocumentSelectedAttemptLedger.Selected("member-0", 1, attempt.id(), attempt.token());
                selected.put("member-0", selection);
                var declared = DocumentUploadPlan.prepare(ordinary, placements, attempts).members().getFirst().attempt().orElseThrow().uploads().getFirst().object();
                new DocumentSelectedAttemptLedger(c.tx()).verifyBatch(owner, selection, List.of(new DocumentSelectedAttemptLedger.Observation(
                        declared.objectKey(), declared.size(), declared.sha256(), declared.contentType(), "new-version", "observed-fixture")));
            }
            var historical = selector(f, original, 0, 0);
            var command = new DocumentPublicationCommand(ordinary.intent().toBuilder().setMembers(0, member
                    .setParts(0, member.getParts(0).toBuilder().setHistoricalReuse(historical))).build());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(caller, historical.getSource(), UUID.fromString(historical.getRevisionId()));
            try (var use = history.use()) {
                var reference = DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(historical), NONE);
                var refs = List.of(reference);
                var plan = DocumentUploadPlan.prepare(command, placements, attempts, refs, () -> {});
                var reuse = DocumentReuseAdmission.prepare(plan);
                var authorization = DocumentAdmissionAuthorization.prepare(plan, refs);
                var slotPlan = DocumentAssessmentSlots.prepare(command, refs, () -> {});
                var bound = c.tx().inTransaction(em -> {
                    RepositoryOperationLedger.fenceLiveOwner(em, owner);
                    DocumentSchemaPolicies.lockUnboundWriter(em, "account");
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization);
                    plan.members().getFirst().placement().drive().lock(em, new DriveLedger(c.tx()));
                    var result = DocumentCommitParts.bindHistoricalAssessment(em, owner, plan, selected, reuse, () -> {});
                    var slots = DocumentAssessmentSlots.bind(em, slotPlan, result.physical(), result.locks(), () -> {});
                    assertThat(slots).hasSize(2);
                    assertThat(slots.getFirst().sourceRevision().toString()).isEqualTo(historical.getRevisionId());
                    assertThat(slots.get(1).declaration()).isEqualTo(upload ? "NEW_CONTENT" : "REUSE");
                    assertThat(result.physical().parts().get(new DocumentCommitParts.Slot("member-0", 0)).id().toString())
                            .isEqualTo(historical.getObject().getObjectId());
                    return result;
                });
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentAssessmentSlots.bind(em, slotPlan, bound.physical(), bound.locks(), () -> {});
                })).isInstanceOf(IllegalStateException.class).hasMessageContaining("another transaction");
                assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, placements, attempts))
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentCommitParts.bindAssessment(em, owner, plan, selected, reuse, () -> {});
                })).isInstanceOf(UnsupportedOperationException.class);
                use.close();
                assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, placements, attempts, refs, () -> {}))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentCommitParts.bindHistoricalAssessment(em, owner, plan, selected, reuse, () -> {});
                })).isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
            } finally { release(ledger, history); }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"valid", "missing-source", "extra-source", "closed-use", "physical-key", "missing-target", "unlocked", "cancelled"})
    void bindsPinnedHistoricalSourceToEveryDestinationSlot(String fault) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 2); var published = publish(c, f, Fault.NONE, em -> {});
            var selected = selector(f, published, 0, 0);
            var intent = f.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString());
            for (int i = 0; i < intent.getMembersCount(); i++) {
                var member = intent.getMembers(i);
                intent.setMembers(i, member.toBuilder().clearParts().addParts(DocumentPublicationPart.newBuilder()
                        .setSlot(selected.getSourceSlot()).setHistoricalReuse(selected)));
            }
            var command = new DocumentPublicationCommand(intent.build());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, selected.getSource(), UUID.fromString(selected.getRevisionId()));
            try (var use = history.use()) {
                var reference = DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(selected), NONE);
                var extra = DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(selector(f, published, 1, 1)), NONE);
                if (fault.equals("missing-source") || fault.equals("extra-source")) {
                    assertThatThrownBy(() -> DocumentAssessmentSlots.prepare(command,
                            fault.equals("missing-source") ? List.of() : List.of(reference, extra), () -> {}))
                            .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                } else {
                    var prepared = DocumentAssessmentSlots.prepare(command, List.of(reference), () -> {});
                    var object = selected.getObject();
                    var physical = new DocumentCommitParts.Physical(UUID.fromString(object.getObjectId()),
                            selected.getSourceSlot().getPartValue(), selected.getSourceSlot().getSubKey(),
                            fault.equals("physical-key") ? "different-key" : object.getObjectKey(), object.getSizeBytes(),
                            object.getSha256(), object.getContentType(), object.hasProviderVersion() ? object.getProviderVersion() : null,
                            null, object.getBackendGeneration(), object.getStorageRealm(), object.getNamespace());
                    var parts = new java.util.HashMap<DocumentCommitParts.Slot, DocumentCommitParts.Physical>();
                    for (var member : command.intent().getMembersList()) parts.put(new DocumentCommitParts.Slot(member.getMemberId(), 0), physical);
                    if (fault.equals("missing-target")) parts.remove(new DocumentCommitParts.Slot("member-1", 0));
                    var bound = new DocumentCommitParts.Bound(parts, Map.of("member-0", 1L, "member-1", 1L));
                    if (fault.equals("closed-use")) use.close();
                    org.assertj.core.api.ThrowableAssert.ThrowingCallable bind = () -> c.tx().inTransaction(em -> {
                        DocumentAdmissionAuthorization.authorizeReplay(em, ADMIN, command);
                        var nodes = f.sources().stream().map(source -> source.row().nodeId).collect(java.util.stream.Collectors.toSet());
                        var locks = DocumentPublicationLocks.lockIndependentOrigins(em, nodes, Set.of(physical.id()), Set.of());
                        if (!fault.equals("unlocked")) DocumentPublicationLocks.lockIndependentRetention(em, locks);
                        var slots = DocumentAssessmentSlots.bind(em, prepared, bound, locks, () -> {
                            if (fault.equals("cancelled")) throw new java.util.concurrent.CancellationException("cancelled historical binding");
                        });
                        assertThat(slots).hasSize(2);
                        assertThat(slots).allSatisfy(slot -> {
                            assertThat(slot.declaration()).isEqualTo("HISTORICAL_REUSE");
                            assertThat(slot.sourceNode()).isEqualTo(DocumentIds.nodeId(selected.getSource()));
                            assertThat(slot.sourceRevision()).isEqualTo(UUID.fromString(selected.getRevisionId()));
                            assertThat(slot.sourceOrdinal()).isZero();
                        });
                    });
                    if (fault.equals("valid")) assertThatCode(bind).doesNotThrowAnyException();
                    else if (fault.equals("closed-use")) assertThatThrownBy(bind)
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
                    else if (fault.equals("unlocked")) assertThatThrownBy(bind)
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("locked proposed object set");
                    else if (fault.equals("cancelled")) assertThatThrownBy(bind)
                            .isInstanceOf(java.util.concurrent.CancellationException.class).hasMessageContaining("cancelled historical binding");
                    else assertThatThrownBy(bind).isInstanceOf(DocumentPartAttemptLedger.FenceException.class)
                            .hasMessageContaining("Assessment slots differ");
                }
            } finally { release(ledger, history); }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"valid", "current-reuse", "non-native", "node", "revision", "ordinal", "object", "missing-node", "oversize-ordinal"})
    void stagingBindsExactHistoricalProvenanceWithoutRequiringCurrentRevision(String fault) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1);
            var original = publish(c, f, Fault.NONE, em -> {});
            advance(c, f, advance(c, f, original));
            var selected = selector(f, original, 0, 0);
            var fixture = new DocumentAssessmentRetentionFixture(c.tx());
            var candidate = fixture.reuseCandidate();
            UUID sourceNode = fault.equals("missing-node") || fault.equals("current-reuse") ? null
                    : fault.equals("node") ? UUID.randomUUID() : DocumentIds.nodeId(selected.getSource());
            UUID revision = fault.equals("revision") ? UUID.randomUUID()
                    : fault.equals("non-native") ? f.sources().getFirst().attempt() : UUID.fromString(selected.getRevisionId());
            UUID object = UUID.fromString(f.sources().getFirst().identities().get(fault.equals("object") ? 1 : 0).getObjectId());
            int ordinal = fault.equals("oversize-ordinal") ? 10000 : fault.equals("ordinal") ? 1 : 0;
            // Real SQL staging guard, not command-to-slot admission: the ordinary
            // envelope is intentionally independent while historical execution is gated.
            org.assertj.core.api.ThrowableAssert.ThrowingCallable stage = () -> c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, candidate.owner());
                fixture.insertOwner(em, candidate, 120, 1);
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,
                            selection_revision,object_id,declaration,source_node,source_revision,source_ordinal)
                        VALUES(:assessment,'member',0,1,:object,:declaration,:node,:revision,:ordinal)
                        """).setParameter("assessment", candidate.assessment()).setParameter("object", object)
                        .setParameter("declaration", fault.equals("current-reuse") ? "REUSE" : "HISTORICAL_REUSE")
                        .setParameter("node", sourceNode).setParameter("revision", revision).setParameter("ordinal", ordinal)
                        .executeUpdate();
                fixture.seal(em, candidate);
            });
            if (fault.equals("valid")) assertThatCode(stage).doesNotThrowAnyException();
            else assertThatThrownBy(stage).hasStackTraceContaining(
                    fault.equals("missing-node") || fault.equals("oversize-ordinal")
                            ? "document_assessment_slots_source_check" : "Assessment candidate differs");
            long expected = fault.equals("valid") ? 1 : 0;
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_owners WHERE assessment_id=:id")
                    .setParameter("id", candidate.assessment()).getSingleResult()).longValue())).isEqualTo(expected);
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                    .setParameter("id", candidate.assessment()).getSingleResult()).longValue())).isEqualTo(expected);
        }
    }

    @Test void selectsOriginalRevisionAfterTwoMorePublicationsAndPreservesTransferredPin() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1);
            var original = publish(c, f, Fault.NONE, em -> {});
            var second = advance(c, f, original);
            var third = advance(c, f, second);
            var selector = selector(f, original, 0, 0);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, selector.getSource(), UUID.fromString(selector.getRevisionId()));
            var initial = history.use(); var use = initial.transfer(); initial.close();
            try (use) {
                var selected = history.selectRetained(use, List.of(selector, selector(f, original, 1, 1)), NONE);
                assertThat(selected).hasSize(2);
                assertThat(selected.getFirst().objectId().toString()).isEqualTo(selector.getObject().getObjectId());
                assertThat(use.plan().revision().toString()).isEqualTo(original.getMembers(0).getRevisionId());
                assertThat(use.plan().publicationRevision()).isLessThan(third.getMembers(0).getMutationRevision());
                var references = DocumentHistoricalReferenceAdmission.prepare(history, use,
                        List.of(selector, selector(f, original, 1, 1)), NONE);
                var objects = selected.stream().map(DocumentHistoricalReadPlan.Entry::objectId)
                        .collect(java.util.stream.Collectors.toSet());
                c.tx().inTransaction(em -> {
                    DocumentAdmissionAuthorization.authorizeHistory(em, ADMIN, selector.getSource());
                    var locks = DocumentPublicationLocks.lockIndependentOrigins(em,
                            Set.of(DocumentIds.nodeId(selector.getSource())), objects, Set.of());
                    DocumentPublicationLocks.lockIndependentRetention(em, locks);
                    DocumentHistoricalReferenceAdmission.requireBoundSources(em, references, locks, NONE);
                });
                assertThat(c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                        "SELECT revision_id FROM document_revision_current WHERE node_id=:node")
                        .setParameter("node", f.sources().getFirst().row().nodeId).getSingleResult()).toString())
                        .isEqualTo(third.getMembers(0).getRevisionId());
                history.close();
                assertThat(history.isDrained()).isFalse();
                assertThatThrownBy(history::release).isInstanceOf(IllegalStateException.class);
                assertThat(count(c, "document_read_pins")).isEqualTo(2);
            }
            release(ledger, history);
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @ParameterizedTest @ValueSource(strings = {"unlocked", "omitted", "previous-transaction", "closed-use"})
    void historicalReferenceCheckRequiresLiveUseAndCompleteTransactionLocks(String fault) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var published = publish(c, f, Fault.NONE, em -> {});
            var selection = selector(f, published, 0, 0);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, selection.getSource(), UUID.fromString(selection.getRevisionId()));
            try (var use = history.use()) {
                var prepared = DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(selection), NONE);
                var nodes = Set.of(DocumentIds.nodeId(selection.getSource()));
                var objects = Set.of(UUID.fromString(selection.getObject().getObjectId()));
                DocumentPublicationLocks.IndependentOrigins previous = fault.equals("previous-transaction")
                        ? c.tx().inTransaction(em -> {
                            DocumentAdmissionAuthorization.authorizeHistory(em, ADMIN, selection.getSource());
                            var locks = DocumentPublicationLocks.lockIndependentOrigins(em, nodes, objects, Set.of());
                            DocumentPublicationLocks.lockIndependentRetention(em, locks);
                            return locks;
                        }) : null;
                if (fault.equals("closed-use")) use.close();
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentAdmissionAuthorization.authorizeHistory(em, ADMIN, selection.getSource());
                    var locks = previous != null ? previous : DocumentPublicationLocks.lockIndependentOrigins(em, nodes,
                            fault.equals("omitted") ? Set.of() : objects, Set.of());
                    if (previous == null && !fault.equals("unlocked"))
                        DocumentPublicationLocks.lockIndependentRetention(em, locks);
                    DocumentHistoricalReferenceAdmission.requireBoundSources(em, prepared, locks, NONE);
                })).isInstanceOf(IllegalStateException.class);
            }
            release(ledger, history);
        }
    }

    @Test void comparesEveryPhysicalCoordinateAndNeverFallsBackToCurrentOrAnotherSlot() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var published = publish(c, f, Fault.NONE, em -> {});
            var valid = selector(f, published, 0, 0);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, valid.getSource(), UUID.fromString(valid.getRevisionId()));
            try (var use = history.use()) {
                for (var wrong : List.of(
                        valid.toBuilder().setRevisionId(UUID.randomUUID().toString()).build(),
                        valid.toBuilder().setSource(valid.getSource().toBuilder().setDocId("different")).build(),
                        valid.toBuilder().setRevisionOrdinal(1).build(),
                        valid.toBuilder().setRevisionOrdinal(9999).build(),
                        valid.toBuilder().setSourceSlot(f.sources().getFirst().slots().get(1)).build()))
                    assertCode(() -> history.selectRetained(use, List.of(wrong), NONE), RepositoryException.Code.FAILED_PRECONDITION);
                var object = valid.getObject();
                for (var wrong : List.of(
                        object.toBuilder().setObjectId(UUID.randomUUID().toString()).build(),
                        object.toBuilder().setBackendGeneration("other").build(),
                        object.toBuilder().setStorageRealm("other").build(),
                        object.toBuilder().setNamespace("other").build(),
                        object.toBuilder().setObjectKey("other").build(),
                        object.toBuilder().setProviderVersion("other").build(),
                        object.toBuilder().clearProviderVersion().build(),
                        object.toBuilder().setSizeBytes(object.getSizeBytes() + 1).build(),
                        object.toBuilder().setSha256("b".repeat(64)).build(),
                        object.toBuilder().setContentType("text/plain").build()))
                    assertCode(() -> history.selectRetained(use, List.of(valid.toBuilder().setObject(wrong).build()), NONE),
                            RepositoryException.Code.FAILED_PRECONDITION);
                // A failed batch returns no partial successful selection and does not release the caller's pin.
                assertCode(() -> history.selectRetained(use, List.of(valid,
                        valid.toBuilder().setRevisionOrdinal(9999).build()), NONE), RepositoryException.Code.FAILED_PRECONDITION);
                assertThat(history.selectRetained(use, List.of(valid), NONE)).hasSize(1);
                assertThat(count(c, "document_read_pins")).isEqualTo(2);
            } finally { release(ledger, history); }
        }
    }

    @Test void fullManifestOrdinalsPreserveGapsAndVersionlessIdentity() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1, false, Duration.ofMinutes(5), true, null);
            var published = publish(c, f, Fault.NONE, em -> {});
            var core = selector(f, published, 0, 1); var chunk = selector(f, published, 1, 3);
            assertThat(core.getObject().hasProviderVersion()).isFalse();
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, core.getSource(), UUID.fromString(core.getRevisionId()));
            try (var use = history.use()) {
                assertThat(use.plan().entries()).extracting(DocumentHistoricalReadPlan.Entry::revisionOrdinal).containsExactly(1, 3);
                assertThat(history.selectRetained(use, List.of(chunk, core), NONE))
                        .extracting(DocumentHistoricalReadPlan.Entry::revisionOrdinal).containsExactly(3, 1);
                for (int gap : List.of(0, 2))
                    assertCode(() -> history.selectRetained(use, List.of(core.toBuilder().setRevisionOrdinal(gap).build()), NONE),
                            RepositoryException.Code.FAILED_PRECONDITION);
                assertCode(() -> history.selectRetained(use, List.of(core.toBuilder().setObject(
                        core.getObject().toBuilder().setProviderVersion("invented")).build()), NONE),
                        RepositoryException.Code.FAILED_PRECONDITION);
            } finally { release(ledger, history); }
        }
    }

    @Test void currentReadRevocationPrecedesBindingDetailsAndWrongAccountCannotCapture() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var published = publish(c, f, Fault.NONE, em -> {});
            var valid = selector(f, published, 0, 0); grant(c, valid.getSource(), true);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var wrong = new RepositoryCaller("reader", false, Set.of("other"), Set.of());
            assertCode(() -> ledger.captureHistorical(wrong, valid.getSource(), UUID.fromString(valid.getRevisionId())),
                    RepositoryException.Code.NOT_FOUND);
            var caller = new RepositoryCaller("reader", false, Set.of("account"), Set.of());
            var history = ledger.captureHistorical(caller, valid.getSource(), UUID.fromString(valid.getRevisionId()));
            try (var use = history.use()) {
                assertThat(history.selectRetained(use, List.of(valid), NONE)).hasSize(1);
                grant(c, valid.getSource(), false);
                assertCode(() -> history.selectRetained(use, List.of(valid), NONE), RepositoryException.Code.NOT_FOUND);
                assertCode(() -> history.selectRetained(use, List.of(valid.toBuilder().setRevisionOrdinal(9999).build()), NONE),
                        RepositoryException.Code.NOT_FOUND);
                assertThat(count(c, "document_read_pins")).isEqualTo(2);
                assertCode(() -> ledger.captureHistorical(caller, valid.getSource(), UUID.fromString(valid.getRevisionId())),
                        RepositoryException.Code.NOT_FOUND);
            } finally { release(ledger, history); }
        }
    }

    @Test void closedOrForeignUsesAndMalformedSelectorsAreRefused() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var published = publish(c, f, Fault.NONE, em -> {});
            var valid = selector(f, published, 0, 0);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, valid.getSource(), UUID.fromString(valid.getRevisionId()));
            var other = ledger.captureHistorical(ADMIN, valid.getSource(), UUID.fromString(valid.getRevisionId()));
            try (var use = history.use(); var foreign = other.use()) {
                assertThatThrownBy(() -> history.selectRetained(foreign, List.of(valid), NONE))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another historical capture");
                assertThatThrownBy(() -> history.selectRetained(use, List.of(valid.toBuilder().setRevisionId("latest").build()), NONE))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid historical selector");
                var unknown = valid.toBuilder().setObject(valid.getObject().toBuilder().setUnknownFields(
                        UnknownFieldSet.newBuilder().addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())).build();
                assertThatThrownBy(() -> history.selectRetained(use, List.of(unknown), NONE))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown historical selector");
                assertThatThrownBy(() -> history.selectRetained(use, List.of(), NONE)).isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> history.selectRetained(use, java.util.Collections.nCopies(10001, valid), NONE))
                        .isInstanceOf(IllegalArgumentException.class);
                var huge = valid.toBuilder().setSource(valid.getSource().toBuilder().setDocId("x".repeat(1_048_577))).build();
                assertThatThrownBy(() -> history.selectRetained(use, List.of(huge), NONE))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("byte bound");
                use.close();
                assertThatThrownBy(() -> history.selectRetained(use, List.of(valid), NONE))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
            } finally {
                other.close(); assertThat(other.awaitDrained(Duration.ofSeconds(1))).isTrue(); other.release();
                release(ledger, history);
            }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"cancel", "deadline", "close"})
    void stoppedUseCannotExposeSelectionAfterRealAuthorizationLockWait(String action) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1); var published = publish(c, f, Fault.NONE, em -> {});
            var valid = selector(f, published, 0, 0);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, valid.getSource(), UUID.fromString(valid.getRevisionId()));
            var stopped = new AtomicBoolean();
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return action.equals("cancel") && stopped.get(); }
                public long remainingNanos() { return action.equals("deadline") && stopped.get() ? 0 : Long.MAX_VALUE; }
            };
            try (var use = history.use(); var blocker = c.emf().createEntityManager();
                    var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                blocker.getTransaction().begin();
                try {
                    blocker.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:node FOR UPDATE")
                            .setParameter("node", DocumentIds.nodeId(valid.getSource())).getSingleResult();
                    int pid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                    var selection = executor.submit(() -> history.selectRetained(use, List.of(valid), control));
                    long end = System.nanoTime() + Duration.ofSeconds(5).toNanos(); boolean waiting = false;
                    while (System.nanoTime() < end && !selection.isDone()) {
                        waiting = c.tx().readOnly(em -> !em.createNativeQuery(
                                "SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                                .setParameter("blocker", pid).getResultList().isEmpty());
                        if (waiting) break;
                        Thread.sleep(10);
                    }
                    assertThat(waiting).as("selection reached actual document lock").isTrue();
                    stopped.set(true); if (action.equals("close")) use.close();
                    blocker.getTransaction().rollback();
                    assertThatThrownBy(() -> selection.get(5, TimeUnit.SECONDS)).hasStackTraceContaining(
                            action.equals("close") ? "use has ended" : action.equals("deadline")
                                    ? "Document read deadline exceeded" : "Document read cancelled");
                } finally { if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback(); }
            } finally { release(ledger, history); }
        }
    }

    private static PublicationHistoricalReuse selector(Prepared f, DocumentPublicationResult result, int part, int ordinal) {
        return PublicationHistoricalReuse.newBuilder().setSource(result.getMembers(0).getAddress())
                .setRevisionId(result.getMembers(0).getRevisionId()).setRevisionOrdinal(ordinal)
                .setSourceSlot(f.sources().getFirst().slots().get(part)).setObject(f.sources().getFirst().identities().get(part)).build();
    }
    private static DocumentPublicationResult advance(Context c, Prepared original, DocumentPublicationResult prior) {
        var row = new DocumentLedger(c.tx()).findByNodeId(original.sources().getFirst().row().nodeId).orElseThrow();
        var m = original.command().intent().getMembers(0).toBuilder();
        var condition = m.getDestination().toBuilder().setExpectedMutationRevision(row.mutationRevision).build();
        m.setDestination(condition);
        for (int i = 0; i < m.getPartsCount(); i++) m.setParts(i, m.getParts(i).toBuilder()
                .setReuse(m.getParts(i).getReuse().toBuilder().setSource(condition)));
        var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).setMembers(0, m).build());
        var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
        var drive = new DriveLedger(c.tx()).findById(UUID.fromString(m.getDriveId())).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        var plan = DocumentOperationUploadAdmission.prepare(command,
                Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test", profile)),
                Map.of(), Duration.ofMinutes(5));
        new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx()))
                .admit(new RepositoryCaller("principal", true), owner, plan);
        var source = original.sources().getFirst();
        return publish(c, new Prepared(command, owner, List.of(new ManagedDocumentFixture(row,
                UUID.fromString(prior.getMembers(0).getRevisionId()), source.slots(), source.identities())), Map.of()), Fault.NONE, em -> {});
    }
    private static void grant(Context c, NodeAddress address, boolean allowed) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("node", DocumentIds.nodeId(address)).setParameter("policy", allowed
                        ? "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}" : "{}")
                .executeUpdate(); });
    }
    private static void release(DocumentReadLedger ledger, DocumentReadLedger.PinnedHistory history) throws Exception {
        history.close(); assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue(); history.release();
        ledger.fence(); ledger.attestLocalQuiescence();
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, RepositoryException.Code code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(code));
    }
}
