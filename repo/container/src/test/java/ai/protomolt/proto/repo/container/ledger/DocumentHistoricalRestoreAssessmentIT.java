package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL/descriptor assessment. Provider observations come from the explicit retention fixture. */
@Testcontainers
class DocumentHistoricalRestoreAssessmentIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("reader", true);
    private static final ByteString COMMAND = ByteString.copyFrom(new byte[32]);
    private static final Instant AT = Instant.parse("2026-10-05T00:00:00Z");

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"valid", "missing-source", "closed-source", "close-during-copy",
            "later-hash", "later-size", "missing-member", "capacity", "cancel-during-copy"})
    void wholeCommandFragmentCaptureChecksHistoricalPinsAndEveryMemberBeforeResolution(String fault) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision());
            var historical = member(f, history);
            var ordinary = f.original().command().intent().getMembers(0).toBuilder().setMemberId("upload");
            ordinary.setDestination(ordinary.getDestination().toBuilder().setAddress(
                    ordinary.getDestination().getAddress().toBuilder().setGraphAddressId("other-node")));
            var command = new DocumentPublicationCommand(f.original().command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).clearMembers().addMembers(historical).addMembers(ordinary).build());
            var budget = new PayloadBudget(fault.equals("capacity") ? 1 : 32L * 1024 * 1024);
            try (var use = history.use()) {
                var selectors = historical.getPartsList().stream().filter(DocumentPublicationPart::hasHistoricalReuse)
                        .map(DocumentPublicationPart::getHistoricalReuse).toList();
                var reference = DocumentHistoricalReferenceAdmission.prepare(history, use, selectors, RepositoryReadControl.NONE);
                var references = fault.equals("missing-source") ? List.<DocumentHistoricalReferenceAdmission.Prepared>of() : List.of(reference);
                var later = new HashMap<>(f.fragments());
                int ordinal = later.keySet().iterator().next();
                if (fault.equals("later-hash")) later.put(ordinal, ByteString.copyFrom(new byte[later.get(ordinal).size()]));
                if (fault.equals("later-size")) later.put(ordinal, later.get(ordinal).concat(ByteString.copyFromUtf8("extra")));
                var mutable = new HashMap<Integer, ByteString>(); var buffers = new ArrayList<byte[]>();
                f.fragments().forEach((index, bytes) -> {
                    byte[] buffer = bytes.toByteArray(); buffers.add(buffer); mutable.put(index, UnsafeByteOperations.unsafeWrap(buffer));
                });
                var supplied = new HashMap<String, Map<Integer, ByteString>>();
                supplied.put("member", mutable);
                if (!fault.equals("missing-member")) supplied.put("upload", later);
                if (fault.equals("closed-source")) use.close();
                Runnable control = () -> {
                    if (budget.reservedBytes() > 0) {
                        if (fault.equals("close-during-copy")) use.close();
                        if (fault.equals("cancel-during-copy")) throw new java.util.concurrent.CancellationException("capture cancelled");
                    }
                };
                assertThatThrownBy(() -> DocumentPublicationFragments.capture(command, supplied, budget, control))
                        .isInstanceOf(UnsupportedOperationException.class);
                if (fault.equals("valid")) {
                    try (var snapshot = DocumentPublicationFragments.captureHistorical(command, supplied, references, budget, control)) {
                        buffers.forEach(buffer -> Arrays.fill(buffer, (byte) 0)); mutable.clear(); supplied.clear();
                        assertThat(snapshot.fragments()).containsOnlyKeys("member", "upload");
                        assertThat(snapshot.fragments().get("member")).isEqualTo(f.fragments());
                        assertThat(snapshot.fragments().get("upload")).isEqualTo(f.fragments());
                        assertThat(budget.reservedBytes()).isEqualTo(2L * f.fragments().values().stream().mapToLong(ByteString::size).sum());
                        var limits = new ai.protomolt.proto.repo.codec.DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 100_000);
                        for (var id : List.of("member", "upload")) {
                            var raw = DocumentCommandContent.checkHistorical(command, id, snapshot.fragments().get(id),
                                    false, limits, references, control);
                            assertThat(raw.assembly().document().getStructuredData()).isEqualTo(
                                    f.original().content().assembly().document().getStructuredData());
                            assertThat(raw.structuredResolution().orElseThrow().getNotAttempted()).isTrue();
                            assertThatThrownBy(() -> DocumentCommandContent.checkHistorical(command, id,
                                    snapshot.fragments().get(id), true, limits, references, control))
                                    .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("Typed content");
                        }
                    }
                    assertThat(use.plan().revision()).isEqualTo(f.revision());
                } else {
                    var refused = assertThatThrownBy(() -> DocumentPublicationFragments.captureHistorical(
                            command, supplied, references, budget, control));
                    switch (fault) {
                        case "missing-source" -> refused.isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                        case "closed-source", "close-during-copy" -> refused.isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
                        case "later-hash" -> refused.hasMessageContaining("hash differs");
                        case "later-size" -> refused.hasMessageContaining("size or ordinal differs");
                        case "missing-member" -> refused.hasMessageContaining("members differ");
                        case "capacity" -> refused.isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                        case "cancel-during-copy" -> refused.isInstanceOf(java.util.concurrent.CancellationException.class);
                        default -> throw new AssertionError(fault);
                    }
                }
                assertThat(budget.reservedBytes()).isZero();
            } finally { release(ledger, history); }
        }
    }

    @Test void ownsCopiedFragmentsAndPinAndBindsNewPolicyCommandAndTime() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision());
            var member = member(f, history); var budget = new PayloadBudget(32L * 1024 * 1024);
            var current = DocumentAdmissionPolicy.of(f.original().batch().policy().policy().definition().toBuilder()
                    .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).build(), () -> {});
            var fragments = new HashMap<Integer, ByteString>(); var buffers = new ArrayList<byte[]>();
            f.fragments().forEach((ordinal, bytes) -> { var buffer = bytes.toByteArray(); buffers.add(buffer);
                fragments.put(ordinal, UnsafeByteOperations.unsafeWrap(buffer)); });
            try (var result = history.assessRestore(member, current, COMMAND, fragments, AT, budget, RepositoryReadControl.NONE)) {
                buffers.forEach(buffer -> Arrays.fill(buffer, (byte) 0)); fragments.clear();
                var view = result.view(RepositoryReadControl.NONE);
                assertThat(view.failure()).isEmpty();
                assertThat(view.request().fragments()).isEqualTo(f.fragments());
                assertThat(view.request().commandSha256()).isEqualTo(COMMAND);
                assertThat(view.request().policySha256()).isEqualTo(current.sha256())
                        .isNotEqualTo(f.original().batch().policy().policy().sha256());
                assertThat(view.evaluatedAt()).isEqualTo(AT);
                assertThat(budget.reservedBytes()).isPositive();
                history.close(); assertThat(history.isDrained()).isFalse();
            }
            assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @Test void refusesCurrentPolicyMismatchAndBadFragmentsWithoutLeaking() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision());
            var member = member(f, history); var budget = new PayloadBudget(32L * 1024 * 1024);
            var denied = DocumentAdmissionPolicy.of(f.original().batch().policy().policy().definition().toBuilder()
                    .setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(DocumentSchemaPolicyBinding.newBuilder()
                            .setTypeUrl("type.test/google.protobuf.Timestamp").setSchema(PublicationSchemaCondition.newBuilder()
                                    .setTypeName("google.protobuf.Timestamp").setDescriptorFingerprint("0".repeat(64)))))
                    .build(), () -> {});
            assertThatThrownBy(() -> history.assessRestore(member, denied, COMMAND, f.fragments(), AT, budget, RepositoryReadControl.NONE))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not eligible");
            assertThat(budget.reservedBytes()).isZero();
            var wrong = new HashMap<>(f.fragments()); var ordinal = wrong.keySet().iterator().next();
            wrong.put(ordinal, ByteString.copyFrom(new byte[wrong.get(ordinal).size()]));
            assertThatThrownBy(() -> history.assessRestore(member, f.original().batch().policy().policy(), COMMAND,
                    wrong, AT, budget, RepositoryReadControl.NONE)).hasMessageContaining("hash differs");
            assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @Test void capacityAndCancellationReleasePinAndEveryReservation() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision()); var member = member(f, history);
            for (boolean cancel : List.of(false, true)) {
                var budget = new PayloadBudget(cancel ? 32L * 1024 * 1024 : 1);
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancel && budget.reservedBytes() > 0; }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> history.assessRestore(member, f.original().batch().policy().policy(), COMMAND,
                        f.fragments(), AT, budget, control)).isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(cancel ? RepositoryException.Code.CANCELLED
                                        : RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(budget.reservedBytes()).isZero();
            }
            release(ledger, history);
        }
    }

    @Test void scopedReadRevocationPreventsFurtherExposure() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); grant(c, f.address(), true);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(new RepositoryCaller("scoped", false, Set.of("account"), Set.of()), f.address(), f.revision());
            var budget = new PayloadBudget(32L * 1024 * 1024);
            try (var result = history.assessRestore(member(f, history), f.original().batch().policy().policy(), COMMAND,
                    f.fragments(), AT, budget, RepositoryReadControl.NONE)) {
                assertThat(result.view(RepositoryReadControl.NONE).failure()).isEmpty();
                grant(c, f.address(), false);
                assertThatThrownBy(() -> result.view(RepositoryReadControl.NONE)).isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            }
            assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @Test void alteredRootColumnsFromRealSqlSnapshotAreRejected() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c);
            var snapshot = c.tx().inTransaction(em -> { return DocumentHistoricalSchemaRows.capture(em,
                    f.address(), f.revision(), () -> {}); });
            var binding = DocumentHistoricalSchemaBinding.read(f.address(), snapshot, () -> {});
            var root = snapshot.roots().getFirst();
            // Alter captured evidence, without disabling the database's immutable-history guards.
            for (var changed : List.of(
                    new DocumentHistoricalSchemaRows.Root(root.ordinal(), "0".repeat(64), root.fragmentSha(), root.fragmentSize(), root.evidence()),
                    new DocumentHistoricalSchemaRows.Root(root.ordinal(), root.locatorSha(), "0".repeat(64), root.fragmentSize(), root.evidence()),
                    new DocumentHistoricalSchemaRows.Root(root.ordinal(), root.locatorSha(), root.fragmentSha(), root.fragmentSize() + 1, root.evidence()))) {
                var roots = new ArrayList<>(snapshot.roots()); roots.set(0, changed);
                var corrupt = new DocumentHistoricalSchemaRows.Snapshot(snapshot.header(), snapshot.references(), roots, snapshot.artifacts());
                var budget = new PayloadBudget(32L * 1024 * 1024);
                assertThatThrownBy(() -> DocumentHistoricalSchemaResolution.requireRootBindings(corrupt, binding,
                        List.of(), budget, RepositoryReadControl.NONE)).isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @Test void rejectsMixedContentCrossAccountAndSlotOrObjectSubstitutionBeforeAssessment() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision()); var member = member(f, history);
            var first = member.getParts(0); var budget = new PayloadBudget(32L * 1024 * 1024);
            for (var invalid : List.of(
                    member.toBuilder().setParts(0, f.original().command().intent().getMembers(0).getParts(0)).build(),
                    member.toBuilder().setDestination(member.getDestination().toBuilder().setAddress(
                            member.getDestination().getAddress().toBuilder().setAccountId("other"))).build(),
                    member.toBuilder().setParts(0, first.toBuilder().setSlot(first.getSlot().toBuilder().setSubKey("wrong"))).build(),
                    member.toBuilder().setParts(0, first.toBuilder().setHistoricalReuse(first.getHistoricalReuse().toBuilder()
                            .setObject(first.getHistoricalReuse().getObject().toBuilder().setObjectId(UUID.randomUUID().toString())))).build())) {
                assertThatThrownBy(() -> history.assessRestore(invalid, f.original().batch().policy().policy(), COMMAND,
                        f.fragments(), AT, budget, RepositoryReadControl.NONE)).isInstanceOfAny(
                                IllegalArgumentException.class, UnsupportedOperationException.class, RepositoryException.class);
                assertThat(budget.reservedBytes()).isZero();
            }
            release(ledger, history);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void revocationDuringAssessmentSuppressesSuccessAndPolicyDetails(boolean eligible) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); grant(c, f.address(), true);
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(new RepositoryCaller("scoped", false, Set.of("account"), Set.of()), f.address(), f.revision());
            var member = member(f, history); var budget = new PayloadBudget(32L * 1024 * 1024);
            long fragmentBytes = f.fragments().values().stream().mapToLong(ByteString::size).sum();
            var revoked = new java.util.concurrent.atomic.AtomicBoolean();
            var node = DocumentIds.nodeId(f.address()); long lock = node.getMostSignificantBits() ^ node.getLeastSignificantBits();
            var control = new RepositoryReadControl() {
                public long remainingNanos() { return Long.MAX_VALUE; }
                public boolean isCancelled() {
                    if (!revoked.get() && budget.reservedBytes() > fragmentBytes) c.tx().inTransaction(em -> {
                        boolean free = (Boolean) em.createNativeQuery("SELECT pg_try_advisory_xact_lock(:key)")
                                .setParameter("key", lock).getSingleResult();
                        if (free) {
                            em.createNativeQuery("UPDATE documents SET security=CAST('{}' AS jsonb) WHERE node_id=:node")
                                    .setParameter("node", node).executeUpdate(); revoked.set(true);
                        }
                    });
                    return false;
                }
            };
            var policy = f.original().batch().policy().policy();
            if (!eligible) policy = DocumentAdmissionPolicy.of(policy.definition().toBuilder()
                    .setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(DocumentSchemaPolicyBinding.newBuilder()
                            .setTypeUrl("type.test/google.protobuf.Timestamp").setSchema(PublicationSchemaCondition.newBuilder()
                                    .setTypeName("google.protobuf.Timestamp").setDescriptorFingerprint("0".repeat(64)))))
                    .build(), () -> {});
            var current = policy;
            assertThatThrownBy(() -> history.assessRestore(member, current, COMMAND, f.fragments(), AT, budget, control))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(revoked).isTrue(); assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @Test void schemaScopeOwnsSqlBytesButBorrowsExactLiveSourceUse() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision());
            var budget = new PayloadBudget(32L * 1024 * 1024);
            try (var use = history.use()) {
                var selected = use.plan().entries();
                var mapping = new HashMap<Integer, Integer>();
                for (var entry : selected) mapping.put(entry.revisionOrdinal(), entry.revisionOrdinal());
                var bad = new ArrayList<>(selected);
                var first = bad.getFirst();
                bad.set(0, new DocumentHistoricalReadPlan.Entry(first.revisionOrdinal(), UUID.randomUUID(), first.part()));
                assertThatThrownBy(() -> DocumentHistoricalSchemaResolution.open(history, use, mapping, bad,
                        budget, RepositoryReadControl.NONE)).hasMessageContaining("differs from pinned source");
                assertThatThrownBy(() -> DocumentHistoricalSchemaResolution.open(history, use, Map.of(), selected,
                        budget, RepositoryReadControl.NONE)).hasMessageContaining("differs from selected ordinals");
                var other = ledger.captureHistorical(ADMIN, f.address(), f.revision());
                try {
                    assertThatThrownBy(() -> DocumentHistoricalSchemaResolution.open(other, use, mapping, selected,
                            budget, RepositoryReadControl.NONE)).hasMessageContaining("another historical capture");
                } finally { other.close(); other.release(); }
                assertThat(budget.reservedBytes()).isZero();
                try (var schemas = DocumentHistoricalSchemaResolution.open(history, use, mapping, selected,
                        budget, RepositoryReadControl.NONE)) {
                    assertThat(schemas.resolution().container()).isNotNull();
                    assertThat(budget.reservedBytes()).isPositive();
                }
                assertThat(budget.reservedBytes()).isZero();
                assertThat(use.plan().revision()).isEqualTo(f.revision());
                try (var schemas = DocumentHistoricalSchemaResolution.open(history, use, mapping, selected,
                        budget, RepositoryReadControl.NONE)) {
                    var borrowed = schemas.resolution();
                    use.close();
                    assertThatThrownBy(schemas::resolution).isInstanceOf(IllegalStateException.class);
                    assertThatThrownBy(borrowed::container).isInstanceOf(IllegalStateException.class);
                }
                assertThat(budget.reservedBytes()).isZero();
            } finally { release(ledger, history); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void schemaLoadingFailureReleasesSqlReservationsWithoutClosingBorrowedUse(boolean cancel) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address(), f.revision());
            var budget = new PayloadBudget(cancel ? 32L * 1024 * 1024 : 1);
            try (var use = history.use()) {
                var selected = use.plan().entries();
                var mapping = new HashMap<Integer, Integer>();
                for (var entry : selected) mapping.put(entry.revisionOrdinal(), entry.revisionOrdinal());
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancel && budget.reservedBytes() > 0; }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> DocumentHistoricalSchemaResolution.open(history, use, mapping, selected,
                        budget, control)).isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(cancel ? RepositoryException.Code.CANCELLED
                                        : RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(budget.reservedBytes()).isZero();
                assertThat(use.plan().revision()).isEqualTo(f.revision());
            } finally { release(ledger, history); }
        }
    }

    private static DocumentPublicationMember member(Fixture f, DocumentReadLedger.PinnedHistory history) {
        var member = f.original().command().intent().getMembers(0).toBuilder().clearParts();
        member.setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(1));
        try (var use = history.use()) {
            var plan = use.plan();
            for (var entry : plan.entries()) {
                var part = entry.part().part(); var binding = entry.part().binding();
                var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString())
                        .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                        .setNamespace(binding.namespace()).setObjectKey(part.key()).setSizeBytes(part.size())
                        .setSha256(part.sha256()).setContentType(part.contentType());
                if (part.providerVersion() != null) object.setProviderVersion(part.providerVersion());
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(
                        PublicationHistoricalReuse.newBuilder().setSource(plan.address()).setRevisionId(plan.revision().toString())
                                .setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot).setObject(object)));
            }
        }
        return member.build();
    }
    private record Fixture(DocumentSchemaRetentionFixture.Fixture original, UUID revision) {
        NodeAddress address() { return original.command().intent().getMembers(0).getDestination().getAddress(); }
        Map<Integer, ByteString> fragments() { return original.batch().proofs().get("member").fragments(); }
    }
    private static Fixture fixture(Context c) throws Exception {
        var f = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
        return new Fixture(f, DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, id, manifest) -> f.retention().write(em, f.owner(), id, () -> {})));
    }
    private static void grant(Context c, NodeAddress address, boolean read) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("node", DocumentIds.nodeId(address)).setParameter("policy", read
                        ? "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}" : "{}")
                .executeUpdate(); });
    }
    private static void release(DocumentReadLedger ledger, DocumentReadLedger.PinnedHistory history) throws Exception {
        history.close(); assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue(); history.release();
        ledger.fence(); ledger.attestLocalQuiescence();
    }
}
