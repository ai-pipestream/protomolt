package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.validate.RuleCompilationException;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.admission.DocumentSchemaPreparationTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaAssessmentTest {
    private static final Instant AT = Instant.parse("2000-01-01T00:00:00Z");

    @Test void ownsNormalizedAssetsAndEvidenceWithoutDependingOnResolverBuffers() throws Exception {
        var f = fixture(true);
        var buffers = new ArrayList<byte[]>();
        var borrowed = new Fixture(f.document(), f.member(), f.fragments(), borrowed(f.container(), buffers),
                borrowed(f.string(), buffers), borrowed(f.timestamp(), buffers));
        var budget = new Reservations();
        var owner = assess(borrowed, selection -> selection.ordinal() == 0 ? borrowed.string().definition()
                : borrowed.timestamp().definition(), budget);
        var expected = new HashMap<>(owner.artifacts());
        try {
            assertThat(owner.failure()).isEmpty();
            assertThat(owner.roots()).hasSize(2);
            assertThat(owner.references()).hasSize(3);
            assertThat(owner.evaluatedAt()).isEqualTo(AT);
            long retained = owner.artifacts().values().stream().mapToLong(ByteString::size).sum()
                    + owner.roots().stream().mapToLong(root -> root.encoded().bytes().size()).sum();
            assertThat(budget.live).isEqualTo(retained);
            buffers.forEach(bytes -> java.util.Arrays.fill(bytes, (byte) 0));
            assertThat(owner.artifacts()).isEqualTo(expected);
            assertThat(owner.request().container().descriptors()).isEqualTo(f.container().descriptors());
            for (var root : owner.roots()) {
                var encoded = root.encoded();
                assertThat(DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(),
                        encoded.sha256(), () -> {}).getOccurrencesCount()).isPositive();
            }
        } finally { owner.close(); }
        owner.close();
        assertThat(budget.live).isZero();
        assertThatThrownBy(owner::artifacts).hasMessageContaining("closed");
    }

    @Test void invalidCoreStillRequiresTheLaterParsedSchema() throws Exception {
        var invalid = invalidFixture();
        var f = invalid.fixture();
        var budget = new Reservations();
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> assess(f, selection -> {
            calls.incrementAndGet();
            return selection.ordinal() == 0 ? invalid.schema().definition() : null;
        }, budget)).hasMessageContaining("unresolved Any schema definition");
        assertThat(calls.get()).isEqualTo(2);
        assertThat(budget.live).isZero();
        try (var result = assess(f, selection -> selection.ordinal() == 0 ? invalid.schema().definition()
                : f.timestamp().definition(), budget)) {
            assertThat(result.failure()).isPresent();
            assertThat(result.failure().orElseThrow().ordinal()).isZero();
            assertThat(result.failure().orElseThrow().ruleId()).isEqualTo("choice.exclusive");
            assertThat(result.roots()).hasSize(2);
            assertThat(result.references()).hasSize(3);
        }
        assertThat(budget.live).isZero();
    }

    @Test void laterFragmentHashMustMatchBeforeAnySchemaSelection() throws Exception {
        var invalid = invalidFixture();
        var f = invalid.fixture();
        var corrupt = new HashMap<>(f.fragments());
        var bytes = corrupt.get(1).toByteArray();
        bytes[bytes.length - 1] ^= 1;
        corrupt.put(1, ByteString.copyFrom(bytes));
        var request = new DocumentSchemaAdmission.Preparation(ByteString.copyFrom(new byte[32]), "a".repeat(64), true,
                f.member(), corrupt, f.container().definition());
        var budget = new Reservations();
        assertThatThrownBy(() -> DocumentSchemaAssessment.prepare(request,
                selection -> { throw new AssertionError("Hash verification must precede resolution"); }, limits(1_000_000),
                budget, AT, () -> {})).hasMessageContaining("fragment differs from publication declaration");
        assertThat(budget.live).isZero();
    }

    @Test void unsupportedLaterRootPreventsAnInvalidAssessment() throws Exception {
        var invalid = invalidFixture();
        var badType = DocumentPayloadCheckTest.choice("this.no_such_field == 1");
        var badAsset = asset(badType, false);
        var badValue = Any.pack(DynamicMessage.newBuilder(badType).build(), "type.test");
        var doc = invalid.fixture().document().toBuilder();
        doc.putParserResults("parsed", doc.getParserResultsOrThrow("parsed").toBuilder()
                .setDocument(doc.getParserResultsOrThrow("parsed").getDocument().toBuilder().setShape(badValue)).build());
        var f = fixture(false, doc.build());
        var budget = new Reservations();
        assertThatThrownBy(() -> assess(f, selection -> selection.ordinal() == 0 ? invalid.schema().definition()
                : badAsset.definition(), budget)).isInstanceOf(RuleCompilationException.class);
        assertThat(budget.live).isZero();
    }

    @Test void everyReservationFailureAndLaterCancellationReleasesTheWholeAssessment() throws Exception {
        var invalid = invalidFixture();
        var f = invalid.fixture();
        DocumentSchemaAdmission.Resolver resolver = selection -> selection.ordinal() == 0 ? invalid.schema().definition()
                : f.timestamp().definition();
        var baseline = new Reservations();
        try (var ignored = assess(f, resolver, baseline)) { assertThat(baseline.live).isPositive(); }
        assertThat(baseline.live).isZero();
        for (int i = 1; i <= baseline.calls; i++) {
            var budget = new Reservations();
            budget.refuseAt = i;
            budget.refusal = new IllegalStateException("reservation " + i);
            assertThatThrownBy(() -> assess(f, resolver, budget)).isSameAs(budget.refusal);
            assertThat(budget.live).isZero();
        }
        var budget = new Reservations();
        var cancelled = new CancellationException("later parsed root cancelled");
        assertThatThrownBy(() -> assess(f, selection -> {
            if (selection.ordinal() > 0) throw cancelled;
            return invalid.schema().definition();
        }, budget)).isSameAs(cancelled);
        assertThat(budget.live).isZero();
    }

    @Test void invalidRootsConsumeAggregateBudgets() throws Exception {
        var invalid = invalidFixture();
        var f = invalid.fixture();
        DocumentSchemaAdmission.Resolver resolver = selection -> selection.ordinal() == 0 ? invalid.schema().definition()
                : f.timestamp().definition();
        var budget = new Reservations();
        long evidence;
        try (var result = assess(f, resolver, budget)) {
            evidence = result.roots().stream().mapToLong(root -> root.encoded().bytes().size()).sum();
        }
        int decoded = f.document().getStructuredData().getValue().size()
                + f.document().getParserResultsOrThrow("parsed").getDocument().getShape().getValue().size();
        var limited = java.util.List.of(
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 1, 4_000_000, 20, 16_000_000, 1_000_000),
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, evidence - 1, 20, 16_000_000, 1_000_000),
                limits(decoded - 1));
        for (var limits : limited) {
            assertThatThrownBy(() -> DocumentSchemaAssessment.prepare(request(f), resolver, limits, budget, AT, () -> {}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(budget.live).isZero();
        }
    }

    @Test void oneEvaluationInstantAppliesToCoreAndParsedRoots() throws Exception {
        var type = DocumentPayloadCheckTest.choice("now == timestamp('2000-01-01T00:00:00Z')");
        var schema = asset(type, false);
        var value = Any.pack(DynamicMessage.newBuilder(type).build(), "type.test");
        var doc = fixture(false).document().toBuilder().setStructuredData(value);
        doc.putParserResults("parsed", doc.getParserResultsOrThrow("parsed").toBuilder()
                .setDocument(doc.getParserResultsOrThrow("parsed").getDocument().toBuilder().setShape(value)).build());
        var f = fixture(false, doc.build());
        var budget = new Reservations();
        for (var instant : java.util.List.of(AT, AT.plusSeconds(1))) {
            var calls = new AtomicInteger();
            try (var result = DocumentSchemaAssessment.prepare(request(f), selection -> {
                calls.incrementAndGet(); return schema.definition();
            }, limits(1_000_000), budget, instant, () -> {})) {
                assertThat(calls.get()).isEqualTo(2);
                assertThat(result.roots()).hasSize(2);
                assertThat(result.failure().isEmpty()).isEqualTo(instant.equals(AT));
            }
            assertThat(budget.live).isZero();
        }
    }

    @Test void acceptedProofOwnsSchemaCopiesAfterAssessmentClosesButBorrowsHostFragments() throws Exception {
        var f = fixture(true);
        var sourceBudget = new Reservations();
        var proofBudget = new Reservations();
        var assessment = assess(f, selection -> selection.ordinal() == 0
                ? f.string().definition() : f.timestamp().definition(), sourceBudget);
        try (var prepared = DocumentSchemaAdmission.checkAccepted(assessment.view(), limits(1_000_000), proofBudget, () -> {})) {
            var proof = prepared.proof();
            assessment.artifacts().forEach((hash, bytes) ->
                    assertThat(proof.artifacts().get(hash)).isEqualTo(bytes).isNotSameAs(bytes));
            assertThat(proof.fragments().get(0)).isSameAs(f.fragments().get(0));
            assessment.close();
            assertThat(sourceBudget.live).isZero();
            assertThat(proofBudget.live).isPositive();
            assertThat(proof.roots()).hasSize(2);
        } finally { assessment.close(); }
        assertThat(proofBudget.live).isZero();
    }

    @Test void everyPromotionReservationFailureReleasesCopiesAndPreservesAssessment() throws Exception {
        var f = fixture(true);
        var sourceBudget = new Reservations();
        try (var assessment = assess(f, selection -> selection.ordinal() == 0
                ? f.string().definition() : f.timestamp().definition(), sourceBudget)) {
            long owned = sourceBudget.live;
            var baseline = new Reservations();
            try (var proof = DocumentSchemaAdmission.checkAccepted(assessment.view(), limits(1_000_000), baseline, () -> {})) {
                assertThat(proof.proof().roots()).hasSize(2);
            }
            assertThat(baseline.calls).isPositive();
            assertThat(baseline.live).isZero();
            for (int at = 1; at <= baseline.calls; at++) {
                var budget = new Reservations();
                budget.refuseAt = at;
                budget.refusal = new IllegalStateException("reservation refusal " + at);
                assertThatThrownBy(() -> DocumentSchemaAdmission.checkAccepted(assessment.view(), limits(1_000_000), budget, () -> {}))
                        .isSameAs(budget.refusal);
                assertThat(budget.live).isZero();
                assertThat(sourceBudget.live).isEqualTo(owned);
                assertThat(assessment.failure()).isEmpty();
            }
        }
        assertThat(sourceBudget.live).isZero();
    }

    @Test void noPayloadRootCannotBecomeATypedAssessment() throws Exception {
        var f = fixture(false, fixture(false).document().toBuilder().clearStructuredData().clearParserResults().build());
        var request = new DocumentSchemaAdmission.Preparation(ByteString.copyFrom(new byte[32]), "a".repeat(64), false,
                f.member(), f.fragments(), f.container().definition());
        var budget = new Reservations();
        assertThatThrownBy(() -> DocumentSchemaAssessment.prepare(request,
                selection -> { throw new AssertionError("No payload root to resolve"); }, limits(1_000_000), budget, AT, () -> {}))
                .hasMessageContaining("requires a payload root");
        assertThat(budget.live).isZero();
    }

    private record InvalidFixture(Fixture fixture, Asset schema) {}
    private static InvalidFixture invalidFixture() throws Exception {
        var type = DocumentPayloadCheckTest.choice("false");
        var schema = asset(type, true);
        var value = DynamicMessage.newBuilder(type).setField(type.findFieldByName("left"), "invalid").build();
        var doc = fixture(false).document().toBuilder().setStructuredData(Any.pack(value, "type.test")).build();
        return new InvalidFixture(fixture(false, doc), schema);
    }
    private static DocumentSchemaAssessment assess(Fixture f, DocumentSchemaAdmission.Resolver resolver,
            Reservations budget) throws Exception {
        return DocumentSchemaAssessment.prepare(request(f), resolver,
                limits(1_000_000), budget, AT, () -> {});
    }
    private static DocumentSchemaAdmission.Preparation request(Fixture f) {
        return new DocumentSchemaAdmission.Preparation(ByteString.copyFrom(new byte[32]),
                "a".repeat(64), true, f.member(), f.fragments(), f.container().definition());
    }
}
