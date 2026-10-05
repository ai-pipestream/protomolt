package ai.protomolt.proto.repo.admission;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.admission.DocumentSchemaPreparationTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaAssessmentReplayTest {
    private static final Instant AT = Instant.parse("2000-01-01T00:00:00Z");
    private static final DocumentAdmissionPolicy POLICY = DocumentAdmissionPolicy.of(DocumentAdmissionPolicyTest.policy(), () -> {});
    private record Captured(DocumentSchemaAssessmentReplay.Request request, Map<String, ByteString> assets) {}

    private static Captured capture(String expression) throws Exception {
        var type = DocumentPayloadCheckTest.choice(expression);
        var schema = asset(type, true);
        var doc = fixture(true).document().toBuilder()
                .setStructuredData(Any.pack(DynamicMessage.newBuilder(type).build(), "type.test")).build();
        var f = fixture(true, doc);
        var budget = new Reservations();
        Captured captured;
        try (var assessment = POLICY.assess(ByteString.copyFrom(new byte[32]), f.member(), f.fragments(), f.container().definition(),
                selection -> selection.ordinal() == 0 ? schema.definition() : f.timestamp().definition(), budget, AT, () -> {})) {
            var borrowed = DocumentSchemaAssessmentReplay.Request.from(assessment.view());
            var c = borrowed.candidate();
            // Model a bounded external capture: every serialized buffer belongs to this fixture,
            // independently of the assessment's reservation lifetime. No provider is simulated.
            var fragments = new HashMap<Integer, ByteString>();
            c.fragments().forEach((ordinal, bytes) -> fragments.put(ordinal, copy(bytes)));
            var evidence = new HashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
            c.evidence().forEach((ordinal, roots) -> evidence.put(ordinal, roots.stream().map(root ->
                    new DocumentSchemaAdmission.EncodedEvidence(root.codec(), root.version(), copy(root.bytes()), root.sha256())).toList()));
            var assets = new HashMap<String, ByteString>();
            assessment.artifacts().forEach((hash, bytes) -> assets.put(hash, copy(bytes)));
            long ownedBytes = fragments.values().stream().mapToLong(ByteString::size).sum()
                    + assets.values().stream().mapToLong(ByteString::size).sum()
                    + evidence.values().stream().flatMap(List::stream).mapToLong(root -> root.bytes().size()).sum();
            assertThat(ownedBytes).isLessThan(4_000_000);
            captured = new Captured(new DocumentSchemaAssessmentReplay.Request(new DocumentSchemaAdmission.Request(
                    copy(c.commandSha256()), c.policySha256(), c.requireStructuredRoot(),
                    ai.protomolt.proto.repo.v1.DocumentPublicationMember.parseFrom(c.member().toByteArray()),
                    Map.copyOf(fragments), Map.copyOf(evidence), c.container(), c.references()),
                    borrowed.evaluatedAt(), borrowed.expectedFailure()), Map.copyOf(assets));
        }
        assertThat(budget.live).isZero();
        return captured;
    }
    private static ByteString copy(ByteString bytes) { return ByteString.copyFrom(bytes.toByteArray()); }
    private static void verify(Captured captured, Reservations budget) throws Exception {
        DocumentSchemaAssessmentReplay.verify(captured.request(), POLICY,
                hash -> Optional.ofNullable(captured.assets().get(hash)), budget, () -> {});
    }
    private static DocumentSchemaAssessmentReplay.Request withEvidence(Captured captured,
            Map<Integer, List<DocumentSchemaAdmission.EncodedEvidence>> evidence) {
        var r = captured.request(); var c = r.candidate();
        return new DocumentSchemaAssessmentReplay.Request(new DocumentSchemaAdmission.Request(c.commandSha256(), c.policySha256(),
                c.requireStructuredRoot(), c.member(), c.fragments(), evidence, c.container(), c.references()), r.evaluatedAt(), r.expectedFailure());
    }

    @Test void replaysAcceptedAndInvalidValuesFromOwnedAssetsAfterOriginalAssessmentClosed() throws Exception {
        for (var expression : List.of("true", "false")) {
            var captured = capture(expression);
            assertThat(captured.request().expectedFailure().isPresent()).isEqualTo(expression.equals("false"));
            var budget = new Reservations();
            verify(captured, budget);
            assertThat(budget.live).isZero();
            assertThat(budget.peak).isPositive();
        }
    }

    @Test void retainedRootInspectionChecksCanonicalBytesAndRecoversLocatorIdentity() throws Exception {
        var captured = capture("false");
        var encoded = captured.request().candidate().evidence().get(0).getFirst();
        var budget = new Reservations();
        var decoded = DocumentSchemaAdmission.decodeRootEvidence(0, encoded, budget, () -> {});
        assertThat(decoded.locatorSha256()).isEqualTo(DocumentSchemaRootCodec.encode(decoded.locator(), () -> {}).sha256());
        assertThat(decoded.locator().getSlot()).isEqualTo(captured.request().candidate().member().getParts(0).getSlot());
        assertThat(decoded.encoded()).isEqualTo(encoded);
        assertThat(budget.live).isZero();
        assertThat(budget.peak).isPositive();
        var corrupt = new DocumentSchemaAdmission.EncodedEvidence(encoded.codec(), encoded.version(),
                ByteString.copyFromUtf8("corrupt"), encoded.sha256());
        assertThatThrownBy(() -> DocumentSchemaAdmission.decodeRootEvidence(0, corrupt, budget, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(budget.live).isZero();
    }

    @Test void strictProofReplayUsesAssessmentTimeAndStillRejectsInvalidValues() throws Exception {
        var captured = capture("now == timestamp('2000-01-01T00:00:00Z')");
        for (boolean budgeted : List.of(false, true)) {
            var budget = new Reservations();
            try (var resources = new DocumentAdmissionResources(budget)) {
                var proof = DocumentSchemaAdmission.check(captured.request().candidate(),
                        hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(),
                        budgeted ? resources : null, AT, () -> {});
                assertThat(proof.member()).isEqualTo(captured.request().candidate().member());
                assertThat(proof.roots()).isNotEmpty();
                assertThatThrownBy(() -> DocumentSchemaAdmission.check(captured.request().candidate(),
                        hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(),
                        budgeted ? resources : null, AT.plusSeconds(1), () -> {}))
                        .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
            }
            assertThat(budget.live).isZero();
            if (budgeted) assertThat(budget.peak).isPositive();
        }
        var invalid = capture("false");
        assertThatThrownBy(() -> DocumentSchemaAdmission.check(invalid.request().candidate(),
                hash -> Optional.ofNullable(invalid.assets().get(hash)), POLICY.limits(), null, AT, () -> {}))
                .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
    }

    @Test void recordedTimeAndExpectedVerdictAreBothRequired() throws Exception {
        var captured = capture("now == timestamp('2000-01-01T00:00:00Z')");
        var budget = new Reservations();
        verify(captured, budget);
        var changedTime = new DocumentSchemaAssessmentReplay.Request(captured.request().candidate(), AT.plusSeconds(1), Optional.empty());
        assertThatThrownBy(() -> verify(new Captured(changedTime, captured.assets()), budget))
                .hasMessageContaining("verdict differs");
        var invalid = capture("false");
        var erasedFailure = new DocumentSchemaAssessmentReplay.Request(invalid.request().candidate(), AT, Optional.empty());
        assertThatThrownBy(() -> verify(new Captured(erasedFailure, invalid.assets()), budget)).hasMessageContaining("verdict differs");
        assertThat(budget.live).isZero();
    }

    @Test void reproducesValueResultWithoutInventingAnExpectedMemberVerdict() throws Exception {
        for (var expression : List.of("true", "false", "now == timestamp('2000-01-01T00:00:00Z')")) {
            var captured = capture(expression);
            var budget = new Reservations();
            var actual = DocumentSchemaAssessmentReplay.replay(captured.request().candidate(), AT, POLICY,
                    hash -> Optional.ofNullable(captured.assets().get(hash)), budget, () -> {});
            assertThat(actual).isEqualTo(captured.request().expectedFailure());
            assertThat(budget.live).isZero();
            if (expression.startsWith("now")) {
                var later = DocumentSchemaAssessmentReplay.replay(captured.request().candidate(), AT.plusSeconds(1), POLICY,
                        hash -> Optional.ofNullable(captured.assets().get(hash)), budget, () -> {});
                assertThat(later).isPresent();
                assertThat(budget.live).isZero();
            }
        }
    }

    @Test void resultReplayDoesNotTurnAssetFailureIntoAValueVerdict() throws Exception {
        var captured = capture("false");
        for (var hash : captured.assets().keySet()) {
            var missing = new HashMap<>(captured.assets()); missing.remove(hash);
            var corrupt = new HashMap<>(captured.assets()); corrupt.put(hash, ByteString.copyFromUtf8("corrupt"));
            for (var assets : List.of(missing, corrupt)) {
                var budget = new Reservations();
                assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.replay(captured.request().candidate(), AT, POLICY,
                        key -> Optional.ofNullable(assets.get(key)), budget, () -> {})).isInstanceOf(Exception.class);
                assertThat(budget.live).isZero();
            }
        }
        var budget = new Reservations();
        var cancelled = new java.util.concurrent.CancellationException("retained input cancelled");
        assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.replay(captured.request().candidate(), AT, POLICY,
                hash -> { throw cancelled; }, budget, () -> {})).isSameAs(cancelled);
        assertThat(budget.live).isZero();
    }

    @Test void resultReplayControlCancellationReleasesAlreadyReservedMemory() throws Exception {
        var captured = capture("false");
        var budget = new Reservations();
        var cancelled = new java.util.concurrent.CancellationException("replay cancelled after reservation");
        assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.replay(captured.request().candidate(), AT, POLICY,
                hash -> Optional.ofNullable(captured.assets().get(hash)), budget, () -> {
                    if (budget.live > 0) throw cancelled;
                })).isSameAs(cancelled);
        assertThat(budget.peak).isPositive();
        assertThat(budget.live).isZero();
    }

    @Test void payloadCanReuseTheContainingDocumentSchemaWithoutADuplicateReference() throws Exception {
        var base = fixture(false).document().toBuilder().clearStructuredData().clearParserResults().build();
        var f = fixture(false, base.toBuilder().setStructuredData(Any.pack(base, "type.test")).build());
        var budget = new Reservations();
        try (var assessment = POLICY.assess(ByteString.copyFrom(new byte[32]), f.member(), f.fragments(),
                f.container().definition(), selection -> f.container().definition(), budget, AT, () -> {})) {
            assertThat(assessment.references()).hasSize(1);
            var request = DocumentSchemaAssessmentReplay.Request.from(assessment.view());
            assertThat(request.candidate().references()).isEmpty();
            DocumentSchemaAssessmentReplay.verify(request, POLICY,
                    hash -> Optional.ofNullable(assessment.artifacts().get(hash)), budget, () -> {});
        }
        assertThat(budget.live).isZero();
    }

    @Test void missingEvidenceCannotHideLaterRoots() throws Exception {
        var captured = capture("false");
        var evidence = new HashMap<>(captured.request().candidate().evidence());
        evidence.remove(1);
        var budget = new Reservations();
        assertThatThrownBy(() -> verify(new Captured(withEvidence(captured, evidence), captured.assets()), budget))
                .hasMessageContaining("no exact retained schema selection");
        assertThat(budget.live).isZero();
    }

    @Test void duplicateRootsAndOutsideOrdinalsFailBeforeAssetReads() throws Exception {
        var captured = capture("true");
        var original = captured.request().candidate().evidence();
        var duplicate = new HashMap<>(original);
        duplicate.put(0, List.of(original.get(0).getFirst(), original.get(0).getFirst()));
        var outside = new HashMap<>(original);
        outside.put(999, original.get(0));
        var wrongSlot = new HashMap<>(original);
        wrongSlot.put(1, original.get(0));
        for (var evidence : List.of(duplicate, outside, wrongSlot)) {
            var budget = new Reservations();
            var reads = new AtomicInteger();
            assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.verify(withEvidence(captured, evidence), POLICY,
                    hash -> { reads.incrementAndGet(); return Optional.ofNullable(captured.assets().get(hash)); }, budget, () -> {}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(reads.get()).isZero();
            assertThat(budget.live).isZero();
        }
    }

    @Test void everyMissingOrCorruptAssetFailsIncludingSourceArchives() throws Exception {
        var captured = capture("false");
        for (var hash : captured.assets().keySet()) {
            var missing = new HashMap<>(captured.assets()); missing.remove(hash);
            var corrupt = new HashMap<>(captured.assets()); corrupt.put(hash, ByteString.copyFromUtf8("corrupted"));
            for (var assets : List.of(missing, corrupt)) {
                var budget = new Reservations();
                assertThatThrownBy(() -> verify(new Captured(captured.request(), assets), budget)).isInstanceOf(Exception.class);
                assertThat(budget.live).isZero();
            }
        }
    }

    @Test void reservationFailuresReleaseAllScratchAndPreserveOriginalFailure() throws Exception {
        var captured = capture("false");
        var baseline = new Reservations();
        verify(captured, baseline);
        assertThat(baseline.live).isZero();
        for (int i = 1; i <= baseline.calls; i++) {
            var budget = new Reservations(); budget.refuseAt = i;
            budget.refusal = new IllegalStateException("reservation " + i);
            assertThatThrownBy(() -> verify(captured, budget)).isSameAs(budget.refusal);
            assertThat(budget.live).isZero();
        }
    }

    @Test void differentPolicyAndDuplicateReferencesFailBeforeAssetReads() throws Exception {
        var captured = capture("true");
        var c = captured.request().candidate();
        var duplicates = new java.util.ArrayList<>(c.references());
        duplicates.add(c.references().getFirst());
        var duplicateRequest = new DocumentSchemaAssessmentReplay.Request(new DocumentSchemaAdmission.Request(c.commandSha256(),
                c.policySha256(), c.requireStructuredRoot(), c.member(), c.fragments(), c.evidence(), c.container(), duplicates), AT, Optional.empty());
        var otherPolicy = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder().setRequireStructuredRoot(false).build(), () -> {});
        var budget = new Reservations();
        DocumentSchemaAdmission.Reader noReads = hash -> { throw new AssertionError("Preflight must precede asset reads"); };
        assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.verify(captured.request(), otherPolicy, noReads, budget, () -> {}))
                .hasMessageContaining("policy snapshot");
        assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.verify(duplicateRequest, POLICY, noReads, budget, () -> {}))
                .hasMessageContaining("duplicate replay schema association");
        assertThat(budget.live).isZero();
    }

    @Test void corruptedEvidenceAndReaderCancellationReleaseScratch() throws Exception {
        var captured = capture("false");
        var evidence = new HashMap<>(captured.request().candidate().evidence());
        var first = evidence.get(0).getFirst();
        evidence.put(0, List.of(new DocumentSchemaAdmission.EncodedEvidence(first.codec(), first.version(),
                ByteString.copyFromUtf8("corrupt"), first.sha256())));
        var budget = new Reservations();
        assertThatThrownBy(() -> verify(new Captured(withEvidence(captured, evidence), captured.assets()), budget))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(budget.live).isZero();
        var cancelled = new java.util.concurrent.CancellationException("retained asset read cancelled");
        var reads = new AtomicInteger();
        assertThatThrownBy(() -> DocumentSchemaAssessmentReplay.verify(captured.request(), POLICY, hash -> {
            if (reads.incrementAndGet() == 3) throw cancelled;
            return Optional.ofNullable(captured.assets().get(hash));
        }, budget, () -> {})).isSameAs(cancelled);
        assertThat(budget.live).isZero();
    }
}
