package ai.protomolt.proto.repo.admission;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import ai.protomolt.proto.repo.v1.*;
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

    @Test void reassessesHistoricalDeclarationsWithNewCommandPolicyAndRemappedOrdinals() throws Exception {
        var captured = capture("true"); var source = captured.request().candidate();
        assertThat(source.member().getPartsCount()).isEqualTo(2);
        var current = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder()
                .setLimits(POLICY.definition().getLimits().toBuilder().setMaxFragments(24)).build(), () -> {});
        var member = historical(source.member(), List.of(1, 0));
        var fragments = Map.of(0, source.fragments().get(1), 1, source.fragments().get(0));
        var command = ByteString.copyFrom(new byte[32]).substring(0, 31).concat(ByteString.copyFromUtf8("x"));
        var budget = new Reservations();
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 1, 1, 0),
                hash -> Optional.ofNullable(captured.assets().get(hash)), current.limits(), budget, () -> {});
                var assessed = current.assess(command, member, fragments, retained.container(), retained, budget, AT.plusSeconds(1), () -> {})) {
            retained.requireComplete(assessed.view());
            assertThat(assessed.failure()).isEmpty();
            assertThat(assessed.view().request().commandSha256()).isEqualTo(command).isNotEqualTo(source.commandSha256());
            assertThat(assessed.view().request().policySha256()).isEqualTo(current.sha256()).isNotEqualTo(source.policySha256());
            assertThat(assessed.view().evaluatedAt()).isEqualTo(AT.plusSeconds(1));
            assertThat(assessed.view().request().member()).isEqualTo(member);
        }
        assertThat(budget.live).isZero();
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0, 1, 1),
                hash -> Optional.ofNullable(captured.assets().get(hash)), current.limits(), budget, () -> {})) {
            assertThatThrownBy(() -> current.assess(command, member, fragments, retained.container(), retained, budget, AT, () -> {}))
                    .hasMessageContaining("no exact retained schema selection");
        }
        assertThat(budget.live).isZero();
    }

    @Test void selectedSubsetDoesNotRequireUnselectedHistoricalDefinitionsInNewAssessment() throws Exception {
        var captured = capture("true"); var source = captured.request().candidate(); var budget = new Reservations();
        var current = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder()
                .setLimits(POLICY.definition().getLimits().toBuilder().setMaxFragments(1)).build(), () -> {});
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0),
                hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(), budget, () -> {});
                var assessed = current.assess(ByteString.copyFromUtf8("x".repeat(32)), historical(source.member(), List.of(0)),
                        Map.of(0, source.fragments().get(0)), retained.container(), retained, budget, AT, () -> {})) {
            retained.requireComplete(assessed.view());
            assertThat(assessed.failure()).isEmpty();
            assertThat(assessed.view().references()).noneMatch(r -> r.typeUrl().endsWith("Timestamp"));
            assertThatThrownBy(() -> retained.requireFullUnion(assessed.view())).hasMessageContaining("complete retained union");
        }
        assertThat(budget.live).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"true", "false"})
    void separateRetainedScopesAccountForOneCompositeAssessment(String expression) throws Exception {
        var captured = capture(expression); var source = captured.request().candidate(); var budget = new Reservations();
        DocumentSchemaAdmission.Reader reader = hash -> Optional.ofNullable(captured.assets().get(hash));
        try (var first = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0), reader, POLICY.limits(), budget, () -> {});
                var second = DocumentRetainedSchemaResolution.open(source, Map.of(1, 1), reader, POLICY.limits(), budget, () -> {});
                var unused = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0), reader, POLICY.limits(), budget, () -> {});
                var composite = DocumentCompositeSchemaResolution.open(historical(source.member(), List.of(0, 1)),
                        List.of(first, second), Optional.empty(), occurrence -> { throw new AssertionError("Retained part reached current resolver"); },
                        POLICY.limits(), budget, () -> {});
                var assessed = POLICY.assess(ByteString.copyFromUtf8("n".repeat(32)), historical(source.member(), List.of(0, 1)),
                        source.fragments(), composite.container(), composite, budget, AT.plusSeconds(1), () -> {})) {
            assertThat(first.container()).isEqualTo(second.container());
            first.requireSelectedComplete(assessed.view()); second.requireSelectedComplete(assessed.view());
            composite.requireComplete(assessed.view());
            assertThat(assessed.failure().isPresent()).isEqualTo(expression.equals("false"));
            assertThatThrownBy(() -> first.requireComplete(assessed.view())).hasMessageContaining("roots or occurrences differ");
            assertThatThrownBy(() -> second.requireComplete(assessed.view())).hasMessageContaining("roots or occurrences differ");
            assertThatThrownBy(() -> unused.requireSelectedComplete(assessed.view())).hasMessageContaining("roots or occurrences differ");
            var references = new java.util.HashSet<>(first.selectedReferences()); references.addAll(second.selectedReferences());
            var artifacts = new HashMap<>(first.selectedArtifacts());
            second.selectedArtifacts().forEach((hash, bytes) -> {
                var previous = artifacts.putIfAbsent(hash, bytes);
                if (previous != null) assertThat(previous).isEqualTo(bytes);
            });
            assertThat(assessed.references()).containsExactlyInAnyOrderElementsOf(references);
            assertThat(assessed.artifacts()).isEqualTo(artifacts);
        }
        assertThat(budget.live).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void mixedRetainedAndCurrentPartsRequireExactAccountedUnion(boolean bypassOrdinaryRoute) throws Exception {
        var captured = capture("true"); var source = captured.request().candidate(); var budget = new Reservations();
        var ordinary = fixture(true);
        var member = historical(source.member(), List.of(0, 1)).toBuilder().setParts(1, source.member().getParts(1)).build();
        var currentCalls = new AtomicInteger();
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0),
                hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(), budget, () -> {});
                var composite = DocumentCompositeSchemaResolution.open(member, List.of(retained), Optional.of(ordinary.container().definition()),
                        occurrence -> {
                            assertThat(occurrence.ordinal()).isEqualTo(1); currentCalls.incrementAndGet();
                            return ordinary.timestamp().definition();
                        }, POLICY.limits(), budget, () -> {});
                var assessed = POLICY.assess(ByteString.copyFromUtf8("m".repeat(32)), member, source.fragments(), composite.container(),
                        occurrence -> bypassOrdinaryRoute && occurrence.ordinal() == 1
                                ? ordinary.timestamp().definition() : composite.select(occurrence), budget, AT, () -> {})) {
            if (bypassOrdinaryRoute) {
                assertThat(currentCalls).hasValue(0);
                assertThatThrownBy(() -> composite.requireComplete(assessed.view())).hasMessageContaining("ordinary occurrences");
            } else {
                composite.requireComplete(assessed.view());
                assertThat(currentCalls).hasValue(1);
                assertThat(assessed.failure()).isEmpty();
            }
            composite.close();
            assertThat(retained.container()).isNotNull();
            assertThatThrownBy(composite::container).isInstanceOf(IllegalStateException.class);
        }
        assertThat(budget.live).isZero();
    }

    @Test void sharedAssetsCannotHideBypassedOrdinaryOccurrence() throws Exception {
        var normal = fixture(true);
        var f = fixture(true, normal.document().toBuilder().setStructuredData(
                Any.pack(com.google.protobuf.Timestamp.newBuilder().setSeconds(3).build(), "type.test")).build());
        var budget = new Reservations();
        try (var original = POLICY.assess(ByteString.copyFrom(new byte[32]), f.member(), f.fragments(), f.container().definition(),
                occurrence -> f.timestamp().definition(), budget, AT, () -> {});
                var retained = DocumentRetainedSchemaResolution.open(DocumentSchemaAssessmentReplay.Request.from(original.view()).candidate(),
                        Map.of(0, 0), hash -> Optional.ofNullable(original.artifacts().get(hash)), POLICY.limits(), budget, () -> {})) {
            var member = historical(f.member(), List.of(0, 1)).toBuilder().setParts(1, f.member().getParts(1)).build();
            try (var composite = DocumentCompositeSchemaResolution.open(member, List.of(retained), Optional.of(f.container().definition()),
                    occurrence -> { throw new AssertionError("Test deliberately bypasses ordinary route"); }, POLICY.limits(), budget, () -> {});
                    var assessed = POLICY.assess(ByteString.copyFrom(new byte[32]), member, f.fragments(), composite.container(),
                            occurrence -> occurrence.ordinal() == 0 ? composite.select(occurrence) : f.timestamp().definition(),
                            budget, AT, () -> {})) {
                assertThat(assessed.artifacts()).isEqualTo(retained.selectedArtifacts());
                assertThatThrownBy(() -> composite.requireComplete(assessed.view())).hasMessageContaining("ordinary occurrences");
            }
        }
        assertThat(budget.live).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void sameTypeUrlDefinitionsStayBoundToTheirExactOrdinaryOccurrence(boolean swap) throws Exception {
        var firstType = DocumentPayloadCheckTest.choice("true"); var secondType = DocumentPayloadCheckTest.choice("false");
        var first = asset(firstType, true); var second = asset(secondType, true);
        var normal = fixture(true);
        var document = normal.document().toBuilder().setStructuredData(Any.pack(DynamicMessage.newBuilder(firstType).build(), "type.test"))
                .putParserResults("parsed", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder()
                        .setShape(Any.pack(DynamicMessage.newBuilder(secondType).build(), "type.test"))).build()).build();
        var f = fixture(true, document); var budget = new Reservations();
        try (var composite = DocumentCompositeSchemaResolution.open(f.member(), List.of(), Optional.of(f.container().definition()),
                occurrence -> occurrence.ordinal() == 0 ? first.definition() : second.definition(), POLICY.limits(), budget, () -> {});
                var assessed = POLICY.assess(ByteString.copyFrom(new byte[32]), f.member(), f.fragments(), composite.container(), occurrence -> {
                    var selected = composite.select(occurrence);
                    return swap ? (occurrence.ordinal() == 0 ? second.definition() : first.definition()) : selected;
                }, budget, AT, () -> {})) {
            assertThat(assessed.references().stream().filter(ref -> ref.typeUrl().equals(first.metadata().getTypeUrl())).count()).isEqualTo(2);
            if (swap) assertThatThrownBy(() -> composite.requireComplete(assessed.view())).hasMessageContaining("different definition");
            else composite.requireComplete(assessed.view());
            assertThat(assessed.failure()).isPresent();
        }
        assertThat(budget.live).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"missing", "overlap", "ordinary-route", "container", "capacity", "cancel"})
    void compositeRefusesInvalidRoutesContainersAndLimitsWithoutClosingSources(String fault) throws Exception {
        var captured = capture("true"); var source = captured.request().candidate(); var budget = new Reservations();
        var ordinary = fixture(true);
        try (var first = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0),
                hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(), budget, () -> {});
                var second = DocumentRetainedSchemaResolution.open(source, Map.of(1, 1),
                        hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(), budget, () -> {})) {
            long before = budget.live;
            var member = fault.equals("ordinary-route") ? source.member() : historical(source.member(), List.of(0, 1));
            var scopes = fault.equals("missing") ? List.of(first) : fault.equals("overlap") ? List.of(first, first) : List.of(first, second);
            var container = fault.equals("container") ? Optional.of(ordinary.string().definition()) : Optional.<DocumentSchemaAdmission.Definition>empty();
            var limits = fault.equals("capacity") ? DocumentAdmissionPolicy.of(POLICY.definition().toBuilder()
                    .setLimits(POLICY.definition().getLimits().toBuilder().setMaxRetainedBytes(1)).build(), () -> {}).limits() : POLICY.limits();
            assertThatThrownBy(() -> DocumentCompositeSchemaResolution.open(member, scopes, container,
                    occurrence -> { throw new AssertionError("No resolver call during preflight"); }, limits, budget, () -> {
                        if (fault.equals("cancel")) throw new java.util.concurrent.CancellationException("cancelled composite");
                    })).isInstanceOfAny(IllegalArgumentException.class, java.util.concurrent.CancellationException.class);
            assertThat(budget.live).isEqualTo(before);
            assertThat(first.container()).isNotNull(); assertThat(second.container()).isNotNull();
        }
        assertThat(budget.live).isZero();
    }

    @Test void selectedRootlessOrdinalCannotGainAnUnaccountedRoot() throws Exception {
        var normal = fixture(true);
        var rootless = fixture(true, normal.document().toBuilder().clearStructuredData().build());
        var budget = new Reservations();
        var sourcePolicy = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder().setRequireStructuredRoot(false).build(), () -> {});
        try (var source = sourcePolicy.assess(ByteString.copyFrom(new byte[32]), rootless.member().toBuilder().clearStructuredSchema().build(), rootless.fragments(),
                rootless.container().definition(), occurrence -> rootless.timestamp().definition(), budget, AT, () -> {});
                var retained = DocumentRetainedSchemaResolution.open(DocumentSchemaAssessmentReplay.Request.from(source.view()).candidate(),
                        Map.of(0, 0), hash -> Optional.ofNullable(source.artifacts().get(hash)), POLICY.limits(), budget, () -> {});
                var changed = POLICY.assess(ByteString.copyFrom(new byte[32]), normal.member(), normal.fragments(),
                        normal.container().definition(), occurrence -> occurrence.ordinal() == 0
                                ? normal.string().definition() : normal.timestamp().definition(), budget, AT, () -> {})) {
            assertThat(source.roots()).noneMatch(root -> root.ordinal() == 0);
            assertThat(changed.roots()).anyMatch(root -> root.ordinal() == 0);
            assertThatThrownBy(() -> retained.requireSelectedComplete(changed.view()))
                    .hasMessageContaining("roots or occurrences differ");
            retained.close();
            assertThatThrownBy(retained::selectedReferences).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(retained::selectedArtifacts).isInstanceOf(IllegalStateException.class);
        }
        assertThat(budget.live).isZero();
    }

    @Test void destinationOrdinalIsIndependentOfSourceLoadingLimit() throws Exception {
        var captured = capture("true"); var budget = new Reservations();
        var sourcePolicy = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder()
                .setLimits(POLICY.definition().getLimits().toBuilder().setMaxFragments(2)).build(), () -> {});
        try (var retained = DocumentRetainedSchemaResolution.open(captured.request().candidate(), Map.of(3, 0),
                hash -> Optional.ofNullable(captured.assets().get(hash)), sourcePolicy.limits(), budget, () -> {})) {
            assertThat(retained.container()).isNotNull();
        }
        assertThatThrownBy(() -> DocumentRetainedSchemaResolution.open(captured.request().candidate(), Map.of(10_000, 0),
                hash -> { throw new AssertionError("mapping must be checked before asset reads"); },
                sourcePolicy.limits(), budget, () -> {})).hasMessageContaining("ordinal mapping");
        assertThat(budget.live).isZero();
    }

    @Test void duplicateSourceOrdinalsFailBeforeReadingAssets() throws Exception {
        var captured = capture("true"); var budget = new Reservations();
        assertThatThrownBy(() -> DocumentRetainedSchemaResolution.open(captured.request().candidate(), Map.of(0, 0, 1, 0),
                hash -> { throw new AssertionError("mapping must be checked before asset reads"); },
                POLICY.limits(), budget, () -> {})).hasMessageContaining("ordinal mapping");
        assertThat(budget.live).isZero();
    }

    @Test void oldAcceptanceCannotOverrideFreshTimeOrCurrentSchemaPolicy() throws Exception {
        var captured = capture("now == timestamp('2000-01-01T00:00:00Z')");
        assertThat(captured.request().expectedFailure()).isEmpty();
        var source = captured.request().candidate(); var budget = new Reservations();
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0, 1, 1),
                hash -> Optional.ofNullable(captured.assets().get(hash)), POLICY.limits(), budget, () -> {});
                var assessed = POLICY.assess(ByteString.copyFromUtf8("n".repeat(32)), historical(source.member(), List.of(0, 1)),
                        source.fragments(), retained.container(), retained, budget, AT.plusSeconds(1), () -> {})) {
            retained.requireComplete(assessed.view());
            assertThat(assessed.failure()).isPresent();
        }
        assertThat(budget.live).isZero();
        var timestamp = fixture(false).timestamp().metadata();
        var restrictive = DocumentAdmissionPolicy.of(POLICY.definition().toBuilder().setAllowedSchemas(
                DocumentSchemaPolicyAllowList.newBuilder().addBindings(DocumentSchemaPolicyBinding.newBuilder()
                        .setTypeUrl(timestamp.getTypeUrl()).setSchema(timestamp.getSchema()))).build(), () -> {});
        try (var retained = DocumentRetainedSchemaResolution.open(source, Map.of(0, 0, 1, 1),
                hash -> Optional.ofNullable(captured.assets().get(hash)), restrictive.limits(), budget, () -> {})) {
            assertThatThrownBy(() -> restrictive.assess(ByteString.copyFromUtf8("n".repeat(32)),
                    historical(source.member(), List.of(0, 1)), source.fragments(), retained.container(), retained, budget, AT, () -> {}))
                    .hasMessageContaining("not eligible");
        }
        assertThat(budget.live).isZero();
    }

    private static DocumentPublicationMember historical(DocumentPublicationMember member, List<Integer> ordinals) {
        var target = member.toBuilder().clearParts().setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(3));
        for (int ordinal : ordinals) {
            var part = member.getParts(ordinal); var upload = part.getUpload();
            target.addParts(part.toBuilder().setHistoricalReuse(PublicationHistoricalReuse.newBuilder()
                    .setSource(member.getDestination().getAddress()).setRevisionId("10000000-0000-4000-8000-000000000001")
                    .setRevisionOrdinal(ordinal).setSourceSlot(part.getSlot()).setObject(PublicationObjectIdentity.newBuilder()
                            .setObjectId(new java.util.UUID(0, ordinal + 1).toString()).setBackendGeneration("fixture")
                            .setStorageRealm("fixture").setNamespace("fixture").setObjectKey("part-" + ordinal)
                            .setSizeBytes(upload.getSizeBytes()).setSha256(upload.getSha256()).setContentType(upload.getContentType()))));
        }
        return target.build();
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
