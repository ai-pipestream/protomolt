package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.StringValue;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.container.ledger.DocumentSchemaBatchTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationAssessmentTest {
    private static final Instant AT = Instant.parse("2000-01-01T00:00:00Z");
    private static final Map<String, DocumentPublicationCandidate.Mode> TYPED = Map.of(
            "member-a", DocumentPublicationCandidate.Mode.TYPED, "member-b", DocumentPublicationCandidate.Mode.TYPED);

    @Test void completesLaterMembersAndOwnsPrivateFragmentsUntilClose() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        var supplied = fragments(f);
        var calls = new ArrayList<String>();
        var policy = selection(policy("account", false, 20));
        var result = DocumentPublicationAssessment.prepare(f.command(), policy, TYPED, supplied,
                Optional.of(f.assets().container().definition()), (member, occurrence) -> {
                    calls.add(member.getMemberId());
                    return member.getMemberId().equals("member-a") ? invalid.definition() : f.assets().payload().definition();
                }, budget, OPAQUE_LIMITS, AT, () -> {});
        var borrowed = result.typed().get("member-a");
        try {
            assertThat(calls).containsExactly("member-a", "member-b");
            assertThat(result.failure().orElseThrow().member()).isEqualTo("member-a");
            assertThat(result.typed()).containsOnlyKeys("member-a", "member-b");
            assertThat(result.opaque()).isEmpty();
            assertThat(result.artifacts()).hasSize(6);
            assertThat(result.policy()).isEqualTo(policy);
            assertThat(result.command()).isSameAs(f.command());
            assertThat(result.evaluatedAt()).isEqualTo(AT);
            assertThat(result.modes()).isEqualTo(TYPED);
            for (var member : result.typed().entrySet()) {
                var assessment = member.getValue();
                assertThat(assessment.request().policySha256()).isEqualTo(policy.policy().sha256());
                assertThat(assessment.evaluatedAt()).isEqualTo(AT);
                supplied.get(member.getKey()).forEach((ordinal, bytes) ->
                        assertThat(assessment.request().fragments().get(ordinal)).isEqualTo(bytes).isNotSameAs(bytes));
            }
            supplied.clear();
            assertThat(budget.reservedBytes()).isPositive();
            long owned = budget.reservedBytes();
            result.verifySchemas(() -> {});
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            assertThat(calls).containsExactly("member-a", "member-b");
        } finally { result.close(); }
        result.close();
        assertThat(budget.reservedBytes()).isZero();
        assertThatThrownBy(result::failure).hasMessageContaining("closed");
        assertThatThrownBy(borrowed::roots).hasMessageContaining("closed");
        assertThatThrownBy(() -> result.verifySchemas(() -> {})).hasMessageContaining("closed");
    }

    @Test void promotionTransfersFragmentsWithoutResolvingAgainAndKeepsOpaqueModeExplicit() throws Exception {
        var f = twoMembers();
        var schema = schema("pinned-time", "now == timestamp('2000-01-01T00:00:00Z')");
        for (boolean mixed : List.of(false, true)) {
            var modes = mixed ? Map.of("member-a", DocumentPublicationCandidate.Mode.TYPED,
                    "member-b", DocumentPublicationCandidate.Mode.OPAQUE) : TYPED;
            var budget = new PayloadBudget(32_000_000);
            var calls = new ArrayList<String>();
            var result = assess(f, modes, selection(policy("account", mixed, 20)), (member, occurrence) -> {
                calls.add(member.getMemberId()); return schema.definition();
            }, budget);
            var originalFragment = result.typed().get("member-a").request().fragments().get(0);
            try (var candidate = result.promoteAccepted(() -> {})) {
                result.close(); // Ownership moved; this must not release candidate bytes.
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(candidate.schemas().proofs().get("member-a").fragments().get(0)).isSameAs(originalFragment);
                assertThat(candidate.schemas().proofs()).hasSize(mixed ? 1 : 2);
                assertThat(candidate.opaque()).hasSize(mixed ? 1 : 0);
                assertThat(calls).hasSize(mixed ? 1 : 2);
                assertThatThrownBy(result::typed).hasMessageContaining("transferred");
                assertThatThrownBy(() -> result.promoteAccepted(() -> {})).hasMessageContaining("transferred");
            } finally { result.close(); }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void promotionCapacityAndCancellationFailuresKeepAssessmentRetryable() throws Exception {
        var f = twoMembers();
        var budget = new PayloadBudget(32_000_000);
        try (var result = assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> f.assets().payload().definition(), budget)) {
            long owned = budget.reservedBytes();
            try (var pressure = budget.reserve(budget.capacity() - owned)) {
                assertThatThrownBy(() -> result.promoteAccepted(() -> {}))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(budget.reservedBytes()).isEqualTo(budget.capacity());
            }
            var cancelled = new java.util.concurrent.CancellationException("promotion cancelled with owned copies");
            assertThatThrownBy(() -> result.promoteAccepted(() -> {
                if (budget.reservedBytes() > owned) throw cancelled;
            })).isSameAs(cancelled);
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            assertThat(result.failure()).isEmpty();
            try (var candidate = result.promoteAccepted(() -> {})) {
                assertThat(candidate.schemas().proofs()).hasSize(2);
            }
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void invalidAssessmentCannotBePromotedAndRemainsAvailableForRejection() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        try (var result = assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> invalid.definition(), budget)) {
            long owned = budget.reservedBytes();
            assertThatThrownBy(() -> result.promoteAccepted(() -> {})).hasMessageContaining("Invalid operation assessment");
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            result.verifySchemas(() -> {});
            assertThat(result.failure()).isPresent();
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterMissingSchemaCannotBeHiddenByAnEarlierInvalidMember() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        var calls = new ArrayList<String>();
        assertThatThrownBy(() -> assess(f, TYPED, selection(policy("account", false, 20)), (member, occurrence) -> {
            calls.add(member.getMemberId());
            return member.getMemberId().equals("member-a") ? invalid.definition() : null;
        }, budget)).hasMessageContaining("unresolved Any schema definition");
        assertThat(calls).containsExactly("member-a", "member-b");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterSameLengthHashMismatchPrecedesEveryResolverCall() throws Exception {
        var f = twoMembers();
        var supplied = fragments(f);
        var parts = new HashMap<>(supplied.get("member-b"));
        var bytes = parts.get(0).toByteArray(); bytes[bytes.length - 1] ^= 1;
        parts.put(0, ByteString.copyFrom(bytes)); supplied.put("member-b", parts);
        var budget = new PayloadBudget(32_000_000);
        assertThatThrownBy(() -> DocumentPublicationAssessment.prepare(f.command(), selection(policy("account", false, 20)),
                TYPED, supplied, Optional.of(f.assets().container().definition()),
                (member, occurrence) -> { throw new AssertionError("All command hashes must match first"); },
                budget, OPAQUE_LIMITS, AT, () -> {})).hasMessageContaining("Fragment hash differs");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterPolicyIneligibleSchemaPreventsAssessmentAndReleasesEarlierAssets() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var allowed = DocumentSchemaPolicyAllowList.newBuilder().addBindings(DocumentSchemaPolicyBinding.newBuilder()
                .setTypeUrl(invalid.definition().metadata().getTypeUrl()).setSchema(invalid.definition().metadata().getSchema()));
        var policy = DocumentAdmissionPolicy.of(policy("account", false, 20).definition().toBuilder()
                .setAllowedSchemas(allowed).build(), () -> {});
        var budget = new PayloadBudget(32_000_000);
        assertThatThrownBy(() -> assess(f, TYPED, selection(policy), (member, occurrence) ->
                member.getMemberId().equals("member-a") ? invalid.definition() : f.assets().payload().definition(), budget))
                .hasMessageContaining("not eligible under policy");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void opaqueModeStaysExplicitAndDoesNotResolveSchemas() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        var modes = Map.of("member-a", DocumentPublicationCandidate.Mode.TYPED,
                "member-b", DocumentPublicationCandidate.Mode.OPAQUE);
        try (var result = assess(f, modes, selection(policy("account", true, 20)), (member, occurrence) -> {
            assertThat(member.getMemberId()).isEqualTo("member-a"); return invalid.definition();
        }, budget)) {
            assertThat(result.failure()).isPresent();
            assertThat(result.opaque()).containsOnlyKeys("member-b");
            assertThat(result.typed()).containsOnlyKeys("member-a");
            long owned = budget.reservedBytes();
            result.verifySchemas(() -> {});
            assertThat(budget.reservedBytes()).isEqualTo(owned);
        }
        assertThat(budget.reservedBytes()).isZero();
        assertThatThrownBy(() -> assess(f, modes, selection(policy("account", false, 20)),
                (member, occurrence) -> { throw new AssertionError("Policy preflight must precede resolution"); }, budget))
                .hasMessageContaining("requires typed");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void replayCapacityFailureAndCancellationPreserveOwnerForRetry() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        try (var result = assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> invalid.definition(), budget)) {
            long owned = budget.reservedBytes();
            try (var pressure = budget.reserve(budget.capacity() - owned)) {
                assertThatThrownBy(() -> result.verifySchemas(() -> {}))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(budget.reservedBytes()).isEqualTo(budget.capacity());
            }
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            var cancelled = new java.util.concurrent.CancellationException("replay cancelled with scratch live");
            assertThatThrownBy(() -> result.verifySchemas(() -> {
                if (budget.reservedBytes() > owned) throw cancelled;
            })).isSameAs(cancelled);
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            result.verifySchemas(() -> {});
            assertThat(result.failure()).isPresent();
            assertThat(budget.reservedBytes()).isEqualTo(owned);
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void activeReplayRefusesCloseAndConcurrentReplayWithoutReleasingItsOwner() throws Exception {
        var f = twoMembers();
        var budget = new PayloadBudget(32_000_000);
        try (var result = assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> f.assets().payload().definition(), budget)) {
            long owned = budget.reservedBytes();
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var replay = workers.submit(() -> {
                    result.verifySchemas(() -> {
                        if (entered.getCount() == 0) return;
                        entered.countDown();
                        try {
                            if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Replay release timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new java.util.concurrent.CancellationException("Replay interrupted");
                        }
                    });
                    return null;
                });
                try {
                    assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(result::close).hasMessageContaining("verification is active");
                    assertThatThrownBy(() -> result.verifySchemas(() -> {})).hasMessageContaining("verification is active");
                    assertThat(budget.reservedBytes()).isEqualTo(owned);
                } finally { release.countDown(); }
                replay.get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            result.verifySchemas(() -> {});
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void manifestBindsVerifiedMembersFailureOwnerPolicyAndExactInstant() throws Exception {
        var f = twoMembers(); var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        var policy = selection(policy("account", false, 20));
        var instant = AT.plusNanos(123456789);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var result = DocumentPublicationAssessment.prepare(f.command(), policy, TYPED, fragments(f),
                Optional.of(f.assets().container().definition()), (member, occurrence) -> {
                    calls.incrementAndGet(); return member.getMemberId().equals("member-a") ? invalid.definition() : f.assets().payload().definition();
                }, budget, OPAQUE_LIMITS, instant, () -> {});
        try (result; var encoded = result.encodeDeclaredManifest(owner("account", f.command().operationId()), runtimeFixture(), () -> {})) {
            var decoded = decodeManifest(encoded, budget);
            assertThat(decoded.getCommandSha256()).isEqualTo(f.command().sha256());
            assertThat(decoded.getOperationId()).isEqualTo(f.command().operationId().toString());
            assertThat(decoded.getAccountId()).isEqualTo("account");
            assertThat(decoded.getPrincipal()).isEqualTo("principal");
            assertThat(decoded.getOwnerGeneration()).isEqualTo(4);
            assertThat(decoded.getPolicyRevision()).isEqualTo(policy.revision());
            assertThat(decoded.getPolicySha256()).isEqualTo(policy.policy().sha256());
            assertThat(decoded.getEvaluatedAt().getEpochSeconds()).isEqualTo(instant.getEpochSecond());
            assertThat(decoded.getEvaluatedAt().getNanos()).isEqualTo(instant.getNano());
            assertThat(decoded.getMembersList()).extracting(DocumentMemberAssessment::getMemberId).containsExactly("member-a", "member-b");
            assertThat(decoded.getFirstFailure().getMemberId()).isEqualTo("member-a");
            assertThat(decoded.getFirstFailure().getRuleId()).isEqualTo("invalid");
            var first = result.typed().get("member-a");
            assertThat(decoded.getMembers(0).getTyped().getContainer()).isEqualTo(first.references().getFirst().toProto());
            assertThat(decoded.getFirstFailure().getRoot().getSha256()).isEqualTo(first.roots().getFirst().encoded().sha256());
            assertThat(decoded.getFirstFailure().getOccurrence().getStepsList()).isEqualTo(first.failure().orElseThrow().occurrence());
            assertThat(calls.get()).isEqualTo(2);
            // The encoded buffer owns a separate reservation. Closing its source does not retain payload/schema bytes.
            result.close();
            assertThat(budget.reservedBytes()).isEqualTo(encoded.bytes().size());
            assertThat(decodeManifest(encoded, budget)).isEqualTo(decoded);
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void mixedAcceptedManifestPreservesOpaqueModeWithoutClaimingTypedValidation() throws Exception {
        var f = twoMembers(); var budget = new PayloadBudget(32_000_000);
        var modes = Map.of("member-a", DocumentPublicationCandidate.Mode.TYPED, "member-b", DocumentPublicationCandidate.Mode.OPAQUE);
        try (var result = assess(f, modes, selection(policy("account", true, 20)),
                (member, occurrence) -> f.assets().payload().definition(), budget);
             var encoded = result.encodeDeclaredManifest(owner("account", f.command().operationId()), runtimeFixture(), () -> {})) {
            var decoded = decodeManifest(encoded, budget);
            assertThat(decoded.hasFirstFailure()).isFalse();
            assertThat(decoded.getMembers(0).hasTyped()).isTrue();
            assertThat(decoded.getMembers(1).hasTyped()).isFalse();
            assertThat(decoded.getMembers(1).getOpaque()).isTrue();
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void allOpaqueManifestHasNoTypedEvidenceOrResolverCalls() throws Exception {
        var f = twoMembers(); var budget = new PayloadBudget(32_000_000);
        var modes = Map.of("member-a", DocumentPublicationCandidate.Mode.OPAQUE, "member-b", DocumentPublicationCandidate.Mode.OPAQUE);
        try (var result = DocumentPublicationAssessment.prepare(f.command(), selection(policy("account", true, 20)), modes,
                fragments(f), Optional.empty(), (member, occurrence) -> { throw new AssertionError("Opaque operation must not resolve schemas"); },
                budget, OPAQUE_LIMITS, AT, () -> {});
             var encoded = result.encodeDeclaredManifest(owner("account", f.command().operationId()), runtimeFixture(), () -> {})) {
            var decoded = decodeManifest(encoded, budget);
            assertThat(decoded.hasFirstFailure()).isFalse();
            assertThat(decoded.getMembersList()).allSatisfy(member -> {
                assertThat(member.hasTyped()).isFalse(); assertThat(member.getOpaque()).isTrue();
            });
            assertThat(result.artifacts()).isEmpty();
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void manifestFailureReleasesBusyStateAndReservationsForRetry() throws Exception {
        var f = twoMembers(); var budget = new PayloadBudget(32_000_000);
        try (var result = assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> f.assets().payload().definition(), budget)) {
            long owned = budget.reservedBytes();
            for (var owner : List.of(owner("other", f.command().operationId()), owner("account", java.util.UUID.randomUUID()))) {
                assertThatThrownBy(() -> result.encodeDeclaredManifest(owner, runtimeFixture(), () -> {})).hasMessageContaining("owner differs");
                assertThat(budget.reservedBytes()).isEqualTo(owned);
            }
            var owner = owner("account", f.command().operationId());
            assertThatThrownBy(() -> result.encodeDeclaredManifest(owner, DocumentAssessmentRuntime.getDefaultInstance(), () -> {}))
                    .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            var cancelled = new java.util.concurrent.CancellationException("manifest preparation cancelled");
            assertThatThrownBy(() -> result.encodeDeclaredManifest(owner, runtimeFixture(), () -> {
                if (budget.reservedBytes() > owned) throw cancelled;
            })).isSameAs(cancelled);
            assertThat(budget.reservedBytes()).isEqualTo(owned);
            try (var pressure = budget.reserve(budget.capacity() - owned)) {
                assertThatThrownBy(() -> result.encodeDeclaredManifest(owner, runtimeFixture(), () -> {}))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
            }
            var checked = new java.util.concurrent.atomic.AtomicBoolean();
            try (var encoded = result.encodeDeclaredManifest(owner, runtimeFixture(), () -> {
                if (checked.compareAndSet(false, true)) {
                    assertThatThrownBy(result::close).hasMessageContaining("verification is active");
                    assertThatThrownBy(() -> result.encodeDeclaredManifest(owner, runtimeFixture(), () -> {}))
                            .hasMessageContaining("verification is active");
                }
            })) {
                assertThat(budget.reservedBytes()).isEqualTo(owned + encoded.bytes().size());
            }
            assertThat(checked).isTrue();
            assertThat(budget.reservedBytes()).isEqualTo(owned);
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    private static RepositoryOperationLedger.Owner owner(String account, java.util.UUID operation) {
        return new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(account, "principal", operation),
                4, java.util.UUID.randomUUID(), AT);
    }
    private static DocumentAssessmentRuntime runtimeFixture() {
        return DocumentAssessmentRuntime.newBuilder().setValidationProfile(ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.PROFILE)
                .setCatalogConfiguration("empty-taxonomy-and-postal/v1")
                .addImplementationArtifacts(SchemaToolIdentity.newBuilder().setName("synthetic-runtime-fixture").setVersion("test").setArtifactSha256("f".repeat(64)))
                .setJvm(SchemaToolIdentity.newBuilder().setName("synthetic-jvm-fixture").setVersion("test")).build();
    }
    private static DocumentPublicationAssessmentManifest decodeManifest(
            ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec.Encoded encoded, PayloadBudget budget) throws Exception {
        return ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec.decode(
                ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec.CODEC, 1, encoded.bytes(), encoded.sha256(),
                bytes -> { var lease = budget.reserve(bytes); return lease::close; }, () -> {});
    }

    @Test void invalidMembersStillConsumeTheOperationArtifactLimit() throws Exception {
        var members = new ArrayList<DocumentPublicationMember>();
        var modes = new HashMap<String, DocumentPublicationCandidate.Mode>();
        var schemas = new HashMap<String, Asset>();
        for (int i = 0; i < 32; i++) {
            var id = "member-" + i;
            members.add(member(id, "doc-" + i)); modes.put(id, DocumentPublicationCandidate.Mode.TYPED);
            schemas.put(id, invalidSchema("invalid-" + i));
        }
        var f = command("account", members);
        var budget = new PayloadBudget(128_000_000);
        assertThatThrownBy(() -> assess(f, modes, selection(policy("account", false, 20)),
                (member, occurrence) -> schemas.get(member.getMemberId()).definition(), budget))
                .hasMessageContaining("Operation schema artifact union exceeds limit");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterOpaqueStructuralFailureCannotProduceAnInvalidOperationVerdict() throws Exception {
        var f = twoMembers();
        var changed = f.command().intent().toBuilder();
        changed.getMembersBuilder(1).setOwnership(changed.getMembers(1).getOwnership().toBuilder().setDatasourceId("different-source"));
        var command = new ai.protomolt.proto.repo.spi.DocumentPublicationCommand(changed.build());
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        assertThatThrownBy(() -> DocumentPublicationAssessment.prepare(command, selection(policy("account", true, 20)),
                Map.of("member-a", DocumentPublicationCandidate.Mode.TYPED, "member-b", DocumentPublicationCandidate.Mode.OPAQUE),
                fragments(f), Optional.of(f.assets().container().definition()), (member, occurrence) -> invalid.definition(),
                budget, OPAQUE_LIMITS, AT, () -> {})).hasMessageContaining("Decoded ownership differs");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterResolverFailureAndSharedCapacityFailureReleaseEverything() throws Exception {
        var f = twoMembers();
        var invalid = invalidSchema("invalid");
        var budget = new PayloadBudget(32_000_000);
        var cancelled = new java.util.concurrent.CancellationException("later member cancelled");
        assertThatThrownBy(() -> assess(f, TYPED, selection(policy("account", false, 20)), (member, occurrence) -> {
            if (member.getMemberId().equals("member-b")) throw cancelled;
            return invalid.definition();
        }, budget)).isSameAs(cancelled);
        assertThat(budget.reservedBytes()).isZero();
        var tiny = new PayloadBudget(fragmentBytes(f));
        assertThatThrownBy(() -> assess(f, TYPED, selection(policy("account", false, 20)),
                (member, occurrence) -> invalid.definition(), tiny))
                .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
        assertThat(tiny.reservedBytes()).isZero();
    }

    private static DocumentPublicationAssessment assess(CommandData f, Map<String, DocumentPublicationCandidate.Mode> modes,
            DocumentSchemaPolicies.Selection policy, DocumentPublicationCandidate.Resolver resolver, PayloadBudget budget) throws Exception {
        return DocumentPublicationAssessment.prepare(f.command(), policy, modes, fragments(f), Optional.of(f.assets().container().definition()),
                resolver, budget, OPAQUE_LIMITS, AT, () -> {});
    }
    private static CommandData twoMembers() throws Exception {
        return command("account", List.of(member("member-a", "doc-a"), member("member-b", "doc-b")));
    }
    private static Asset invalidSchema(String ruleId) throws Exception {
        return schema(ruleId, "false");
    }
    private static Asset schema(String ruleId, String expression) throws Exception {
        var proto = StringValue.getDescriptor().getFile().toProto().toBuilder().addDependency(ValidateProto.getDescriptor().getName());
        for (var type : proto.getMessageTypeBuilderList()) if (type.getName().equals("StringValue")) {
            type.setOptions(type.getOptions().toBuilder().setExtension(ValidateProto.message,
                    MessageRules.newBuilder().addCel(CelRule.newBuilder().setId(ruleId).setExpression(expression)).build()));
        }
        var file = FileDescriptor.buildFrom(proto.build(), new FileDescriptor[]{ValidateProto.getDescriptor()});
        return asset(file.findMessageTypeByName("StringValue"));
    }
}
