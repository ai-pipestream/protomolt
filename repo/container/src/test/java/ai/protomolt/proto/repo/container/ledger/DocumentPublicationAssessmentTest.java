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
        var proto = StringValue.getDescriptor().getFile().toProto().toBuilder().addDependency(ValidateProto.getDescriptor().getName());
        for (var type : proto.getMessageTypeBuilderList()) if (type.getName().equals("StringValue")) {
            type.setOptions(type.getOptions().toBuilder().setExtension(ValidateProto.message,
                    MessageRules.newBuilder().addCel(CelRule.newBuilder().setId(ruleId).setExpression("false")).build()));
        }
        var file = FileDescriptor.buildFrom(proto.build(), new FileDescriptor[]{ValidateProto.getDescriptor()});
        return asset(file.findMessageTypeByName("StringValue"));
    }
}
