package ai.pipestream.proto.mesh;

import ai.pipestream.proto.mesh.v1.EntityEnvelope;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the mesh contract's own validate.v1 annotations compile and evaluate: the dogfood gate.
 * Every boundary calls {@link MeshValidation} on write, so a regression here means a malformed
 * entity could cross into routing, persistence, or execution.
 */
class MeshContractValidationTest {

    private static ValidationResult validate(EntityEnvelope entity) {
        return ProtoValidator.forMessageType(EntityEnvelope.getDescriptor()).validate(entity);
    }

    private static void assertRuleFails(EntityEnvelope entity, String ruleId) {
        ValidationResult result = validate(entity);
        assertThat(result.valid()).as("expected rule %s to fail", ruleId).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo(ruleId));
    }

    @Test
    void aWellFormedInlineEntityValidates() {
        ValidationResult result = validate(MeshFixtures.inlineEntity().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aWellFormedClaimCheckEntityValidates() {
        ValidationResult result = validate(MeshFixtures.claimCheckEntity().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aWellFormedChildEntityValidates() {
        ValidationResult result = validate(MeshFixtures.childEntity().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void anEntityCarryingBothBodiesFailsTheExclusivityCel() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck())
                .build();
        assertRuleFails(entity, "exactly-one-body");
    }

    @Test
    void anEntityCarryingNoBodyFailsTheExclusivityCel() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .clearPayload()
                .build();
        assertRuleFails(entity, "exactly-one-body");
    }

    @Test
    void aClaimCheckThatDisagreesWithTheSchemaFails() {
        EntityEnvelope entity = MeshFixtures.claimCheckEntity()
                .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck().toBuilder()
                        .setPayloadTypeName("ai.pipestream.proto.mesh.test.v1.TestResult"))
                .build();
        assertRuleFails(entity, "claim-check-names-schema");
    }

    @Test
    void aRootEntityWithDepthFailsTheParentDepthCel() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                        .setScopeDepth(1))
                .build();
        assertRuleFails(entity, "parent-implies-depth");
    }

    @Test
    void aChildEntityWithoutDepthFailsTheParentDepthCel() {
        EntityEnvelope entity = MeshFixtures.childEntity()
                .setHeader(MeshFixtures.childEntity().getHeader().toBuilder()
                        .setScopeDepth(0))
                .build();
        assertRuleFails(entity, "parent-implies-depth");
    }

    @Test
    void aDeadlineBeforeCreationFailsTheDeadlineCel() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                        .setDeadline(Timestamp.newBuilder().setSeconds(1_600_000_000L)))
                .build();
        assertRuleFails(entity, "deadline-after-creation");
    }

    @Test
    void anInvalidTypeNameFailsTheSchemaFieldRule() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder()
                        .setTypeName("not a type name"))
                .build();
        ValidationResult result = validate(entity);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("type_name"));
    }

    @Test
    void anUppercaseFingerprintFailsTheSchemaFieldRule() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setSchema(MeshFixtures.schema().toBuilder()
                        .setDescriptorFingerprint("A".repeat(64)))
                .build();
        ValidationResult result = validate(entity);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("descriptor_fingerprint"));
    }

    @Test
    void aNonUuidEntityIdFailsTheHeaderFieldRule() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                        .setEntityId("not-a-uuid"))
                .build();
        ValidationResult result = validate(entity);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("entity_id"));
    }

    @Test
    void aMissingCompletionPolicyFailsTheHeaderFieldRule() {
        EntityEnvelope entity = MeshFixtures.inlineEntity()
                .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                        .clearCompletionPolicy())
                .build();
        ValidationResult result = validate(entity);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("completion_policy"));
    }
}
