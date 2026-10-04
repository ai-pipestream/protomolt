package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentAssessmentContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test void acceptsExplicitMixedModesAndNanosecondEvaluationIdentity() throws Exception {
        check(manifest().build(), true);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(-1).setNanos(999999999)).build(), true);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.getDefaultInstance()).build(), true);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(-62135596800L)).build(), true);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(253402300799L).setNanos(999999999)).build(), true);
    }
    @Test void refusesMissingManifestIdentitiesAndPrecisionOverflow() throws Exception {
        var valid = manifest().build();
        for (var field : valid.getDescriptorForType().getFields()) {
            if (!field.getName().equals("first_failure")) check(valid.toBuilder().clearField(field).build(), false);
        }
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(-62135596801L)).build(), false);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(253402300800L)).build(), false);
        check(manifest().setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setNanos(1000000000)).build(), false);
        check(manifest().setOwnerGeneration(-1).build(), false);
        check(manifest().setPolicyRevision(-1).build(), false);
        check(manifest().setCommandVersion(2).build(), false);
        check(manifest().setCommandSha256("A".repeat(64)).build(), false);
    }
    @Test void refusesImplicitOrFalseOpaqueModesAndIncompleteTypedEvidence() throws Exception {
        check(DocumentMemberAssessment.newBuilder().setMemberId("m").build(), false);
        check(DocumentMemberAssessment.newBuilder().setMemberId("m").setOpaque(false).build(), false);
        check(DocumentMemberAssessment.newBuilder().setMemberId("m").setTyped(DocumentTypedAssessment.getDefaultInstance()).build(), false);
        var typed = typed().build();
        for (var field : typed.getDescriptorForType().getFields()) check(typed.toBuilder().clearField(field).build(), false);
        check(root().setOrdinal(10000).build(), false);
        check(root().setCodec("other").build(), false);
        check(root().setVersion(2).build(), false);
    }
    @Test void runtimeRequiresContentIdentityAndExplicitCatalogConfiguration() throws Exception {
        var runtime = runtime().build();
        for (var field : runtime.getDescriptorForType().getFields()) check(runtime.toBuilder().clearField(field).build(), false);
        check(runtime.toBuilder().setImplementationArtifacts(0, tool().clearArtifactSha256()).build(), false);
        check(runtime.toBuilder().setCatalogConfiguration("unspecified").build(), false);
        check(runtime.toBuilder().setValidationProfile("future-profile").build(), false);
        check(runtime.toBuilder().addAllImplementationArtifacts(java.util.Collections.nCopies(64, tool().build())).build(), false);
    }
    @Test void enforcesMemberAndReferenceCounts() throws Exception {
        check(manifest().addAllMembers(java.util.Collections.nCopies(63,
                DocumentMemberAssessment.newBuilder().setMemberId("extra").setOpaque(true).build())).build(), false);
        check(typed().addAllPayloadSchemas(java.util.Collections.nCopies(63, reference().build())).build(), false);
        check(typed().addAllRoots(java.util.Collections.nCopies(1024, root().build())).build(), false);
    }
    @Test void failureIdentityRequiresMemberRootAndCompleteOccurrence() throws Exception {
        check(DocumentAssessmentFailure.getDefaultInstance(), false);
        var failure = failure().build();
        check(failure, true);
        check(failure.toBuilder().clearMemberId().build(), false);
        check(failure.toBuilder().clearRoot().build(), false);
        check(failure.toBuilder().clearOccurrence().build(), false);
        check(failure.toBuilder().setOccurrence(RepositorySchemaOccurrencePath.getDefaultInstance()).build(), false);
        check(failure.toBuilder().setRuleId("x".repeat(16385)).build(), false);
        check(failure.toBuilder().setFieldPath("x".repeat(16385)).build(), false);
        check(failure.toBuilder().setRulePath("x".repeat(16385)).build(), false);
        check(manifest().setFirstFailure(failure).build(), true);
        check(manifest().setFirstFailure(failure.toBuilder().setMemberId("b")).build(), false);
        check(manifest().setFirstFailure(failure.toBuilder().setMemberId("missing")).build(), false);
        check(manifest().setFirstFailure(failure.toBuilder().setRoot(root().setOrdinal(1))).build(), false);
    }
    @Test void jsonSchemaExposesBoundsAndRuntimeOnlyConditions() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(DocumentAssessmentRuntime.getDescriptor()));
        assertThat(schema.at("/properties/implementationArtifacts/maxItems").asInt()).isEqualTo(64);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("assessment-runtime-artifacts");
    }
    private static DocumentPublicationAssessmentManifest.Builder manifest() {
        return DocumentPublicationAssessmentManifest.newBuilder().setEncodingVersion(1)
                .setOperationId("abcdefab-cdef-4abc-8def-abcdefabcdef").setAccountId("account").setPrincipal("principal")
                .setOwnerGeneration(1).setCommandSha256("a".repeat(64)).setCommandCodec("document-publication").setCommandVersion(1)
                .setPolicyRevision(1).setPolicySha256("b".repeat(64))
                .setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(946684800).setNanos(123456789))
                .setRuntime(runtime()).addMembers(DocumentMemberAssessment.newBuilder().setMemberId("a").setTyped(typed()))
                .addMembers(DocumentMemberAssessment.newBuilder().setMemberId("b").setOpaque(true));
    }
    private static DocumentTypedAssessment.Builder typed() {
        return DocumentTypedAssessment.newBuilder().setContainer(reference()).addPayloadSchemas(reference()).addRoots(root());
    }
    private static RepositorySchemaAssetReference.Builder reference() {
        return RepositorySchemaAssetReference.newBuilder().setTypeUrl("type.test/example.Payload").setDescriptorSha256("c".repeat(64))
                .setMetadataCodec("repository-schema-asset").setMetadataVersion(1).setMetadataSha256("d".repeat(64));
    }
    private static DocumentAssessmentRootReference.Builder root() {
        return DocumentAssessmentRootReference.newBuilder().setOrdinal(0).setCodec("document-root-schema-evidence").setVersion(1).setSha256("e".repeat(64));
    }
    private static DocumentAssessmentFailure.Builder failure() {
        var boundary = RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl("type.test/example.Payload").setValueSha256("a".repeat(64)).setValueSizeBytes(12)
                .setResolved(RepositoryResolvedSchema.newBuilder().setArtifactSha256("c".repeat(64))
                        .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("example.Payload").setDescriptorFingerprint("b".repeat(64)))));
        return DocumentAssessmentFailure.newBuilder().setMemberId("a").setRoot(root())
                .setOccurrence(RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary))
                .setRuleId("synthetic-rule");
    }
    private static SchemaToolIdentity.Builder tool() {
        return SchemaToolIdentity.newBuilder().setName("synthetic-contract-fixture").setVersion("test").setArtifactSha256("f".repeat(64));
    }
    private static DocumentAssessmentRuntime.Builder runtime() {
        return DocumentAssessmentRuntime.newBuilder().setValidationProfile("protomolt-retained-schema-admission/v1")
                .setCatalogConfiguration("empty-taxonomy-and-postal/v1").addImplementationArtifacts(tool())
                .setJvm(SchemaToolIdentity.newBuilder().setName("synthetic-jvm-fixture").setVersion("test"));
    }
    private static void check(Message value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString())).valid())
                .as("dynamic %s", value).isEqualTo(valid);
    }
}
