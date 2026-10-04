package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaPolicyValidationTest {
    private static final String SHA = "a".repeat(64);
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource()));

    @Test void validatesTypedAndExplicitOpaquePoliciesAsGeneratedAndDynamicMessages() throws Exception {
        var typed = valid().setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setAnyResolvedSchema(true).build();
        validBoth(typed);
        var allowListed = valid().setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(binding())).build();
        validBoth(allowListed);
        var opaque = valid().setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).build();
        validBoth(opaque);
        invalidBoth(opaque.toBuilder().setRequireStructuredRoot(true).build());
    }

    @Test void requiresAnEligibilityOneofAndTypedPermissionForRequiredStructuredRoot() throws Exception {
        var typed = valid().setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).build();
        invalidBoth(typed);
        invalidBoth(valid().setAnyResolvedSchema(false).build());
        invalidBoth(valid().setModeValue(0).setAnyResolvedSchema(true).build());
        invalidBoth(valid().setModeValue(99).setAnyResolvedSchema(true).build());
        invalidBoth(valid().setEncodingVersion(2).setAnyResolvedSchema(true).build());
        invalidBoth(valid().setValidationProfile("other/v1").setAnyResolvedSchema(true).build());
        invalidBoth(valid().setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setRequireStructuredRoot(true).setAnyResolvedSchema(true).build());
    }

    @Test void checksExactTypeUrlAgainstFullNameAndRequiresSchemaIdentity() throws Exception {
        var malformed = binding().toBuilder().setTypeUrl("type.test/other.Record").build();
        invalidBoth(valid().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(malformed)).build());
        invalidBoth(valid().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(
                binding().toBuilder().setTypeUrl("example.Record").build())).build());
        invalidBoth(valid().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(
                binding().toBuilder().clearSchema().build())).build());
        validBoth(valid().setAllowedSchemas(DocumentSchemaPolicyAllowList.newBuilder().addBindings(binding())).build());
    }

    @Test void validatesEveryResourceLimitAtItsLowerAndUpperBound() throws Exception {
        var base = limits();
        validBoth(valid().setAnyResolvedSchema(true).build());
        validBoth(valid().setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder()
                .setMaxFragments(10000).setMaxFragmentBytes(268435456L).setMaxRoots(1024)
                .setMaxEvidenceBytes(16777216L).setMaxBindings(64).setMaxRetainedBytes(67108864L)
                .setMaxDecodedBytes(268435456)).build());
        invalidBoth(valid().clearLimits().setAnyResolvedSchema(true).build());
        var boundaries = List.of(
                new LimitCase("max_fragments", b -> b.setMaxFragments(0), b -> b.setMaxFragments(10001)),
                new LimitCase("max_fragment_bytes", b -> b.setMaxFragmentBytes(0), b -> b.setMaxFragmentBytes(268435457L)),
                new LimitCase("max_roots", b -> b.setMaxRoots(0), b -> b.setMaxRoots(1025)),
                new LimitCase("max_evidence_bytes", b -> b.setMaxEvidenceBytes(0), b -> b.setMaxEvidenceBytes(16777217L)),
                new LimitCase("max_bindings", b -> b.setMaxBindings(0), b -> b.setMaxBindings(65)),
                new LimitCase("max_retained_bytes", b -> b.setMaxRetainedBytes(0), b -> b.setMaxRetainedBytes(67108865L)),
                new LimitCase("max_decoded_bytes", b -> b.setMaxDecodedBytes(0), b -> b.setMaxDecodedBytes(268435457)));
        for (var c : boundaries) {
            validBoth(valid().setLimits(base.toBuilder().build()).setAnyResolvedSchema(true).build());
            invalidBoth(valid().setLimits(c.low.apply(base.toBuilder()).build()).setAnyResolvedSchema(true).build());
            invalidBoth(valid().setLimits(c.high.apply(base.toBuilder()).build()).setAnyResolvedSchema(true).build());
        }
    }

    @Test void allowlistMustBeNonemptyAndCannotExceedItsItemLimit() throws Exception {
        invalidBoth(valid().setAllowedSchemas(DocumentSchemaPolicyAllowList.getDefaultInstance()).build());
        var large = DocumentSchemaPolicyAllowList.newBuilder();
        for (int i = 0; i < 1025; i++) large.addBindings(binding().toBuilder()
                .setTypeUrl("type.test/example.Record"));
        invalidBoth(valid().setAllowedSchemas(large).build());
        invalidBoth(valid().setAccountId("  ").setAnyResolvedSchema(true).build());
        validBoth(valid().setAnyResolvedSchema(true).build());
    }

    @Test void jsonSchemaExposesPolicyFieldsAndRuntimeOnlyCrossFieldRules() {
        var mapper = new ObjectMapper();
        JsonNode schema = mapper.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(DocumentSchemaPolicy.getDescriptor()));
        assertThat(schema.at("/properties/encodingVersion/const").asInt()).isEqualTo(1);
        assertThat(schema.at("/properties/accountId/minLength").asInt()).isEqualTo(1);
        assertThat(schema.at("/$defs/ai.protomolt.proto.repo.v1.DocumentSchemaPolicyLimits/properties/maxRoots/maximum")
                .asInt()).isEqualTo(1024);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("schema-policy-eligibility", "schema-policy-required-root");
        JsonNode bindingSchema = mapper.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(DocumentSchemaPolicyBinding.getDescriptor()));
        assertThat(bindingSchema.path("x-protomolt-cel").toString()).contains("schema-policy-binding-url");
    }

    private static DocumentSchemaPolicy.Builder valid() {
        return DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account-1")
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setLimits(limits());
    }

    private static DocumentSchemaPolicyLimits limits() {
        return DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(1).setMaxFragmentBytes(1)
                .setMaxRoots(1).setMaxEvidenceBytes(1).setMaxBindings(1).setMaxRetainedBytes(1)
                .setMaxDecodedBytes(1).build();
    }

    private static DocumentSchemaPolicyBinding binding() {
        return DocumentSchemaPolicyBinding.newBuilder().setTypeUrl("type.test/example.Record")
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("example.Record")
                        .setDescriptorFingerprint(SHA)).build();
    }

    private static void validBoth(Message message) throws Exception { checkBoth(message, true); }
    private static void invalidBoth(Message message) throws Exception { checkBoth(message, false); }
    private static void checkBoth(Message message, boolean valid) throws Exception {
        for (Message candidate : List.of(message,
                DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray()))) {
            ValidationResult result = VALIDATOR.validate(candidate);
            assertThat(result.valid()).as("%s: %s", candidate.getDescriptorForType().getFullName(), result)
                    .isEqualTo(valid);
        }
    }

    @FunctionalInterface private interface LimitEdit { DocumentSchemaPolicyLimits.Builder apply(DocumentSchemaPolicyLimits.Builder value); }
    private record LimitCase(String name, LimitEdit low, LimitEdit high) {}
}
