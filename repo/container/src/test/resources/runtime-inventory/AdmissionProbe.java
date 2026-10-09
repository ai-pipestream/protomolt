package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import build.buf.validate.FieldRules;
import build.buf.validate.StringRules;
import build.buf.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.time.Instant;

/** Test-only probe compiled against the generated production bundle, without test libraries. */
public final class AdmissionProbe {
    public static int run() throws Exception {
        var validator = DocumentSchemaAdmission.VALIDATOR;
        var at = Instant.parse("2000-01-01T00:00:00Z");
        var valid = RepositorySchemaAssetReference.newBuilder().setTypeUrl("type.example/Item")
                .setDescriptorSha256("a".repeat(64)).setMetadataCodec("repository-schema-asset")
                .setMetadataVersion(1).setMetadataSha256("b".repeat(64)).build();
        require(validator.firstViolation(valid, at).isEmpty(), "native valid");
        var nativeFailure = validator.firstViolation(valid.toBuilder().setMetadataVersion(2).build(), at).orElseThrow();
        require(nativeFailure.path().equals("metadata_version") && nativeFailure.ruleId().endsWith(".const"), "native constant rule");
        var field = FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING).setOptions(FieldOptions.newBuilder()
                        .setExtension(ValidateProto.field, FieldRules.newBuilder()
                                .setString(StringRules.newBuilder().setMinLen(3)).build()));
        var message = DescriptorProto.newBuilder().setName("Item").addField(field)
                .setOptions(MessageOptions.newBuilder().setExtension(ValidateProto.message,
                        build.buf.validate.MessageRules.newBuilder().addCel(build.buf.validate.Rule.newBuilder()
                                .setId("not-blocked").setMessage("blocked value").setExpression("this.name != 'blocked'"))
                                .build()));
        var file = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("runtime-probe.proto")
                .setPackage("probe").setSyntax("proto3").addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(message).build(), new FileDescriptor[]{ValidateProto.getDescriptor()});
        var descriptor = file.findMessageTypeByName("Item");
        validator.prepareSchema(descriptor, 10, 10, () -> {});
        var builder = DynamicMessage.newBuilder(descriptor);
        var name = descriptor.findFieldByName("name");
        require(validator.firstViolation(builder.setField(name, "valid").build(), at).isEmpty(), "buf valid");
        require(validator.firstViolation(builder.setField(name, "x").build(), at).orElseThrow().ruleId().equals("string.min_len"), "buf minimum length");
        require(validator.firstViolation(builder.setField(name, "blocked").build(), at).orElseThrow().ruleId().equals("not-blocked"), "buf CEL");
        return 5;
    }
    private static void require(boolean condition, String label) {
        if (!condition) throw new AssertionError("Production admission runtime failed: " + label);
    }
}
