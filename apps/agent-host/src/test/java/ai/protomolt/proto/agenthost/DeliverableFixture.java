package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;

import java.util.Base64;

/**
 * A deliverable type built in the test: {@code delivery.v1.ReviewReport} with a required
 * {@code headline} of at least eight characters and a message rule that {@code findings}
 * must be positive. Mirrors the fixture the delegation module tests its gate with.
 */
final class DeliverableFixture {

    static final String TYPE_NAME = "delivery.v1.ReviewReport";
    static final String TYPE_URL = "type.googleapis.com/" + TYPE_NAME;
    static final String CEL_MESSAGE = "a review report must count at least one finding";
    private static final FileDescriptorSet SET = buildSet();

    private DeliverableFixture() {
    }

    static DeliverableContract contract() {
        return DeliverableContract.newBuilder()
                .setDescriptorSet(SET.toByteString())
                .setTypeName(TYPE_NAME)
                .build();
    }

    static String descriptorSetBase64() {
        return Base64.getEncoder().encodeToString(SET.toByteArray());
    }

    private static FileDescriptorSet buildSet() {
        FieldDescriptorProto headline = FieldDescriptorProto.newBuilder()
                .setName("headline")
                .setNumber(1)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setJsonName("headline")
                .setOptions(FieldOptions.newBuilder()
                        .setExtension(ValidateProto.field, FieldRules.newBuilder()
                                .setRequired(true)
                                .setString(StringRules.newBuilder()
                                        .setMinLen(8)
                                        .setMaxLen(120)
                                        .build())
                                .build()))
                .build();
        FieldDescriptorProto findings = FieldDescriptorProto.newBuilder()
                .setName("findings")
                .setNumber(2)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                .setJsonName("findings")
                .build();
        DescriptorProto report = DescriptorProto.newBuilder()
                .setName("ReviewReport")
                .addField(headline)
                .addField(findings)
                .setOptions(MessageOptions.newBuilder()
                        .setExtension(ValidateProto.message, MessageRules.newBuilder()
                                .addCel(CelRule.newBuilder()
                                        .setId("report-counts-findings")
                                        .setMessage(CEL_MESSAGE)
                                        .setExpression("this.findings > 0")
                                        .build())
                                .build()))
                .build();
        return FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("delivery/v1/review_report.proto")
                        .setPackage("delivery.v1")
                        .setSyntax("proto3")
                        .addMessageType(report)
                        .build())
                .build();
    }
}
