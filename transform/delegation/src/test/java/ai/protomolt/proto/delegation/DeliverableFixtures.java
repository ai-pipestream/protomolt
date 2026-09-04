package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

/**
 * A deliverable contract built in the test rather than compiled from a checked-in proto:
 * one message with a declared field rule and a message-level CEL rule, so the reducer's
 * contract gate is exercised against rules that travel in the contract's own descriptor
 * set instead of rules the running process happens to have on its classpath.
 *
 * <pre>
 *   package delivery.v1;
 *   message ReviewReport {
 *     option (validate.message) = {cel: {id: "report-counts-findings", ...
 *                                        expression: "this.findings &gt; 0"}};
 *     string headline = 1 [(validate.field) = {required: true,
 *                                              string: {min_len: 8, max_len: 120}}];
 *     int32 findings = 2;
 *   }
 * </pre>
 */
final class DeliverableFixtures {

    /** The deliverable message a task's contract names. */
    static final String TYPE_NAME = "delivery.v1.ReviewReport";

    /** What the declared field rule says when the headline is too short. */
    static final String HEADLINE_RULE = "string.min_len";

    /** The message-level CEL rule's own message. */
    static final String CEL_MESSAGE = "a review report must count at least one finding";

    private static final FileDescriptorSet SET = buildSet();
    private static final Descriptor REPORT = buildReportType();

    private DeliverableFixtures() {
    }

    /** The serialized descriptor set a contract carries. */
    static ByteString descriptorSet() {
        return SET.toByteString();
    }

    /** A contract naming the fixture's message, with no rendered schema yet. */
    static DeliverableContract contract() {
        return contract(TYPE_NAME);
    }

    /** A contract naming {@code typeName} against the fixture's descriptor set. */
    static DeliverableContract contract(String typeName) {
        return DeliverableContract.newBuilder()
                .setDescriptorSet(descriptorSet())
                .setTypeName(typeName)
                .build();
    }

    /** The linked descriptor of the deliverable message. */
    static Descriptor reportType() {
        return REPORT;
    }

    /** One deliverable packed as the Any a candidate carries. */
    static Any result(String headline, int findings) {
        DynamicMessage report = DynamicMessage.newBuilder(REPORT)
                .setField(REPORT.findFieldByName("headline"), headline)
                .setField(REPORT.findFieldByName("findings"), findings)
                .build();
        return Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + TYPE_NAME)
                .setValue(report.toByteString())
                .build();
    }

    /** A deliverable of a type the contract does not name. */
    static Any resultOfAnotherType() {
        return Any.pack(Timestamp.newBuilder().setSeconds(1_700_000_000L).build());
    }

    private static Descriptor buildReportType() {
        try {
            FileDescriptor file = FileDescriptor.buildFrom(
                    SET.getFile(0), new FileDescriptor[0]);
            return file.findMessageTypeByName("ReviewReport");
        } catch (Exception e) {
            throw new IllegalStateException("the fixture descriptor set does not link", e);
        }
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
