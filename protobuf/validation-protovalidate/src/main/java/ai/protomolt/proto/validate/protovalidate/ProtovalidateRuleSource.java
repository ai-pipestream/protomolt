package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import build.buf.validate.FieldRules;
import build.buf.validate.MessageRules;
import build.buf.validate.Rule;
import build.buf.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@link ValidationRuleSource} for the <a href="https://github.com/bufbuild/protovalidate">
 * protovalidate</a> annotation standard: reads {@code (buf.validate.field)} and
 * {@code (buf.validate.message)} options off descriptors and translates them into the neutral
 * rule model, so schemas annotated for protovalidate validate through {@code ProtoValidator}
 * unchanged. Registered via {@code ServiceLoader} — adding this module to the classpath enables
 * the standard.
 *
 * <p>The protovalidate standard and its {@code buf/validate/validate.proto} schema were created
 * by Buf; this project consumes that schema verbatim and aims to be a compatible superset — every
 * conformant protovalidate annotation validates identically here, while this project may extend
 * the rule set further. The vendored {@code validate.proto} is pinned and attributed in this
 * module's {@code NOTICE}, and its wire types remain under the {@code build.buf.validate} package
 * for exact compatibility.
 *
 * <p>Coverage notes: every rule family maps onto the neutral model — all scalar
 * variants with correct unsigned semantics, string byte-length rules, bytes
 * {@code pattern}/{@code in}/{@code not_in} and well-known formats, string
 * well-known formats including the prefix-length IP forms and
 * {@code well_known_regex}, collection rules, {@code Any} and {@code FieldMask}
 * rules, message {@code oneof} rules, the {@code ignore} modes, custom CEL rules
 * (the protovalidate CEL function library is registered in the validator's
 * environment), and predefined rules ({@code (buf.validate.predefined)} CEL
 * extensions on {@code buf.validate.<T>Rules} fields), which are translated into
 * CEL constraints with their configured value bound as {@code rule}.
 *
 * <p>Descriptors linked without the {@code buf.validate} extension registry (for
 * example from a {@code FileDescriptorSet} parsed with plain {@code parseFrom})
 * keep the rule annotations only as unknown fields; this source detects that and
 * reparses the options against its own registry, so rules are never silently
 * dropped.
 *
 * <p>This class is the SPI surface and the descriptor-level checks: reading the options off a
 * field or message, and refusing a rule family that cannot legally annotate the field it is on.
 * The rule-by-rule mapping onto the neutral model lives in {@link FieldRuleTranslation}, the
 * predefined-rule lookup in {@link PredefinedRules}, and the unknown-field recovery in
 * {@link OptionReparse}.
 */
public final class ProtovalidateRuleSource implements ValidationRuleSource {

    @Override
    public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        FieldRules rules = fieldRules(field.getOptions());
        if (rules == null) {
            return Optional.empty();
        }
        checkRuleType(field, rules);
        return Optional.of(FieldRuleTranslation.toFieldConstraints(
                rules, PredefinedRules.indexFor(field.getFile())));
    }

    /**
     * The {@code (buf.validate.field)} rules on {@code options}, or null when absent. Descriptors
     * linked without the buf.validate extension registry carry the annotation only as an unknown
     * field; reparse the options against a knowing registry rather than silently dropping rules.
     */
    private static FieldRules fieldRules(DescriptorProtos.FieldOptions options) {
        options = OptionReparse.recover(options, ValidateProto.field.getNumber(),
                DescriptorProtos.FieldOptions::parseFrom, "(buf.validate.field)");
        return options.hasExtension(ValidateProto.field) ? options.getExtension(ValidateProto.field) : null;
    }

    /**
     * Rejects a type-specific rule whose type does not match the field it annotates (e.g. double
     * rules on an int32 field, or scalar rules on a {@code google.protobuf.Any}). protovalidate
     * treats this as a compile-time error rather than silently ignoring the rule.
     */
    private static void checkRuleType(FieldDescriptor field, FieldRules rules) {
        FieldRules.TypeCase actual = rules.getTypeCase();
        if (actual == FieldRules.TypeCase.TYPE_NOT_SET) {
            return;
        }
        FieldRules.TypeCase expected = expectedTypeCase(field);
        if (actual != expected) {
            throw new RuleCompilationException("mismatched rule type and field type");
        }
    }

    /** The single {@link FieldRules.TypeCase} that may legally annotate {@code field}. */
    private static FieldRules.TypeCase expectedTypeCase(FieldDescriptor field) {
        if (field.isMapField()) {
            return FieldRules.TypeCase.MAP;
        }
        if (field.isRepeated()) {
            return FieldRules.TypeCase.REPEATED;
        }
        return switch (field.getType()) {
            case INT32 -> FieldRules.TypeCase.INT32;
            case INT64 -> FieldRules.TypeCase.INT64;
            case UINT32 -> FieldRules.TypeCase.UINT32;
            case UINT64 -> FieldRules.TypeCase.UINT64;
            case SINT32 -> FieldRules.TypeCase.SINT32;
            case SINT64 -> FieldRules.TypeCase.SINT64;
            case FIXED32 -> FieldRules.TypeCase.FIXED32;
            case FIXED64 -> FieldRules.TypeCase.FIXED64;
            case SFIXED32 -> FieldRules.TypeCase.SFIXED32;
            case SFIXED64 -> FieldRules.TypeCase.SFIXED64;
            case FLOAT -> FieldRules.TypeCase.FLOAT;
            case DOUBLE -> FieldRules.TypeCase.DOUBLE;
            case BOOL -> FieldRules.TypeCase.BOOL;
            case STRING -> FieldRules.TypeCase.STRING;
            case BYTES -> FieldRules.TypeCase.BYTES;
            case ENUM -> FieldRules.TypeCase.ENUM;
            case MESSAGE, GROUP -> messageTypeCase(field.getMessageType().getFullName());
        };
    }

    /** Well-known and wrapper message types accept a specific rule type; others accept none. */
    private static FieldRules.TypeCase messageTypeCase(String fullName) {
        return switch (fullName) {
            case "google.protobuf.Timestamp" -> FieldRules.TypeCase.TIMESTAMP;
            case "google.protobuf.Duration" -> FieldRules.TypeCase.DURATION;
            case "google.protobuf.Any" -> FieldRules.TypeCase.ANY;
            case "google.protobuf.FieldMask" -> FieldRules.TypeCase.FIELD_MASK;
            case "google.protobuf.Int32Value" -> FieldRules.TypeCase.INT32;
            case "google.protobuf.Int64Value" -> FieldRules.TypeCase.INT64;
            case "google.protobuf.UInt32Value" -> FieldRules.TypeCase.UINT32;
            case "google.protobuf.UInt64Value" -> FieldRules.TypeCase.UINT64;
            case "google.protobuf.FloatValue" -> FieldRules.TypeCase.FLOAT;
            case "google.protobuf.DoubleValue" -> FieldRules.TypeCase.DOUBLE;
            case "google.protobuf.BoolValue" -> FieldRules.TypeCase.BOOL;
            case "google.protobuf.StringValue" -> FieldRules.TypeCase.STRING;
            case "google.protobuf.BytesValue" -> FieldRules.TypeCase.BYTES;
            // A plain message field admits no type-specific rule; anything set is a mismatch.
            default -> FieldRules.TypeCase.TYPE_NOT_SET;
        };
    }

    @Override
    public Optional<MessageConstraints> messageConstraints(Descriptor message) {
        // Required protobuf oneofs are declared on the oneof, not via the message extension, so a
        // message can carry oneof rules with no (buf.validate.message) option at all.
        List<String> requiredOneofs = requiredOneofs(message);
        MessageRules rules = messageRules(message.getOptions());
        if (rules == null) {
            return requiredOneofs.isEmpty()
                    ? Optional.empty()
                    : Optional.of(new MessageConstraints(List.of(), List.of(), requiredOneofs));
        }
        List<CelConstraint> cel = new ArrayList<>(rules.getCelList().size());
        for (Rule rule : rules.getCelList()) {
            cel.add(FieldRuleTranslation.toCel(rule));
        }
        for (String expression : rules.getCelExpressionList()) {
            cel.add(new CelConstraint("", expression, "", "cel_expression"));
        }
        List<MessageConstraints.Oneof> oneofs = new ArrayList<>(rules.getOneofCount());
        for (build.buf.validate.MessageOneofRule oneof : rules.getOneofList()) {
            oneofs.add(toOneof(oneof, message));
        }
        return Optional.of(new MessageConstraints(cel, oneofs, requiredOneofs));
    }

    /** As {@link #fieldRules} for the {@code (buf.validate.message)} extension. */
    private static MessageRules messageRules(DescriptorProtos.MessageOptions options) {
        options = OptionReparse.recover(options, ValidateProto.message.getNumber(),
                DescriptorProtos.MessageOptions::parseFrom, "(buf.validate.message)");
        return options.hasExtension(ValidateProto.message) ? options.getExtension(ValidateProto.message) : null;
    }

    /** Names of the message's real protobuf oneofs annotated {@code (buf.validate.oneof).required}. */
    private static List<String> requiredOneofs(Descriptor message) {
        List<String> names = new ArrayList<>();
        for (com.google.protobuf.Descriptors.OneofDescriptor oneof : message.getRealOneofs()) {
            var opts = OptionReparse.recover(oneof.getOptions(), ValidateProto.oneof.getNumber(),
                    DescriptorProtos.OneofOptions::parseFrom, "(buf.validate.oneof)");
            if (opts.hasExtension(ValidateProto.oneof)
                    && opts.getExtension(ValidateProto.oneof).getRequired()) {
                names.add(oneof.getName());
            }
        }
        return names;
    }

    /**
     * Translates a message {@code oneof} rule, validating it against the message descriptor. buf's
     * conformance suite expects malformed oneof rules to surface as compilation errors, so an empty
     * field list, an unknown field name, or a duplicated field name throws
     * {@link RuleCompilationException} with buf's exact wording.
     */
    private static MessageConstraints.Oneof toOneof(
            build.buf.validate.MessageOneofRule oneof, Descriptor message) {
        List<String> fields = oneof.getFieldsList();
        if (fields.isEmpty()) {
            throw new RuleCompilationException(
                    "at least one field must be specified in oneof rule for the message "
                            + message.getFullName());
        }
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        for (String name : fields) {
            if (message.findFieldByName(name) == null) {
                throw new RuleCompilationException(
                        "field " + name + " not found in message " + message.getFullName());
            }
            if (!seen.add(name)) {
                throw new RuleCompilationException(
                        "duplicate " + name + " in oneof rule for the message " + message.getFullName());
            }
        }
        return new MessageConstraints.Oneof(fields, oneof.getRequired());
    }
}
