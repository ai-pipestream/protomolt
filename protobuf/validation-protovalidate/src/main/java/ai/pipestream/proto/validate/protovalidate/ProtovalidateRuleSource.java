package ai.pipestream.proto.validate.protovalidate;

import ai.pipestream.proto.validate.RuleCompilationException;
import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.BytesFormat;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
import ai.pipestream.proto.validate.model.HttpHeaderRule;
import ai.pipestream.proto.validate.model.IgnoreMode;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MapConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.RepeatedConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.model.StringFormat;
import ai.pipestream.proto.validate.model.TimestampConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import build.buf.validate.BoolRules;
import build.buf.validate.BytesRules;
import build.buf.validate.DoubleRules;
import build.buf.validate.DurationRules;
import build.buf.validate.EnumRules;
import build.buf.validate.FieldRules;
import build.buf.validate.Fixed32Rules;
import build.buf.validate.Fixed64Rules;
import build.buf.validate.FloatRules;
import build.buf.validate.Ignore;
import build.buf.validate.Int32Rules;
import build.buf.validate.Int64Rules;
import build.buf.validate.MapRules;
import build.buf.validate.MessageRules;
import build.buf.validate.RepeatedRules;
import build.buf.validate.Rule;
import build.buf.validate.SFixed32Rules;
import build.buf.validate.SFixed64Rules;
import build.buf.validate.SInt32Rules;
import build.buf.validate.SInt64Rules;
import build.buf.validate.StringRules;
import build.buf.validate.TimestampRules;
import build.buf.validate.UInt32Rules;
import build.buf.validate.UInt64Rules;
import build.buf.validate.ValidateProto;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

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
 */
public final class ProtovalidateRuleSource implements ValidationRuleSource {

    /** Registry knowing the {@code buf.validate} option extensions, for reparsing options. */
    private static final ExtensionRegistry EXTENSIONS = createExtensionRegistry();

    // Predefined-rule extension index per proto file (built from the file and its transitive
    // imports). Simple clear-on-threshold bound; wiped and repopulated on demand when full.
    private static final int MAX_CACHED_FILES = 64;
    private static final java.util.Map<FileDescriptor, PredefinedIndex> PREDEFINED_INDEXES =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static ExtensionRegistry createExtensionRegistry() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidateProto.registerAllExtensions(registry);
        return registry;
    }

    @Override
    public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        FieldRules rules = fieldRules(field.getOptions());
        if (rules == null) {
            return Optional.empty();
        }
        checkRuleType(field, rules);
        return Optional.of(toFieldConstraints(rules, predefinedIndex(field.getFile())));
    }

    /**
     * The {@code (buf.validate.field)} rules on {@code options}, or null when absent. Descriptors
     * linked without the buf.validate extension registry carry the annotation only as an unknown
     * field; reparse the options against a knowing registry rather than silently dropping rules.
     */
    private static FieldRules fieldRules(DescriptorProtos.FieldOptions options) {
        if (options.hasExtension(ValidateProto.field)) {
            return options.getExtension(ValidateProto.field);
        }
        if (!options.getUnknownFields().hasField(ValidateProto.field.getNumber())) {
            return null;
        }
        return reparse(options, DescriptorProtos.FieldOptions::parseFrom, "(buf.validate.field)")
                .getExtension(ValidateProto.field);
    }

    /** Parses {@code options}' bytes against {@link #EXTENSIONS}; failures are schema errors. */
    private static <T extends com.google.protobuf.Message> T reparse(
            T options, OptionsParser<T> parser, String extensionName) {
        try {
            return parser.parse(options.toByteString(), EXTENSIONS);
        } catch (InvalidProtocolBufferException e) {
            throw new RuleCompilationException(
                    "cannot reparse options carrying " + extensionName + ": " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface OptionsParser<T> {
        T parse(com.google.protobuf.ByteString bytes, ExtensionRegistry registry)
                throws InvalidProtocolBufferException;
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
            cel.add(toCel(rule));
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
        if (options.hasExtension(ValidateProto.message)) {
            return options.getExtension(ValidateProto.message);
        }
        if (!options.getUnknownFields().hasField(ValidateProto.message.getNumber())) {
            return null;
        }
        return reparse(options, DescriptorProtos.MessageOptions::parseFrom, "(buf.validate.message)")
                .getExtension(ValidateProto.message);
    }

    /** Names of the message's real protobuf oneofs annotated {@code (buf.validate.oneof).required}. */
    private static List<String> requiredOneofs(Descriptor message) {
        List<String> names = new ArrayList<>();
        for (com.google.protobuf.Descriptors.OneofDescriptor oneof : message.getRealOneofs()) {
            var opts = oneof.getOptions();
            if (!opts.hasExtension(ValidateProto.oneof)
                    && opts.getUnknownFields().hasField(ValidateProto.oneof.getNumber())) {
                opts = reparse(opts, DescriptorProtos.OneofOptions::parseFrom, "(buf.validate.oneof)");
            }
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

    private static IgnoreMode toIgnoreMode(Ignore ignore) {
        return switch (ignore) {
            case IGNORE_ALWAYS -> IgnoreMode.ALWAYS;
            case IGNORE_IF_ZERO_VALUE -> IgnoreMode.IF_ZERO_VALUE;
            default -> IgnoreMode.UNSPECIFIED;
        };
    }

    /** Recursively translates {@link FieldRules} (also used for items/keys/values). */
    private static FieldConstraints toFieldConstraints(FieldRules rules, PredefinedIndex predefined) {
        FieldConstraints.Builder builder = FieldConstraints.builder()
                .required(rules.hasRequired() && rules.getRequired())
                .ignore(toIgnoreMode(rules.getIgnore()));
        switch (rules.getTypeCase()) {
            case STRING -> builder.string(toStringConstraints(rules.getString()));
            case INT32 -> builder.integral(toInt32(rules.getInt32()));
            case INT64 -> builder.integral(toInt64(rules.getInt64()));
            case UINT32 -> builder.integral(toUInt32(rules.getUint32()));
            case UINT64 -> builder.integral(toUInt64(rules.getUint64()));
            case SINT32 -> builder.integral(toSInt32(rules.getSint32()));
            case SINT64 -> builder.integral(toSInt64(rules.getSint64()));
            case FIXED32 -> builder.integral(toFixed32(rules.getFixed32()));
            case FIXED64 -> builder.integral(toFixed64(rules.getFixed64()));
            case SFIXED32 -> builder.integral(toSFixed32(rules.getSfixed32()));
            case SFIXED64 -> builder.integral(toSFixed64(rules.getSfixed64()));
            case FLOAT -> builder.floating(toFloat(rules.getFloat()));
            case DOUBLE -> builder.floating(toDouble(rules.getDouble()));
            case BOOL -> builder.bool(toBool(rules.getBool()));
            case BYTES -> builder.bytes(toBytes(rules.getBytes()));
            case ENUM -> builder.enumeration(toEnum(rules.getEnum()));
            case REPEATED -> builder.repeated(toRepeated(rules.getRepeated(), predefined));
            case MAP -> builder.map(toMap(rules.getMap(), predefined));
            case TIMESTAMP -> builder.timestamp(toTimestamp(rules.getTimestamp()));
            case DURATION -> builder.duration(toDuration(rules.getDuration()));
            case ANY -> builder.any(new ai.pipestream.proto.validate.model.AnyConstraints(
                    rules.getAny().getInList(), rules.getAny().getNotInList()));
            case FIELD_MASK -> builder.fieldMask(toFieldMask(rules.getFieldMask()));
            case TYPE_NOT_SET -> {
            }
        }
        for (Rule cel : rules.getCelList()) {
            builder.addCel(toCel(cel));
        }
        // cel_expression is the shorthand form: a bare expression whose id is the expression text.
        for (String expression : rules.getCelExpressionList()) {
            builder.addCel(new CelConstraint("", expression, "", "cel_expression"));
        }
        addPredefinedCel(builder, rules, predefined);
        return builder.build();
    }

    private static CelConstraint toCel(Rule rule) {
        return new CelConstraint(rule.getId(), rule.getExpression(), rule.getMessage());
    }

    private static ai.pipestream.proto.validate.model.FieldMaskConstraints toFieldMask(
            build.buf.validate.FieldMaskRules r) {
        return new ai.pipestream.proto.validate.model.FieldMaskConstraints(
                r.hasConst() ? Optional.of(String.join(",", r.getConst().getPathsList())) : Optional.empty(),
                r.getInList(),
                r.getNotInList());
    }

    private static StringConstraints toStringConstraints(StringRules r) {
        StringConstraints.Builder b = StringConstraints.builder();
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasLen()) {
            b.len(r.getLen());
        }
        if (r.hasMinLen()) {
            b.minLen(r.getMinLen());
        }
        if (r.hasMaxLen()) {
            b.maxLen(r.getMaxLen());
        }
        if (r.hasLenBytes()) {
            b.lenBytes(r.getLenBytes());
        }
        if (r.hasMinBytes()) {
            b.minBytes(r.getMinBytes());
        }
        if (r.hasMaxBytes()) {
            b.maxBytes(r.getMaxBytes());
        }
        if (r.hasPattern() && !r.getPattern().isEmpty()) {
            b.pattern(r.getPattern());
        }
        if (r.hasPrefix()) {
            b.prefix(r.getPrefix());
        }
        if (r.hasSuffix()) {
            b.suffix(r.getSuffix());
        }
        if (r.hasContains()) {
            b.contains(r.getContains());
        }
        if (r.hasNotContains()) {
            b.notContains(r.getNotContains());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        switch (r.getWellKnownCase()) {
            case EMAIL -> applyFlag(b, StringFormat.EMAIL, r.getEmail());
            case HOSTNAME -> applyFlag(b, StringFormat.HOSTNAME, r.getHostname());
            case IP -> applyFlag(b, StringFormat.IP, r.getIp());
            case IPV4 -> applyFlag(b, StringFormat.IPV4, r.getIpv4());
            case IPV6 -> applyFlag(b, StringFormat.IPV6, r.getIpv6());
            case URI -> applyFlag(b, StringFormat.URI, r.getUri());
            case URI_REF -> applyFlag(b, StringFormat.URI_REF, r.getUriRef());
            case ADDRESS -> applyFlag(b, StringFormat.ADDRESS, r.getAddress());
            case UUID -> applyFlag(b, StringFormat.UUID, r.getUuid());
            case TUUID -> applyFlag(b, StringFormat.TUUID, r.getTuuid());
            case ULID -> applyFlag(b, StringFormat.ULID, r.getUlid());
            case IP_PREFIX -> applyFlag(b, StringFormat.IP_PREFIX, r.getIpPrefix());
            case IPV4_PREFIX -> applyFlag(b, StringFormat.IPV4_PREFIX, r.getIpv4Prefix());
            case IPV6_PREFIX -> applyFlag(b, StringFormat.IPV6_PREFIX, r.getIpv6Prefix());
            case HOST_AND_PORT -> applyFlag(b, StringFormat.HOST_AND_PORT, r.getHostAndPort());
            case IP_WITH_PREFIXLEN -> applyFlag(b, StringFormat.IP_WITH_PREFIXLEN, r.getIpWithPrefixlen());
            case IPV4_WITH_PREFIXLEN ->
                    applyFlag(b, StringFormat.IPV4_WITH_PREFIXLEN, r.getIpv4WithPrefixlen());
            case IPV6_WITH_PREFIXLEN ->
                    applyFlag(b, StringFormat.IPV6_WITH_PREFIXLEN, r.getIpv6WithPrefixlen());
            case PROTOBUF_FQN -> applyFlag(b, StringFormat.PROTOBUF_FQN, r.getProtobufFqn());
            case PROTOBUF_DOT_FQN -> applyFlag(b, StringFormat.PROTOBUF_DOT_FQN, r.getProtobufDotFqn());
            default -> {
            }
        }
        boolean strict = !r.hasStrict() || r.getStrict();
        switch (r.getWellKnownRegex()) {
            case KNOWN_REGEX_HTTP_HEADER_NAME ->
                    b.httpHeader(strict ? HttpHeaderRule.NAME_STRICT : HttpHeaderRule.NAME_LOOSE);
            case KNOWN_REGEX_HTTP_HEADER_VALUE ->
                    b.httpHeader(strict ? HttpHeaderRule.VALUE_STRICT : HttpHeaderRule.VALUE_LOOSE);
            default -> {
            }
        }
        return b.build();
    }

    private static void applyFlag(StringConstraints.Builder b, StringFormat format, boolean on) {
        if (on) {
            b.format(format);
        }
    }

    private static IntegralConstraints toInt32(Int32Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("int32");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList().stream().map(Integer::longValue).toList());
        b.notIn(r.getNotInList().stream().map(Integer::longValue).toList());
        return b.build();
    }

    private static IntegralConstraints toInt64(Int64Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("int64");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        return b.build();
    }

    private static IntegralConstraints toSInt32(SInt32Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("sint32");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList().stream().map(Integer::longValue).toList());
        b.notIn(r.getNotInList().stream().map(Integer::longValue).toList());
        return b.build();
    }

    private static IntegralConstraints toSInt64(SInt64Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("sint64");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        return b.build();
    }

    private static IntegralConstraints toSFixed32(SFixed32Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("sfixed32");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList().stream().map(Integer::longValue).toList());
        b.notIn(r.getNotInList().stream().map(Integer::longValue).toList());
        return b.build();
    }

    private static IntegralConstraints toSFixed64(SFixed64Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.builder("sfixed64");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        return b.build();
    }

    private static IntegralConstraints toUInt32(UInt32Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.unsignedBuilder("uint32");
        if (r.hasConst()) {
            b.constant(Integer.toUnsignedLong(r.getConst()));
        }
        if (r.hasGt()) {
            b.gt(Integer.toUnsignedLong(r.getGt()));
        }
        if (r.hasGte()) {
            b.gte(Integer.toUnsignedLong(r.getGte()));
        }
        if (r.hasLt()) {
            b.lt(Integer.toUnsignedLong(r.getLt()));
        }
        if (r.hasLte()) {
            b.lte(Integer.toUnsignedLong(r.getLte()));
        }
        b.in(r.getInList().stream().map(Integer::toUnsignedLong).toList());
        b.notIn(r.getNotInList().stream().map(Integer::toUnsignedLong).toList());
        return b.build();
    }

    private static IntegralConstraints toUInt64(UInt64Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.unsignedBuilder("uint64");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        return b.build();
    }

    private static IntegralConstraints toFixed32(Fixed32Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.unsignedBuilder("fixed32");
        if (r.hasConst()) {
            b.constant(Integer.toUnsignedLong(r.getConst()));
        }
        if (r.hasGt()) {
            b.gt(Integer.toUnsignedLong(r.getGt()));
        }
        if (r.hasGte()) {
            b.gte(Integer.toUnsignedLong(r.getGte()));
        }
        if (r.hasLt()) {
            b.lt(Integer.toUnsignedLong(r.getLt()));
        }
        if (r.hasLte()) {
            b.lte(Integer.toUnsignedLong(r.getLte()));
        }
        b.in(r.getInList().stream().map(Integer::toUnsignedLong).toList());
        b.notIn(r.getNotInList().stream().map(Integer::toUnsignedLong).toList());
        return b.build();
    }

    private static IntegralConstraints toFixed64(Fixed64Rules r) {
        IntegralConstraints.Builder b = IntegralConstraints.unsignedBuilder("fixed64");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        return b.build();
    }

    private static FloatingConstraints toFloat(FloatRules r) {
        FloatingConstraints.Builder b = FloatingConstraints.builder("float");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList().stream().map(Float::doubleValue).toList());
        b.notIn(r.getNotInList().stream().map(Float::doubleValue).toList());
        b.finite(r.hasFinite() && r.getFinite());
        return b.build();
    }

    private static FloatingConstraints toDouble(DoubleRules r) {
        FloatingConstraints.Builder b = FloatingConstraints.builder("double");
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasGt()) {
            b.gt(r.getGt());
        }
        if (r.hasGte()) {
            b.gte(r.getGte());
        }
        if (r.hasLt()) {
            b.lt(r.getLt());
        }
        if (r.hasLte()) {
            b.lte(r.getLte());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        b.finite(r.hasFinite() && r.getFinite());
        return b.build();
    }

    private static BoolConstraints toBool(BoolRules r) {
        return new BoolConstraints(
                r.hasConst() ? Optional.of(r.getConst()) : Optional.empty());
    }

    private static BytesConstraints toBytes(BytesRules r) {
        BytesConstraints.Builder b = BytesConstraints.builder();
        if (r.hasConst()) {
            b.constant(r.getConst());
        }
        if (r.hasLen()) {
            b.len(r.getLen());
        }
        if (r.hasMinLen()) {
            b.minLen(r.getMinLen());
        }
        if (r.hasMaxLen()) {
            b.maxLen(r.getMaxLen());
        }
        if (r.hasPrefix()) {
            b.prefix(r.getPrefix());
        }
        if (r.hasSuffix()) {
            b.suffix(r.getSuffix());
        }
        if (r.hasContains()) {
            b.contains(r.getContains());
        }
        if (r.hasPattern() && !r.getPattern().isEmpty()) {
            b.pattern(r.getPattern());
        }
        b.in(r.getInList());
        b.notIn(r.getNotInList());
        switch (r.getWellKnownCase()) {
            case IP -> applyBytesFlag(b, BytesFormat.IP, r.getIp());
            case IPV4 -> applyBytesFlag(b, BytesFormat.IPV4, r.getIpv4());
            case IPV6 -> applyBytesFlag(b, BytesFormat.IPV6, r.getIpv6());
            case UUID -> applyBytesFlag(b, BytesFormat.UUID, r.getUuid());
            default -> {
            }
        }
        return b.build();
    }

    private static void applyBytesFlag(BytesConstraints.Builder b, BytesFormat format, boolean on) {
        if (on) {
            b.format(format);
        }
    }

    private static EnumConstraints toEnum(EnumRules r) {
        return new EnumConstraints(
                r.hasConst() ? OptionalInt.of(r.getConst()) : OptionalInt.empty(),
                r.hasDefinedOnly() && r.getDefinedOnly(),
                r.getInList(),
                r.getNotInList());
    }

    private static RepeatedConstraints toRepeated(RepeatedRules r, PredefinedIndex predefined) {
        return new RepeatedConstraints(
                r.hasMinItems() ? OptionalLong.of(r.getMinItems()) : OptionalLong.empty(),
                r.hasMaxItems() ? OptionalLong.of(r.getMaxItems()) : OptionalLong.empty(),
                r.hasUnique() && r.getUnique(),
                r.hasItems()
                        ? Optional.of(toFieldConstraints(r.getItems(), predefined))
                        : Optional.empty());
    }

    private static MapConstraints toMap(MapRules r, PredefinedIndex predefined) {
        return new MapConstraints(
                r.hasMinPairs() ? OptionalLong.of(r.getMinPairs()) : OptionalLong.empty(),
                r.hasMaxPairs() ? OptionalLong.of(r.getMaxPairs()) : OptionalLong.empty(),
                r.hasKeys()
                        ? Optional.of(toFieldConstraints(r.getKeys(), predefined))
                        : Optional.empty(),
                r.hasValues()
                        ? Optional.of(toFieldConstraints(r.getValues(), predefined))
                        : Optional.empty());
    }

    private static TimestampConstraints toTimestamp(TimestampRules r) {
        return new TimestampConstraints(
                r.hasConst() ? Optional.of(toInstant(r.getConst())) : Optional.empty(),
                r.hasGt() ? Optional.of(toInstant(r.getGt())) : Optional.empty(),
                r.hasGte() ? Optional.of(toInstant(r.getGte())) : Optional.empty(),
                r.hasLt() ? Optional.of(toInstant(r.getLt())) : Optional.empty(),
                r.hasLte() ? Optional.of(toInstant(r.getLte())) : Optional.empty(),
                r.getLtNow(),
                r.getGtNow(),
                r.hasWithin() ? Optional.of(toJavaDuration(r.getWithin())) : Optional.empty());
    }

    private static DurationConstraints toDuration(DurationRules r) {
        return new DurationConstraints(
                r.hasConst() ? Optional.of(toJavaDuration(r.getConst())) : Optional.empty(),
                r.hasGt() ? Optional.of(toJavaDuration(r.getGt())) : Optional.empty(),
                r.hasGte() ? Optional.of(toJavaDuration(r.getGte())) : Optional.empty(),
                r.hasLt() ? Optional.of(toJavaDuration(r.getLt())) : Optional.empty(),
                r.hasLte() ? Optional.of(toJavaDuration(r.getLte())) : Optional.empty(),
                r.getInList().stream().map(ProtovalidateRuleSource::toJavaDuration).toList(),
                r.getNotInList().stream().map(ProtovalidateRuleSource::toJavaDuration).toList());
    }

    private static Instant toInstant(com.google.protobuf.Timestamp ts) {
        try {
            return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        } catch (java.time.DateTimeException | ArithmeticException e) {
            // A rule bound that cannot be represented is a schema error, not a raw leak.
            throw new RuleCompilationException("timestamp rule value out of range: " + e.getMessage(), e);
        }
    }

    private static java.time.Duration toJavaDuration(com.google.protobuf.Duration d) {
        try {
            return java.time.Duration.ofSeconds(d.getSeconds(), d.getNanos());
        } catch (java.time.DateTimeException | ArithmeticException e) {
            throw new RuleCompilationException("duration rule value out of range: " + e.getMessage(), e);
        }
    }

    // ---- predefined rules ((buf.validate.predefined) CEL extensions on <T>Rules fields) ----

    /** One predefined extension: its descriptor and the CEL rules attached to it. */
    private record PredefinedExt(FieldDescriptor descriptor, List<Rule> rules) {
    }

    /** rulesTypeFullName (e.g. {@code buf.validate.Int32Rules}) → extension number → extension. */
    private record PredefinedIndex(java.util.Map<String, java.util.Map<Integer, PredefinedExt>> bySubRules) {
        static final PredefinedIndex EMPTY = new PredefinedIndex(java.util.Map.of());

        java.util.Map<Integer, PredefinedExt> forType(String rulesTypeFullName) {
            return bySubRules.getOrDefault(rulesTypeFullName, java.util.Map.of());
        }

        boolean isEmpty() {
            return bySubRules.isEmpty();
        }
    }

    /** The predefined extensions visible from {@code file} (itself plus transitive imports). */
    private static PredefinedIndex predefinedIndex(FileDescriptor file) {
        PredefinedIndex existing = PREDEFINED_INDEXES.get(file);
        if (existing != null) {
            return existing;
        }
        if (PREDEFINED_INDEXES.size() >= MAX_CACHED_FILES) {
            PREDEFINED_INDEXES.clear();
        }
        return PREDEFINED_INDEXES.computeIfAbsent(file, ProtovalidateRuleSource::buildPredefinedIndex);
    }

    private static PredefinedIndex buildPredefinedIndex(FileDescriptor file) {
        java.util.Map<String, java.util.Map<Integer, PredefinedExt>> index = new java.util.HashMap<>();
        collectPredefined(file, new java.util.HashSet<>(), index);
        return index.isEmpty() ? PredefinedIndex.EMPTY : new PredefinedIndex(index);
    }

    private static void collectPredefined(
            FileDescriptor file,
            java.util.Set<String> visited,
            java.util.Map<String, java.util.Map<Integer, PredefinedExt>> index) {
        if (!visited.add(file.getFullName())) {
            return;
        }
        indexExtensions(file.getExtensions(), index);
        for (Descriptor message : file.getMessageTypes()) {
            indexNested(message, index);
        }
        for (FileDescriptor dep : file.getDependencies()) {
            collectPredefined(dep, visited, index);
        }
    }

    private static void indexNested(
            Descriptor message, java.util.Map<String, java.util.Map<Integer, PredefinedExt>> index) {
        indexExtensions(message.getExtensions(), index);
        for (Descriptor nested : message.getNestedTypes()) {
            indexNested(nested, index);
        }
    }

    private static void indexExtensions(
            List<FieldDescriptor> extensions,
            java.util.Map<String, java.util.Map<Integer, PredefinedExt>> index) {
        for (FieldDescriptor ext : extensions) {
            String containing = ext.getContainingType().getFullName();
            if (!containing.startsWith("buf.validate.")) {
                continue;
            }
            List<Rule> rules = predefinedRules(ext);
            if (rules.isEmpty()) {
                continue;
            }
            index.computeIfAbsent(containing, k -> new java.util.HashMap<>())
                    .put(ext.getNumber(), new PredefinedExt(ext, rules));
        }
    }

    /** Reads the {@code (buf.validate.predefined).cel} rules off an extension's options. */
    private static List<Rule> predefinedRules(FieldDescriptor ext) {
        DescriptorProtos.FieldOptions options = ext.getOptions();
        if (!options.hasExtension(ValidateProto.predefined)) {
            if (!options.getUnknownFields().hasField(ValidateProto.predefined.getNumber())) {
                return List.of();
            }
            // Custom options on a dynamically linked descriptor may survive only as unknown fields.
            options = reparse(options, DescriptorProtos.FieldOptions::parseFrom,
                    "(buf.validate.predefined)");
            if (!options.hasExtension(ValidateProto.predefined)) {
                return List.of();
            }
        }
        return options.getExtension(ValidateProto.predefined).getCelList();
    }

    /**
     * Appends a CEL constraint for every predefined extension set on {@code rules}' active
     * sub-rules message, with the extension's configured value carried as the {@code rule}
     * binding and an extension-shaped rule path ({@code <type>.[<ext.full.name>]}).
     */
    private static void addPredefinedCel(
            FieldConstraints.Builder builder, FieldRules rules, PredefinedIndex predefined) {
        if (predefined.isEmpty()) {
            return;
        }
        com.google.protobuf.Message subRules = activeSubRules(rules);
        if (subRules == null) {
            return;
        }
        java.util.Map<Integer, PredefinedExt> byNumber =
                predefined.forType(subRules.getDescriptorForType().getFullName());
        if (byNumber.isEmpty()) {
            return;
        }
        String subField = typeFieldName(rules.getTypeCase());
        for (int number : extensionNumbersSetOn(subRules)) {
            PredefinedExt ext = byNumber.get(number);
            if (ext == null) {
                continue;
            }
            Object ruleValue = ruleValue(subRules, ext.descriptor());
            String rulePath = subField + ".[" + ext.descriptor().getFullName() + "]";
            for (Rule rule : ext.rules()) {
                builder.addCel(new CelConstraint(
                        rule.getId(), rule.getExpression(), rule.getMessage(),
                        "cel", rulePath, ruleValue));
            }
        }
    }

    /**
     * The field numbers of extensions set on {@code subRules}: unknown fields (the common case —
     * user extensions are unknown to the generated {@code buf.validate} types) plus any extension
     * fields known to the message's registry.
     */
    private static List<Integer> extensionNumbersSetOn(com.google.protobuf.Message subRules) {
        java.util.TreeSet<Integer> numbers =
                new java.util.TreeSet<>(subRules.getUnknownFields().asMap().keySet());
        for (FieldDescriptor fd : subRules.getAllFields().keySet()) {
            if (fd.isExtension()) {
                numbers.add(fd.getNumber());
            }
        }
        return List.copyOf(numbers);
    }

    /** The set type sub-rules message (int32/float/…/repeated/map), or null when none is set. */
    private static com.google.protobuf.Message activeSubRules(FieldRules rules) {
        FieldDescriptor typeField = rules.getDescriptorForType()
                .findFieldByName(typeFieldName(rules.getTypeCase()));
        if (typeField == null || !rules.hasField(typeField)) {
            return null;
        }
        return (com.google.protobuf.Message) rules.getField(typeField);
    }

    private static String typeFieldName(FieldRules.TypeCase typeCase) {
        return switch (typeCase) {
            case TYPE_NOT_SET -> "";
            default -> typeCase.name().toLowerCase(java.util.Locale.ROOT);
        };
    }

    /**
     * Reads the value the predefined extension is set to on {@code subRules}, as the CEL value the
     * rule's {@code rule} variable binds to. The extension is typically an unknown field on our
     * generated sub-rules message, so the message bytes are re-parsed against the extension's own
     * descriptor to recover a typed value. An undecodable value is a schema error.
     */
    private static Object ruleValue(com.google.protobuf.Message subRules, FieldDescriptor ext) {
        try {
            ExtensionRegistry registry = ExtensionRegistry.newInstance();
            if (ext.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                registry.add(ext, DynamicMessage.getDefaultInstance(ext.getMessageType()));
            } else {
                registry.add(ext);
            }
            DynamicMessage parsed = DynamicMessage.parseFrom(
                    ext.getContainingType(), subRules.toByteString(), registry);
            return celValue(ext, parsed.getField(ext));
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new RuleCompilationException(
                    "cannot decode predefined rule value " + ext.getFullName() + ": " + e.getMessage(), e);
        }
    }

    /** Converts a protobuf field value to the Java type CEL expects for that proto type. */
    private static Object celValue(FieldDescriptor fd, Object value) {
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            for (Object element : list) {
                converted.add(scalarCelValue(fd, element));
            }
            return converted;
        }
        return scalarCelValue(fd, value);
    }

    private static Object scalarCelValue(FieldDescriptor fd, Object value) {
        return switch (fd.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) value));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) value);
            case INT32, SINT32, SFIXED32 -> ((Integer) value).longValue();
            case FLOAT -> ((Float) value).doubleValue();
            case ENUM -> (long) ((com.google.protobuf.Descriptors.EnumValueDescriptor) value).getNumber();
            case MESSAGE, GROUP -> messageCelValue((com.google.protobuf.Message) value);
            default -> value;
        };
    }

    /** Well-known temporal messages map to their CEL temporal representation. */
    private static Object messageCelValue(com.google.protobuf.Message value) {
        Descriptor descriptor = value.getDescriptorForType();
        return switch (descriptor.getFullName()) {
            case "google.protobuf.Duration" -> java.time.Duration.ofSeconds(
                    (Long) value.getField(descriptor.findFieldByName("seconds")),
                    (Integer) value.getField(descriptor.findFieldByName("nanos")));
            case "google.protobuf.Timestamp" -> Instant.ofEpochSecond(
                    (Long) value.getField(descriptor.findFieldByName("seconds")),
                    (Integer) value.getField(descriptor.findFieldByName("nanos")));
            default -> value;
        };
    }
}
