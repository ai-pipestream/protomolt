package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.model.AnyConstraints;
import ai.protomolt.proto.validate.model.BoolConstraints;
import ai.protomolt.proto.validate.model.BytesConstraints;
import ai.protomolt.proto.validate.model.BytesFormat;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.DurationConstraints;
import ai.protomolt.proto.validate.model.EnumConstraints;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FieldMaskConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.HttpHeaderRule;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.IntegralConstraints;
import ai.protomolt.proto.validate.model.MapConstraints;
import ai.protomolt.proto.validate.model.RepeatedConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.model.StringFormat;
import ai.protomolt.proto.validate.model.TimestampConstraints;
import build.buf.validate.BoolRules;
import build.buf.validate.BytesRules;
import build.buf.validate.DoubleRules;
import build.buf.validate.DurationRules;
import build.buf.validate.EnumRules;
import build.buf.validate.FieldMaskRules;
import build.buf.validate.FieldRules;
import build.buf.validate.Fixed32Rules;
import build.buf.validate.Fixed64Rules;
import build.buf.validate.FloatRules;
import build.buf.validate.Ignore;
import build.buf.validate.Int32Rules;
import build.buf.validate.Int64Rules;
import build.buf.validate.MapRules;
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

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Translates one {@code buf.validate} {@link FieldRules} message into the neutral
 * {@link FieldConstraints} model, recursing through {@code items}, {@code keys} and
 * {@code values}.
 *
 * <p>The translation is mechanical and deliberately exhaustive: protovalidate declares a separate
 * rules message per scalar width ({@code Int32Rules}, {@code SFixed64Rules}, …) while the neutral
 * model has one {@link IntegralConstraints} family, so each width gets a converter that also
 * records the rule-id prefix and whether comparisons are unsigned. Collapsing them would lose the
 * prefix a violation reports.
 *
 * <p>No descriptor is consulted here — whether a rule family may legally annotate a given field is
 * {@link ProtovalidateRuleSource}'s check, made before this translation runs.
 */
final class FieldRuleTranslation {

    private FieldRuleTranslation() {
    }

    static IgnoreMode toIgnoreMode(Ignore ignore) {
        return switch (ignore) {
            case IGNORE_ALWAYS -> IgnoreMode.ALWAYS;
            case IGNORE_IF_ZERO_VALUE -> IgnoreMode.IF_ZERO_VALUE;
            default -> IgnoreMode.UNSPECIFIED;
        };
    }

    /** Recursively translates {@link FieldRules} (also used for items/keys/values). */
    static FieldConstraints toFieldConstraints(FieldRules rules, PredefinedIndex predefined) {
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
            case ANY -> builder.any(new AnyConstraints(
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
        PredefinedRules.addPredefinedCel(builder, rules, predefined);
        return builder.build();
    }

    /** A {@code buf.validate} CEL rule as a neutral constraint; the message-rule path shares it. */
    static CelConstraint toCel(Rule rule) {
        return new CelConstraint(rule.getId(), rule.getExpression(), rule.getMessage());
    }

    private static FieldMaskConstraints toFieldMask(
            FieldMaskRules r) {
        return new FieldMaskConstraints(
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
                r.getInList().stream().map(FieldRuleTranslation::toJavaDuration).toList(),
                r.getNotInList().stream().map(FieldRuleTranslation::toJavaDuration).toList());
    }

    private static Instant toInstant(com.google.protobuf.Timestamp ts) {
        try {
            return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        } catch (DateTimeException | ArithmeticException e) {
            // A rule bound that cannot be represented is a schema error, not a raw leak.
            throw new RuleCompilationException("timestamp rule value out of range: " + e.getMessage(), e);
        }
    }

    private static Duration toJavaDuration(com.google.protobuf.Duration d) {
        try {
            return Duration.ofSeconds(d.getSeconds(), d.getNanos());
        } catch (DateTimeException | ArithmeticException e) {
            throw new RuleCompilationException("duration rule value out of range: " + e.getMessage(), e);
        }
    }
}
