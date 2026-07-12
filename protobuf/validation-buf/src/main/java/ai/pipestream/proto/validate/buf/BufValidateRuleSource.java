package ai.pipestream.proto.validate.buf;

import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * {@link ValidationRuleSource} for the protovalidate annotation dialect: reads
 * {@code (buf.validate.field)} and {@code (buf.validate.message)} options off
 * descriptors and translates them into the neutral rule model, so schemas annotated
 * for protovalidate validate through {@code ProtoValidator} unchanged. Registered via
 * {@code ServiceLoader} — adding this module to the classpath enables the dialect.
 *
 * <p>The vendored {@code buf/validate/validate.proto} is pinned and attributed in this
 * module's {@code NOTICE}; its semantics are defined by the upstream project.
 *
 * <p>Coverage notes (translated ↔ skipped): all scalar/collection rule families map
 * onto the neutral model, including every integer variant with correct unsigned
 * semantics, and {@code const} on timestamp/duration translates to equal upper and
 * lower bounds. Not yet translated: byte-length string rules ({@code len_bytes},
 * {@code min_bytes}, {@code max_bytes} — the core counts code points), bytes
 * {@code pattern}/{@code in}/{@code not_in}/{@code ip}, exotic well-known string
 * formats ({@code uri_ref}, {@code address}, {@code tuuid}, prefix-length IP forms,
 * {@code well_known_regex}), duration {@code in}/{@code not_in}, {@code Any} and
 * {@code FieldMask} rules, predefined-rule extensions, message {@code oneof} rules,
 * and the non-trivial {@code ignore} modes ({@code IGNORE_ALWAYS} is honored).
 * Protovalidate CEL rules referencing its custom function library are translated
 * verbatim and will report an evaluation-error violation until that library is
 * available in the CEL environment.
 */
public final class BufValidateRuleSource implements ValidationRuleSource {

    @Override
    public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        var options = field.getOptions();
        if (!options.hasExtension(ValidateProto.field)) {
            return Optional.empty();
        }
        FieldRules rules = options.getExtension(ValidateProto.field);
        return Optional.of(toFieldConstraints(rules));
    }

    @Override
    public Optional<MessageConstraints> messageConstraints(Descriptor message) {
        var options = message.getOptions();
        if (!options.hasExtension(ValidateProto.message)) {
            return Optional.empty();
        }
        MessageRules rules = options.getExtension(ValidateProto.message);
        List<CelConstraint> cel = new ArrayList<>(rules.getCelList().size());
        for (Rule rule : rules.getCelList()) {
            cel.add(toCel(rule));
        }
        return Optional.of(new MessageConstraints(cel));
    }

    private static IgnoreMode toIgnoreMode(Ignore ignore) {
        return switch (ignore) {
            case IGNORE_ALWAYS -> IgnoreMode.ALWAYS;
            case IGNORE_IF_ZERO_VALUE -> IgnoreMode.IF_ZERO_VALUE;
            default -> IgnoreMode.UNSPECIFIED;
        };
    }

    /** Recursively translates {@link FieldRules} (also used for items/keys/values). */
    private static FieldConstraints toFieldConstraints(FieldRules rules) {
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
            case REPEATED -> builder.repeated(toRepeated(rules.getRepeated()));
            case MAP -> builder.map(toMap(rules.getMap()));
            case TIMESTAMP -> builder.timestamp(toTimestamp(rules.getTimestamp()));
            case DURATION -> builder.duration(toDuration(rules.getDuration()));
            case ANY, FIELD_MASK, TYPE_NOT_SET -> {
            }
        }
        for (Rule cel : rules.getCelList()) {
            builder.addCel(toCel(cel));
        }
        return builder.build();
    }

    private static CelConstraint toCel(Rule rule) {
        return new CelConstraint(rule.getId(), rule.getExpression(), rule.getMessage());
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
            default -> {
                // *_with_prefixlen and well_known_regex: no core evaluator yet — skipped.
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
        // pattern / in / not_in / ip forms are not yet evaluable by the core — skipped.
        return new BytesConstraints(
                r.hasLen() ? OptionalLong.of(r.getLen()) : OptionalLong.empty(),
                r.hasMinLen() ? OptionalLong.of(r.getMinLen()) : OptionalLong.empty(),
                r.hasMaxLen() ? OptionalLong.of(r.getMaxLen()) : OptionalLong.empty(),
                r.hasPrefix() ? Optional.of(r.getPrefix()) : Optional.empty(),
                r.hasSuffix() ? Optional.of(r.getSuffix()) : Optional.empty(),
                r.hasContains() ? Optional.of(r.getContains()) : Optional.empty());
    }

    private static EnumConstraints toEnum(EnumRules r) {
        return new EnumConstraints(
                r.hasConst() ? OptionalInt.of(r.getConst()) : OptionalInt.empty(),
                r.hasDefinedOnly() && r.getDefinedOnly(),
                r.getInList(),
                r.getNotInList());
    }

    private static RepeatedConstraints toRepeated(RepeatedRules r) {
        return new RepeatedConstraints(
                r.hasMinItems() ? OptionalLong.of(r.getMinItems()) : OptionalLong.empty(),
                r.hasMaxItems() ? OptionalLong.of(r.getMaxItems()) : OptionalLong.empty(),
                r.hasUnique() && r.getUnique(),
                r.hasItems() ? Optional.of(toFieldConstraints(r.getItems())) : Optional.empty());
    }

    private static MapConstraints toMap(MapRules r) {
        return new MapConstraints(
                r.hasMinPairs() ? OptionalLong.of(r.getMinPairs()) : OptionalLong.empty(),
                r.hasMaxPairs() ? OptionalLong.of(r.getMaxPairs()) : OptionalLong.empty(),
                r.hasKeys() ? Optional.of(toFieldConstraints(r.getKeys())) : Optional.empty(),
                r.hasValues() ? Optional.of(toFieldConstraints(r.getValues())) : Optional.empty());
    }

    private static TimestampConstraints toTimestamp(TimestampRules r) {
        // const translates to equal lower and upper bounds.
        Optional<Instant> constant = r.hasConst()
                ? Optional.of(toInstant(r.getConst())) : Optional.empty();
        return new TimestampConstraints(
                r.hasGt() ? Optional.of(toInstant(r.getGt())) : Optional.empty(),
                constant.or(() -> r.hasGte()
                        ? Optional.of(toInstant(r.getGte())) : Optional.empty()),
                r.hasLt() ? Optional.of(toInstant(r.getLt())) : Optional.empty(),
                constant.or(() -> r.hasLte()
                        ? Optional.of(toInstant(r.getLte())) : Optional.empty()),
                r.getLtNow(),
                r.getGtNow(),
                r.hasWithin() ? Optional.of(toJavaDuration(r.getWithin())) : Optional.empty());
    }

    private static DurationConstraints toDuration(DurationRules r) {
        // const translates to equal lower and upper bounds; in / not_in are skipped.
        Optional<java.time.Duration> constant = r.hasConst()
                ? Optional.of(toJavaDuration(r.getConst())) : Optional.empty();
        return new DurationConstraints(
                r.hasGt() ? Optional.of(toJavaDuration(r.getGt())) : Optional.empty(),
                constant.or(() -> r.hasGte()
                        ? Optional.of(toJavaDuration(r.getGte())) : Optional.empty()),
                r.hasLt() ? Optional.of(toJavaDuration(r.getLt())) : Optional.empty(),
                constant.or(() -> r.hasLte()
                        ? Optional.of(toJavaDuration(r.getLte())) : Optional.empty()));
    }

    private static Instant toInstant(com.google.protobuf.Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private static java.time.Duration toJavaDuration(com.google.protobuf.Duration d) {
        return java.time.Duration.ofSeconds(d.getSeconds(), d.getNanos());
    }
}
