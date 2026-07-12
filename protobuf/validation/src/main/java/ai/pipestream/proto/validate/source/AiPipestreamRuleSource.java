package ai.pipestream.proto.validate.source;

import ai.pipestream.proto.validate.BoolRules;
import ai.pipestream.proto.validate.BytesRules;
import ai.pipestream.proto.validate.CelRule;
import ai.pipestream.proto.validate.DoubleRules;
import ai.pipestream.proto.validate.DurationRules;
import ai.pipestream.proto.validate.EnumRules;
import ai.pipestream.proto.validate.FieldRules;
import ai.pipestream.proto.validate.FloatRules;
import ai.pipestream.proto.validate.Int32Rules;
import ai.pipestream.proto.validate.Int64Rules;
import ai.pipestream.proto.validate.MapRules;
import ai.pipestream.proto.validate.MessageRules;
import ai.pipestream.proto.validate.RepeatedRules;
import ai.pipestream.proto.validate.StringRules;
import ai.pipestream.proto.validate.TimestampRules;
import ai.pipestream.proto.validate.UInt32Rules;
import ai.pipestream.proto.validate.UInt64Rules;
import ai.pipestream.proto.validate.ValidateProto;
import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MapConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.RepeatedConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.model.StringFormat;
import ai.pipestream.proto.validate.model.TimestampConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Built-in {@link ValidationRuleSource} reading the Pipestream {@code validate.v1}
 * options — {@code (ai.pipestream.proto.validate.v1.field)} and {@code (…​.message)} —
 * off protobuf descriptors and translating them into the neutral rule model.
 */
public final class AiPipestreamRuleSource implements ValidationRuleSource {

    @Override
    public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        var options = field.getOptions();
        if (!options.hasExtension(ValidateProto.field)) {
            return Optional.empty();
        }
        return Optional.of(toFieldConstraints(options.getExtension(ValidateProto.field)));
    }

    @Override
    public Optional<MessageConstraints> messageConstraints(Descriptor message) {
        var options = message.getOptions();
        if (!options.hasExtension(ValidateProto.message)) {
            return Optional.empty();
        }
        MessageRules rules = options.getExtension(ValidateProto.message);
        List<CelConstraint> cel = new ArrayList<>(rules.getCelList().size());
        for (CelRule rule : rules.getCelList()) {
            cel.add(toCel(rule));
        }
        return Optional.of(new MessageConstraints(cel));
    }

    /** Recursively translates {@link FieldRules} (also used for items/keys/values). */
    private static FieldConstraints toFieldConstraints(FieldRules rules) {
        FieldConstraints.Builder builder = FieldConstraints.builder().required(rules.getRequired());
        if (rules.hasString()) {
            builder.string(toStringConstraints(rules.getString()));
        }
        // A field carries at most one numeric rule set; prefer the wider signed set,
        // then the unsigned sets, when more than one is populated.
        if (rules.hasInt64()) {
            builder.integral(toInt64(rules.getInt64()));
        } else if (rules.hasInt32()) {
            builder.integral(toInt32(rules.getInt32()));
        } else if (rules.hasUint64()) {
            builder.integral(toUInt64(rules.getUint64()));
        } else if (rules.hasUint32()) {
            builder.integral(toUInt32(rules.getUint32()));
        }
        if (rules.hasDouble()) {
            builder.floating(toDouble(rules.getDouble()));
        } else if (rules.hasFloat()) {
            builder.floating(toFloat(rules.getFloat()));
        }
        if (rules.hasBool()) {
            builder.bool(toBool(rules.getBool()));
        }
        if (rules.hasBytes()) {
            builder.bytes(toBytes(rules.getBytes()));
        }
        if (rules.hasEnum()) {
            builder.enumeration(toEnum(rules.getEnum()));
        }
        if (rules.hasRepeated()) {
            builder.repeated(toRepeated(rules.getRepeated()));
        }
        if (rules.hasMap()) {
            builder.map(toMap(rules.getMap()));
        }
        if (rules.hasTimestamp()) {
            builder.timestamp(toTimestamp(rules.getTimestamp()));
        }
        if (rules.hasDuration()) {
            builder.duration(toDuration(rules.getDuration()));
        }
        for (CelRule cel : rules.getCelList()) {
            builder.addCel(toCel(cel));
        }
        return builder.build();
    }

    private static CelConstraint toCel(CelRule rule) {
        return new CelConstraint(rule.getId(), rule.getExpression(), rule.getMessage());
    }

    private static StringConstraints toStringConstraints(StringRules rules) {
        StringConstraints.Builder b = StringConstraints.builder();
        if (rules.hasConst()) {
            b.constant(rules.getConst());
        }
        if (rules.hasLen()) {
            b.len(rules.getLen());
        }
        if (rules.hasMinLen()) {
            b.minLen(rules.getMinLen());
        }
        if (rules.hasMaxLen()) {
            b.maxLen(rules.getMaxLen());
        }
        if (rules.hasPattern() && !rules.getPattern().isEmpty()) {
            b.pattern(rules.getPattern());
        }
        if (rules.hasPrefix()) {
            b.prefix(rules.getPrefix());
        }
        if (rules.hasSuffix()) {
            b.suffix(rules.getSuffix());
        }
        if (rules.hasContains()) {
            b.contains(rules.getContains());
        }
        if (rules.hasNotContains()) {
            b.notContains(rules.getNotContains());
        }
        b.in(rules.getInList());
        b.notIn(rules.getNotInList());
        if (rules.getEmail()) {
            b.format(StringFormat.EMAIL);
        }
        if (rules.getUuid()) {
            b.format(StringFormat.UUID);
        }
        if (rules.getHostname()) {
            b.format(StringFormat.HOSTNAME);
        }
        if (rules.getUri()) {
            b.format(StringFormat.URI);
        }
        if (rules.getIp()) {
            b.format(StringFormat.IP);
        }
        if (rules.getIpv4()) {
            b.format(StringFormat.IPV4);
        }
        if (rules.getIpv6()) {
            b.format(StringFormat.IPV6);
        }
        return b.build();
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
        b.finite(r.getFinite());
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
        b.finite(r.getFinite());
        return b.build();
    }

    private static BoolConstraints toBool(BoolRules r) {
        return new BoolConstraints(
                r.hasConst() ? Optional.of(r.getConst()) : Optional.empty());
    }

    private static BytesConstraints toBytes(BytesRules r) {
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
                r.getDefinedOnly(),
                r.getInList(),
                r.getNotInList());
    }

    private static RepeatedConstraints toRepeated(RepeatedRules r) {
        return new RepeatedConstraints(
                r.hasMinItems() ? OptionalLong.of(r.getMinItems()) : OptionalLong.empty(),
                r.hasMaxItems() ? OptionalLong.of(r.getMaxItems()) : OptionalLong.empty(),
                r.getUnique(),
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
        return new TimestampConstraints(
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
                r.hasGt() ? Optional.of(toJavaDuration(r.getGt())) : Optional.empty(),
                r.hasGte() ? Optional.of(toJavaDuration(r.getGte())) : Optional.empty(),
                r.hasLt() ? Optional.of(toJavaDuration(r.getLt())) : Optional.empty(),
                r.hasLte() ? Optional.of(toJavaDuration(r.getLte())) : Optional.empty());
    }

    private static Instant toInstant(com.google.protobuf.Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private static java.time.Duration toJavaDuration(com.google.protobuf.Duration d) {
        return java.time.Duration.ofSeconds(d.getSeconds(), d.getNanos());
    }
}
