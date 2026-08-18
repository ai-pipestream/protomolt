package ai.pipestream.proto.validate.source;

import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
import ai.pipestream.proto.validate.model.IgnoreMode;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.model.StringFormat;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.List;
import java.util.Optional;

/**
 * Built-in constraints for the {@code google.type} commons, adopted rather than
 * reinvented: the types ship no validation of their own, so this source carries
 * their documented invariants keyed by type full name. Any descriptor named
 * {@code google.type.*} gets them, whichever proto file it was compiled from —
 * no dependency on the generated classes, no annotation required on the schema.
 *
 * <p>Covered: {@code Money} (bounded {@code nanos}, sign agreement between
 * {@code units} and {@code nanos}, ISO 4217 {@code currency_code} required as
 * soon as an amount is set), {@code Interval} ({@code start_time} not after
 * {@code end_time}), {@code Date} (documented field bounds and the
 * day-without-month contradiction; whether a day exists in its month is calendar
 * work the structural bounds deliberately leave alone), and {@code LatLng}
 * (coordinate bounds).
 */
public final class GoogleTypeRuleSource implements ValidationRuleSource {

    private static final long NANOS_BOUND = 999_999_999L;

    @Override
    public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        return switch (field.getContainingType().getFullName() + "#" + field.getName()) {
            // Empty passes here; the message rule requires a currency as soon as an
            // amount is set, so a zero Money stays a legal zero value.
            case "google.type.Money#currency_code" -> Optional.of(FieldConstraints.builder()
                    .ignore(IgnoreMode.IF_ZERO_VALUE)
                    .string(StringConstraints.builder()
                            .format(StringFormat.CURRENCY_CODE).build())
                    .build());
            case "google.type.Money#nanos" -> Optional.of(FieldConstraints.builder()
                    .integral(IntegralConstraints.builder("int32")
                            .gte(-NANOS_BOUND).lte(NANOS_BOUND).build())
                    .build());
            // Zero means unset for every Date field, so each bound starts at 0.
            case "google.type.Date#year" -> Optional.of(FieldConstraints.builder()
                    .integral(IntegralConstraints.builder("int32").gte(0).lte(9999).build())
                    .build());
            case "google.type.Date#month" -> Optional.of(FieldConstraints.builder()
                    .integral(IntegralConstraints.builder("int32").gte(0).lte(12).build())
                    .build());
            case "google.type.Date#day" -> Optional.of(FieldConstraints.builder()
                    .integral(IntegralConstraints.builder("int32").gte(0).lte(31).build())
                    .build());
            case "google.type.LatLng#latitude" -> Optional.of(FieldConstraints.builder()
                    .floating(FloatingConstraints.builder("double")
                            .gte(-90.0).lte(90.0).build())
                    .build());
            case "google.type.LatLng#longitude" -> Optional.of(FieldConstraints.builder()
                    .floating(FloatingConstraints.builder("double")
                            .gte(-180.0).lte(180.0).build())
                    .build());
            default -> Optional.empty();
        };
    }

    @Override
    public Optional<MessageConstraints> messageConstraints(Descriptor message) {
        return switch (message.getFullName()) {
            case "google.type.Money" -> Optional.of(new MessageConstraints(List.of(
                    new CelConstraint(
                            "money.sign_agreement",
                            "this.units == 0 || this.nanos == 0"
                                    + " || (this.units > 0) == (this.nanos > 0)",
                            "units and nanos must agree in sign"),
                    new CelConstraint(
                            "money.currency_required_with_amount",
                            "(this.units == 0 && this.nanos == 0)"
                                    + " || this.currency_code != ''",
                            "an amount requires a currency_code"))));
            case "google.type.Interval" -> Optional.of(new MessageConstraints(List.of(
                    new CelConstraint(
                            "interval.ordered",
                            // Timestamps are CEL's native timestamp type: they compare
                            // directly and do not support field selection.
                            "!has(this.start_time) || !has(this.end_time)"
                                    + " || this.start_time <= this.end_time",
                            "start_time must not be after end_time"))));
            case "google.type.Date" -> Optional.of(new MessageConstraints(List.of(
                    new CelConstraint(
                            "date.day_requires_month",
                            "this.month != 0 || this.day == 0",
                            "a day without a month is a contradiction"))));
            default -> Optional.empty();
        };
    }
}
