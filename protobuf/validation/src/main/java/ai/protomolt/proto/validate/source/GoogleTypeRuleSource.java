package ai.protomolt.proto.validate.source;

import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.IntegralConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.model.StringFormat;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
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
 * work the structural bounds deliberately leave alone), {@code LatLng}
 * (coordinate bounds), {@code PhoneNumber} (exactly one kind; the e164 form
 * through the Tier-1 E.164 parser; a complete short code; the documented
 * 40-character extension bound), and {@code PostalAddress} (required
 * {@code region_code} against the JDK's ISO 3166 table, BCP 47
 * {@code language_code}, revision pinned to 0). Everything here is data-free:
 * vocabulary checks ride what the JDK ships, and per-country postal-code
 * grammar deliberately waits on an operator-loaded pack rather than bundled
 * data.
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
            // Empty passes here; the message rule requires one kind, and a
            // present e164 form goes through the Tier-1 E.164 parser.
            case "google.type.PhoneNumber#e164_number" -> Optional.of(FieldConstraints.builder()
                    .ignore(IgnoreMode.IF_ZERO_VALUE)
                    .string(StringConstraints.builder()
                            .format(StringFormat.PHONE_NUMBER).build())
                    .build());
            // Documented bound: an extension is at most 40 characters.
            case "google.type.PhoneNumber#extension" -> Optional.of(FieldConstraints.builder()
                    .string(StringConstraints.builder().maxLen(40).build())
                    .build());
            // The JDK's own ISO 3166 table: vocabulary with zero bundled data.
            case "google.type.PhoneNumber.ShortCode#region_code",
                    "google.type.PostalAddress#region_code" ->
                    Optional.of(FieldConstraints.builder()
                            .ignore(IgnoreMode.IF_ZERO_VALUE)
                            .string(StringConstraints.builder()
                                    .format(StringFormat.REGION_CODE).build())
                            .build());
            case "google.type.PostalAddress#language_code" ->
                    Optional.of(FieldConstraints.builder()
                            .ignore(IgnoreMode.IF_ZERO_VALUE)
                            .string(StringConstraints.builder()
                                    .format(StringFormat.LANGUAGE_TAG).build())
                            .build());
            // The documented schema revision is 0; zero is also the proto
            // default, so only an actually-wrong revision reaches the rule.
            case "google.type.PostalAddress#revision" -> Optional.of(FieldConstraints.builder()
                    .integral(IntegralConstraints.builder("int32").gte(0).lte(0).build())
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
            case "google.type.PhoneNumber" -> Optional.of(new MessageConstraints(List.of(
                    new CelConstraint(
                            "phone.kind_required",
                            "this.e164_number != '' || has(this.short_code)",
                            "a phone number needs an e164_number or a short_code"))));
            case "google.type.PhoneNumber.ShortCode" ->
                    Optional.of(new MessageConstraints(List.of(
                            new CelConstraint(
                                    "short_code.complete",
                                    "this.region_code != '' && this.number != ''",
                                    "a short code needs both region_code and number"))));
            case "google.type.PostalAddress" -> Optional.of(new MessageConstraints(List.of(
                    new CelConstraint(
                            "address.region_required",
                            "this.region_code != ''",
                            "region_code is required (ISO 3166-1 alpha-2)"))));
            default -> Optional.empty();
        };
    }
}
