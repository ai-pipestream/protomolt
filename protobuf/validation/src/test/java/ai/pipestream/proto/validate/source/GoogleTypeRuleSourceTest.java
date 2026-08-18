package ai.pipestream.proto.validate.source;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The google.type commons validate by type name alone: these tests compile the
 * google.type shapes as their own dynamic descriptors, proving the rules need
 * neither the generated classes nor any annotation on the schema — a descriptor
 * named {@code google.type.Money} is enough, whichever file it came from.
 */
class GoogleTypeRuleSourceTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static final Descriptor MONEY = moneyDescriptor();
    private static final Descriptor INTERVAL = intervalDescriptor();
    private static final Descriptor DATE = dateDescriptor();
    private static final Descriptor LAT_LNG = latLngDescriptor();

    private static List<String> ruleIds(Message message) {
        return VALIDATOR.validate(message).violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void wellFormedValuesPass() {
        assertThat(ruleIds(money("USD", 5, 500_000_000))).isEmpty();
        assertThat(ruleIds(money("CHF", -2, -750_000_000))).isEmpty();
        // A zero Money is the type's own legal zero value, currency and all.
        assertThat(ruleIds(DynamicMessage.getDefaultInstance(MONEY))).isEmpty();
        assertThat(ruleIds(interval(10, 0, 20, 0))).isEmpty();
        assertThat(ruleIds(interval(10, 5, 10, 5))).isEmpty();
        assertThat(ruleIds(date(2026, 8, 18))).isEmpty();
        // Partial dates are legal forms: year only, year and month.
        assertThat(ruleIds(date(2026, 0, 0))).isEmpty();
        assertThat(ruleIds(date(2026, 8, 0))).isEmpty();
        assertThat(ruleIds(latLng(46.2, 6.1))).isEmpty();
    }

    @Test
    void moneyNanosAreBounded() {
        assertThat(ruleIds(money("USD", 0, 1_000_000_000)))
                .containsExactly("int32.gte_lte");
    }

    @Test
    void moneySignsMustAgree() {
        assertThat(ruleIds(money("USD", 1, -500_000_000)))
                .containsExactly("money.sign_agreement");
    }

    @Test
    void anAmountRequiresACurrency() {
        assertThat(ruleIds(money("", 1, 0)))
                .containsExactly("money.currency_required_with_amount");
    }

    @Test
    void theCurrencyGoesThroughTheIso4217Table() {
        assertThat(ruleIds(money("usd", 1, 0)))
                .containsExactly("string.currency_code");
    }

    @Test
    void anInvertedIntervalIsRefusedByName() {
        assertThat(ruleIds(interval(20, 0, 10, 0)))
                .containsExactly("interval.ordered");
        // The nanos tiebreak on equal seconds.
        assertThat(ruleIds(interval(10, 6, 10, 5)))
                .containsExactly("interval.ordered");
    }

    @Test
    void aDayWithoutAMonthIsAContradiction() {
        assertThat(ruleIds(date(2026, 0, 18)))
                .containsExactly("date.day_requires_month");
    }

    @Test
    void dateFieldsKeepTheirDocumentedBounds() {
        assertThat(ruleIds(date(2026, 13, 1))).containsExactly("int32.gte_lte");
        assertThat(ruleIds(date(10000, 1, 1))).containsExactly("int32.gte_lte");
    }

    @Test
    void coordinatesKeepTheirBounds() {
        assertThat(ruleIds(latLng(90.5, 0))).containsExactly("double.gte_lte");
        assertThat(ruleIds(latLng(0, -180.5))).containsExactly("double.gte_lte");
    }

    private static Message money(String currency, long units, int nanos) {
        return DynamicMessage.newBuilder(MONEY)
                .setField(MONEY.findFieldByName("currency_code"), currency)
                .setField(MONEY.findFieldByName("units"), units)
                .setField(MONEY.findFieldByName("nanos"), nanos)
                .build();
    }

    private static Message interval(long startSeconds, int startNanos,
            long endSeconds, int endNanos) {
        return DynamicMessage.newBuilder(INTERVAL)
                .setField(INTERVAL.findFieldByName("start_time"),
                        Timestamp.newBuilder()
                                .setSeconds(startSeconds).setNanos(startNanos).build())
                .setField(INTERVAL.findFieldByName("end_time"),
                        Timestamp.newBuilder()
                                .setSeconds(endSeconds).setNanos(endNanos).build())
                .build();
    }

    private static Message date(int year, int month, int day) {
        return DynamicMessage.newBuilder(DATE)
                .setField(DATE.findFieldByName("year"), year)
                .setField(DATE.findFieldByName("month"), month)
                .setField(DATE.findFieldByName("day"), day)
                .build();
    }

    private static Message latLng(double latitude, double longitude) {
        return DynamicMessage.newBuilder(LAT_LNG)
                .setField(LAT_LNG.findFieldByName("latitude"), latitude)
                .setField(LAT_LNG.findFieldByName("longitude"), longitude)
                .build();
    }

    private static Descriptor moneyDescriptor() {
        return build("google/type/money.proto", DescriptorProto.newBuilder()
                .setName("Money")
                .addField(scalar("currency_code", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .addField(scalar("units", 2, FieldDescriptorProto.Type.TYPE_INT64))
                .addField(scalar("nanos", 3, FieldDescriptorProto.Type.TYPE_INT32)));
    }

    private static Descriptor intervalDescriptor() {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("google/type/interval.proto")
                .setPackage("google.type")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Interval")
                        .addField(timestampField("start_time", 1))
                        .addField(timestampField("end_time", 2)))
                .build();
        try {
            return FileDescriptor.buildFrom(
                            file, new FileDescriptor[]{Timestamp.getDescriptor().getFile()})
                    .findMessageTypeByName("Interval");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Descriptor dateDescriptor() {
        return build("google/type/date.proto", DescriptorProto.newBuilder()
                .setName("Date")
                .addField(scalar("year", 1, FieldDescriptorProto.Type.TYPE_INT32))
                .addField(scalar("month", 2, FieldDescriptorProto.Type.TYPE_INT32))
                .addField(scalar("day", 3, FieldDescriptorProto.Type.TYPE_INT32)));
    }

    private static Descriptor latLngDescriptor() {
        return build("google/type/latlng.proto", DescriptorProto.newBuilder()
                .setName("LatLng")
                .addField(scalar("latitude", 1, FieldDescriptorProto.Type.TYPE_DOUBLE))
                .addField(scalar("longitude", 2, FieldDescriptorProto.Type.TYPE_DOUBLE)));
    }

    private static Descriptor build(String fileName, DescriptorProto.Builder message) {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName(fileName)
                .setPackage("google.type")
                .setSyntax("proto3")
                .addMessageType(message)
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                    .findMessageTypeByName(message.getName());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static FieldDescriptorProto.Builder scalar(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static FieldDescriptorProto.Builder timestampField(String name, int number) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Timestamp")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
