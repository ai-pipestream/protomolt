package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The transforms against a schema with declared validation rules: validate (fail, drop,
 * header), map (text and CEL rules, every value format, Confluent frame preserved), and
 * the CEL filter — descriptor-native, rules readable from the reparsed descriptor set.
 */
class TransformsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package smt.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field) = {
                string: {min_len: 3}
              }];
              int64 seq = 2;
              string note = 3;
              string category = 4;
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    private Object transform;

    @BeforeAll
    static void compileSchema() throws Exception {
        String validateProto;
        try (InputStream in = TransformsTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")) {
            validateProto = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("smt/test/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("smt/test/event.proto").orElseThrow();
        eventType = file.findMessageTypeByName("Event");
    }

    @AfterEach
    void closeTransform() {
        if (transform instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        transform = null;
    }

    private static Map<String, String> baseConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "smt.test.Event");
        return props;
    }

    private ValidateMessage<SinkRecord> validate(Map<String, String> props) {
        ValidateMessage<SinkRecord> smt = new ValidateMessage<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private MapMessage<SinkRecord> map(Map<String, String> props) {
        MapMessage<SinkRecord> smt = new MapMessage<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private CelFilter<SinkRecord> filter(Map<String, String> props) {
        CelFilter<SinkRecord> smt = new CelFilter<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private static DynamicMessage event(String id, long seq, String note) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventType);
        builder.setField(eventType.findFieldByName("id"), id);
        builder.setField(eventType.findFieldByName("seq"), seq);
        builder.setField(eventType.findFieldByName("note"), note);
        return builder.build();
    }

    private static SinkRecord record(Object value) {
        return new SinkRecord("events", 0, null, null, null, value, 0);
    }

    private static DynamicMessage decode(SinkRecord record) throws Exception {
        return DynamicMessage.parseFrom(eventType, (byte[]) record.value());
    }

    private static String field(DynamicMessage message, String name) {
        return (String) message.getField(eventType.findFieldByName(name));
    }

    @Test
    void validRecordsPassThroughUntouched() {
        ValidateMessage<SinkRecord> smt = validate(baseConfig());
        SinkRecord record = record(event("abc", 1, "").toByteArray());
        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void invalidRecordsFailByDefault() {
        ValidateMessage<SinkRecord> smt = validate(baseConfig());
        assertThatThrownBy(() -> smt.apply(record(event("x", 1, "").toByteArray())))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("\"field\":\"id\"")
                .hasMessageContaining("min_len");
    }

    @Test
    void onInvalidDropDropsTheRecord() {
        Map<String, String> props = baseConfig();
        props.put(ValidateMessage.ON_INVALID, "drop");
        ValidateMessage<SinkRecord> smt = validate(props);
        assertThat(smt.apply(record(event("x", 1, "").toByteArray()))).isNull();
        assertThat(smt.apply(record(event("abc", 1, "").toByteArray()))).isNotNull();
    }

    @Test
    void onInvalidHeaderAttachesViolationsAndPassesThrough() {
        Map<String, String> props = baseConfig();
        props.put(ValidateMessage.ON_INVALID, "header");
        ValidateMessage<SinkRecord> smt = validate(props);
        byte[] value = event("x", 1, "").toByteArray();
        SinkRecord out = smt.apply(record(value));
        assertThat((byte[]) out.value()).isEqualTo(value);
        String header = (String) out.headers()
                .lastWithName("protomolt.violations").value();
        assertThat(header).contains("\"field\":\"id\"").contains("min_len");
        assertThat(smt.apply(record(event("abc", 1, "").toByteArray()))
                .headers().lastWithName("protomolt.violations")).isNull();
    }

    @Test
    void tombstonesAlwaysPassThrough() {
        SinkRecord tombstone = record(null);
        assertThat(validate(baseConfig()).apply(tombstone)).isSameAs(tombstone);

        Map<String, String> mapProps = baseConfig();
        mapProps.put(MapMessage.RULES, "note = id");
        assertThat(map(mapProps).apply(tombstone)).isSameAs(tombstone);

        Map<String, String> filterProps = baseConfig();
        filterProps.put(CelFilter.EXPRESSION, "false");
        assertThat(filter(filterProps).apply(tombstone)).isSameAs(tombstone);
    }

    @Test
    void textRulesReshapeTheValue() throws Exception {
        Map<String, String> props = baseConfig();
        props.put(MapMessage.RULES, "note = id");
        MapMessage<SinkRecord> smt = map(props);
        SinkRecord out = smt.apply(record(event("abc", 7, "").toByteArray()));
        DynamicMessage mapped = decode(out);
        assertThat(field(mapped, "note")).isEqualTo("abc");
        assertThat(field(mapped, "id")).isEqualTo("abc");
    }

    @Test
    void celRulesFilterAndSelect() throws Exception {
        Map<String, String> props = baseConfig();
        props.put(MapMessage.CEL_RULES_JSON, """
                [{"filter": "input.seq > 5", "selector": "'big'", "target": "category"}]
                """);
        MapMessage<SinkRecord> smt = map(props);
        DynamicMessage big = decode(smt.apply(record(event("abc", 7, "").toByteArray())));
        assertThat(field(big, "category")).isEqualTo("big");
        DynamicMessage small = decode(smt.apply(record(event("abc", 2, "").toByteArray())));
        assertThat(field(small, "category")).isEmpty();
    }

    @Test
    void confluentFramedValuesKeepTheirFrame() throws Exception {
        Map<String, String> props = baseConfig();
        props.put(ValueCodec.VALUE_FORMAT, "confluent");
        props.put(MapMessage.RULES, "note = id");
        MapMessage<SinkRecord> smt = map(props);

        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        framed.write(0);
        framed.writeBytes(new byte[] {0, 0, 0, 42});   // schema id 42
        framed.write(0);                                // message-indexes: [0]
        framed.writeBytes(event("abc", 7, "").toByteArray());

        SinkRecord out = smt.apply(record(framed.toByteArray()));
        byte[] value = (byte[]) out.value();
        assertThat(value[0]).isZero();
        assertThat(value[4]).isEqualTo((byte) 42);      // frame prefix preserved
        DynamicMessage mapped = DynamicMessage.parseFrom(
                eventType, ConfluentWireFormat.payload(value));
        assertThat(field(mapped, "note")).isEqualTo("abc");
    }

    @Test
    void jsonValuesRoundTripAsJson() {
        Map<String, String> props = baseConfig();
        props.put(ValueCodec.VALUE_FORMAT, "json");
        props.put(MapMessage.RULES, "note = id");
        MapMessage<SinkRecord> smt = map(props);
        SinkRecord out = smt.apply(record("{\"id\": \"abc\", \"seq\": \"7\"}"));
        assertThat(out.value()).isInstanceOf(String.class);
        assertThat((String) out.value()).contains("\"note\"").contains("abc");
    }

    @Test
    void mapWithoutAnyRulesIsRejectedAtConfigure() {
        assertThatThrownBy(() -> map(baseConfig()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(MapMessage.RULES);
    }

    /**
     * Every malformed shape of 'cel.rules.json' has to fail at configure time. A rule list that
     * silently parsed to zero rules would leave the transform running as a no-op.
     */
    @Test
    void malformedCelRulesJsonIsRejectedAtConfigure() {
        Map<String, String> notJson = baseConfig();
        notJson.put(MapMessage.CEL_RULES_JSON, "[{\"target\": ");
        assertThatThrownBy(() -> map(notJson))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(MapMessage.CEL_RULES_JSON)
                .hasMessageContaining("not valid JSON");

        Map<String, String> notArray = baseConfig();
        notArray.put(MapMessage.CEL_RULES_JSON, "{\"target\": \"category\"}");
        assertThatThrownBy(() -> map(notArray))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(MapMessage.CEL_RULES_JSON)
                .hasMessageContaining("must be a JSON array");

        Map<String, String> notObjects = baseConfig();
        notObjects.put(MapMessage.CEL_RULES_JSON, "[\"category = id\"]");
        assertThatThrownBy(() -> map(notObjects))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("each rule must be a JSON object");

        Map<String, String> noTarget = baseConfig();
        noTarget.put(MapMessage.CEL_RULES_JSON, "[{\"selector\": \"'big'\"}]");
        assertThatThrownBy(() -> map(noTarget))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("each rule needs a 'target' field path");

        Map<String, String> blankTarget = baseConfig();
        blankTarget.put(MapMessage.CEL_RULES_JSON, "[{\"selector\": \"'big'\", \"target\": \"  \"}]");
        assertThatThrownBy(() -> map(blankTarget))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("each rule needs a 'target' field path");
    }

    @Test
    void celRuleExpressionsTypeCheckAtConfigure() {
        Map<String, String> badFilter = baseConfig();
        badFilter.put(MapMessage.CEL_RULES_JSON,
                "[{\"filter\": \"input.no_such_field > 1\", \"target\": \"category\"}]");
        assertThatThrownBy(() -> map(badFilter))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(MapMessage.CEL_RULES_JSON)
                .hasMessageContaining("does not compile");
    }

    @Test
    void nonByteArrayValuesAreRejectedForBinaryFormats() {
        ValueCodec protobuf = new ValueCodec(eventType, "protobuf");
        assertThatThrownBy(() -> protobuf.decode("{\"id\":\"abc\"}", "topic events"))
                .isInstanceOf(DataException.class)
                .hasMessage("Record value must be byte[] for this format; got java.lang.String"
                        + " (use the ByteArrayConverter for value.converter)");

        ValueCodec confluent = new ValueCodec(eventType, "confluent");
        assertThatThrownBy(() -> confluent.decode("framed", "topic events"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Record value must be byte[] for this format");
        assertThatThrownBy(() -> confluent.encode(event("abc", 1, ""), "framed"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Record value must be byte[] for this format");
    }

    /**
     * JSON values re-encode as the Java type they arrived as, so a worker using the
     * ByteArrayConverter (byte[]) and one using the StringConverter (String) both round-trip.
     */
    @Test
    void jsonValuesReEncodeAsTheJavaTypeTheyArrivedAs() {
        Map<String, String> props = baseConfig();
        props.put(ValueCodec.VALUE_FORMAT, "json");
        props.put(MapMessage.RULES, "note = id");
        MapMessage<SinkRecord> smt = map(props);

        SinkRecord fromBytes = smt.apply(record(
                "{\"id\": \"abc\", \"seq\": \"7\"}".getBytes(StandardCharsets.UTF_8)));
        assertThat(fromBytes.value()).isInstanceOf(byte[].class);
        assertThat(new String((byte[]) fromBytes.value(), StandardCharsets.UTF_8))
                .contains("\"note\"").contains("abc");

        SinkRecord fromString = smt.apply(record("{\"id\": \"abc\", \"seq\": \"7\"}"));
        assertThat(fromString.value()).isInstanceOf(String.class);
    }

    @Test
    void filterKeepsMatchesAndDropsTheRest() {
        Map<String, String> props = baseConfig();
        props.put(CelFilter.EXPRESSION, "input.seq >= 5");
        CelFilter<SinkRecord> smt = filter(props);
        assertThat(smt.apply(record(event("abc", 7, "").toByteArray()))).isNotNull();
        assertThat(smt.apply(record(event("abc", 2, "").toByteArray()))).isNull();
    }

    @Test
    void filterExpressionsTypeCheckAtConfigure() {
        Map<String, String> props = baseConfig();
        props.put(CelFilter.EXPRESSION, "input.no_such_field == 1");
        assertThatThrownBy(() -> filter(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("does not compile");
    }

    @Test
    void undecodableValuesFollowOnError() {
        Map<String, String> props = baseConfig();
        props.put(CelFilter.EXPRESSION, "true");
        props.put(CelFilter.ON_ERROR, "drop");
        CelFilter<SinkRecord> smt = filter(props);
        assertThat(smt.apply(record("not protobuf".getBytes(StandardCharsets.UTF_8))))
                .isNull();

        props.put(CelFilter.ON_ERROR, "fail");
        CelFilter<SinkRecord> failing = filter(props);
        assertThatThrownBy(() -> failing.apply(
                record("not protobuf".getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(DataException.class);
    }

    @Test
    void redactMasksDeclaredSensitivityClasses() throws Exception {
        String metadataProto = new String(getClass().getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/meta/v1/metadata.proto")
                .readAllBytes(), StandardCharsets.UTF_8);
        String customerProto = """
                syntax = "proto3";
                package smt.test;
                import "ai/pipestream/proto/meta/v1/metadata.proto";
                message Customer {
                  string id = 1;
                  string email = 2 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
                }
                """;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/meta/v1/metadata.proto", metadataProto, "test")
                .add("smt/test/customer.proto", customerProto, "test")
                .build());
        Descriptor customer = compiled.descriptorFor("smt/test/customer.proto").orElseThrow()
                .findMessageTypeByName("Customer");

        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray()));
        props.put(ValueCodec.MESSAGE_TYPE, "smt.test.Customer");
        props.put(RedactMessage.STRATEGY, "redact");
        RedactMessage<SinkRecord> smt = new RedactMessage<>();
        smt.configure(props);
        transform = smt;

        DynamicMessage message = DynamicMessage.newBuilder(customer)
                .setField(customer.findFieldByName("id"), "c-1")
                .setField(customer.findFieldByName("email"), "pat@example.com")
                .build();
        SinkRecord out = smt.apply(record(message.toByteArray()));
        DynamicMessage masked = DynamicMessage.parseFrom(customer, (byte[]) out.value());
        assertThat(masked.getField(customer.findFieldByName("email"))).isEqualTo("***");
        assertThat(masked.getField(customer.findFieldByName("id"))).isEqualTo("c-1");
    }

    @Test
    void unknownMessageTypesAreRejectedAtConfigure() {
        Map<String, String> props = baseConfig();
        props.put(ValueCodec.MESSAGE_TYPE, "smt.test.NoSuch");
        props.put(CelFilter.EXPRESSION, "true");
        assertThatThrownBy(() -> filter(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("smt.test.NoSuch");
    }
}
