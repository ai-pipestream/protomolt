package ai.protomolt.proto.kafka.connect;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link CelFilter}'s {@code on.error} contract. The keep/drop happy path and compile-time
 * type-checking live in {@link TransformsTest}; this class pins what happens when the value
 * does not decode or the expression compiles but blows up at runtime — the two moments an
 * operator learns whether their {@code on.error} choice does what they think it does.
 */
class CelFilterTest {

    private static final String PROTO = """
            syntax = "proto3";
            package filter.test;
            message Event { int64 seq = 1; string id = 2; }
            """;

    /** Compiles, then divides by zero on any tick whose seq is 0. */
    private static final String FRAGILE = "10 / input.seq > 1";

    private static Descriptor eventType;
    private static String descriptorSetBase64;

    private CelFilter<SinkRecord> filter;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("filter/test/event.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("filter/test/event.proto").orElseThrow();
        eventType = file.findMessageTypeByName("Event");
    }

    @AfterEach
    void close() {
        if (filter != null) {
            filter.close();
            filter = null;
        }
    }

    private CelFilter<SinkRecord> filter(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "filter.test.Event");
        props.put(CelFilter.EXPRESSION, "true");
        props.putAll(overrides);
        CelFilter<SinkRecord> smt = new CelFilter<>();
        smt.configure(props);
        filter = smt;
        return smt;
    }

    private static SinkRecord record(Object value) {
        return new SinkRecord("events", 0, null, null, null, value, 0);
    }

    private static byte[] event(long seq) {
        return DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("seq"), seq)
                .build()
                .toByteArray();
    }

    private static byte[] garbage() {
        return "not protobuf".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void onErrorKeepPassesUndecodableValuesThrough() {
        CelFilter<SinkRecord> smt = filter(Map.of(CelFilter.ON_ERROR, "keep"));
        SinkRecord record = record(garbage());
        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void onErrorKeepStillAppliesTheExpressionToGoodValues() {
        CelFilter<SinkRecord> smt = filter(Map.of(
                CelFilter.EXPRESSION, "input.seq >= 5",
                CelFilter.ON_ERROR, "keep"));
        assertThat(smt.apply(record(event(7)))).isNotNull();
        assertThat(smt.apply(record(event(2)))).isNull();
    }

    @Test
    void aRuntimeFailureFailsTheRecordByDefault() {
        CelFilter<SinkRecord> smt = filter(Map.of(CelFilter.EXPRESSION, FRAGILE));
        assertThatThrownBy(() -> smt.apply(record(event(0))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Filter failed")
                .hasMessageContaining("filter.test.Event")
                .hasMessageContaining("events");
        // The same expression over a value it survives keeps working.
        assertThat(smt.apply(record(event(5)))).isNotNull();
        assertThat(smt.apply(record(event(20)))).isNull();
    }

    @Test
    void aRuntimeFailureKeepsUnderKeep() {
        CelFilter<SinkRecord> smt = filter(Map.of(
                CelFilter.EXPRESSION, FRAGILE,
                CelFilter.ON_ERROR, "keep"));
        SinkRecord record = record(event(0));
        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void aRuntimeFailureDropsUnderDrop() {
        CelFilter<SinkRecord> smt = filter(Map.of(
                CelFilter.EXPRESSION, FRAGILE,
                CelFilter.ON_ERROR, "drop"));
        assertThat(smt.apply(record(event(0)))).isNull();
        assertThat(smt.apply(record(event(5)))).isNotNull();
    }

    @Test
    void onErrorIsCaseInsensitive() {
        CelFilter<SinkRecord> smt = filter(Map.of(
                CelFilter.EXPRESSION, FRAGILE,
                CelFilter.ON_ERROR, "KEEP"));
        SinkRecord record = record(event(0));
        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void anUnknownOnErrorIsRejectedAtConfigure() {
        assertThatThrownBy(() -> filter(Map.of(CelFilter.ON_ERROR, "explode")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CelFilter.ON_ERROR);
    }

    @Test
    void theExpressionIsRequired() {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "filter.test.Event");
        CelFilter<SinkRecord> smt = new CelFilter<>();
        assertThatThrownBy(() -> smt.configure(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CelFilter.EXPRESSION);
    }
}
