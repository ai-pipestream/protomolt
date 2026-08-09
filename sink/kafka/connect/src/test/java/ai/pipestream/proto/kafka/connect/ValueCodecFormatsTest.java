package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ValueCodec} decode paths {@link ValueCodecTest} (malformed frames) and
 * {@link TransformsTest} (transform round-trips) leave alone: a Confluent frame whose
 * message-index is not the common {@code [0]}, JSON arriving as {@code byte[]} or
 * {@code String} with unknown fields tolerated, and the raw protobuf round-trip.
 */
class ValueCodecFormatsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package codec.test;
            message First { string id = 1; }
            message Second { string id = 1; int64 qty = 2; }
            """;

    private static Descriptor firstType;
    private static Descriptor secondType;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("codec/test/types.proto", PROTO, "test").build());
        FileDescriptor file = compiled.descriptorFor("codec/test/types.proto").orElseThrow();
        firstType = file.findMessageTypeByName("First");
        secondType = file.findMessageTypeByName("Second");
    }

    private static DynamicMessage second(String id, long qty) {
        return DynamicMessage.newBuilder(secondType)
                .setField(secondType.findFieldByName("id"), id)
                .setField(secondType.findFieldByName("qty"), qty)
                .build();
    }

    /**
     * The payload sits after a message-index array whose width varies with the path; a frame for
     * the second message in the file ({@code [1]}) must decode exactly like the common
     * single-zero-byte case.
     */
    @Test
    void aConfluentFrameWithANonZeroMessageIndexDecodes() {
        ValueCodec codec = new ValueCodec(secondType, "confluent");
        byte[] framed = ConfluentWireFormat.frame(7, List.of(1), second("s-1", 9).toByteArray());
        DynamicMessage decoded = codec.decode(framed, "topic orders");
        assertThat(decoded.getField(secondType.findFieldByName("id"))).isEqualTo("s-1");
        assertThat(decoded.getField(secondType.findFieldByName("qty"))).isEqualTo(9L);
    }

    /**
     * The frame prefix rides along untouched on re-encode, whatever the message index: the
     * schema id the worker's deserializer resolves stays the one the producer wrote.
     */
    @Test
    void aConfluentFrameReEncodesUnderItsOriginalPrefix() throws Exception {
        ValueCodec codec = new ValueCodec(secondType, "confluent");
        byte[] framed = ConfluentWireFormat.frame(42, List.of(1), second("s-1", 9).toByteArray());
        byte[] reEncoded = (byte[]) codec.encode(second("s-2", 3), framed);
        assertThat(ConfluentWireFormat.schemaId(reEncoded)).isEqualTo(42);
        assertThat(ConfluentWireFormat.messageIndex(reEncoded)).isEqualTo(List.of(1));
        DynamicMessage decoded = DynamicMessage.parseFrom(
                secondType, ConfluentWireFormat.payload(reEncoded));
        assertThat(decoded.getField(secondType.findFieldByName("id"))).isEqualTo("s-2");
    }

    @Test
    void jsonDecodesFromBytesAndStringsAlike() {
        ValueCodec codec = new ValueCodec(firstType, "json");
        DynamicMessage fromString = codec.decode("{\"id\": \"a\"}", "topic orders");
        DynamicMessage fromBytes = codec.decode(
                "{\"id\": \"a\"}".getBytes(StandardCharsets.UTF_8), "topic orders");
        assertThat(fromString).isEqualTo(fromBytes);
        assertThat(fromString.getField(firstType.findFieldByName("id"))).isEqualTo("a");
    }

    @Test
    void jsonUnknownFieldsAreIgnored() {
        ValueCodec codec = new ValueCodec(firstType, "json");
        DynamicMessage decoded = codec.decode("{\"id\": \"a\", \"nope\": 42}", "topic orders");
        assertThat(decoded.getField(firstType.findFieldByName("id"))).isEqualTo("a");
    }

    @Test
    void protobufRoundTripsThroughBytes() {
        ValueCodec codec = new ValueCodec(secondType, "protobuf");
        DynamicMessage message = second("s-1", 9);
        Object encoded = codec.encode(message, null);
        assertThat(encoded).isInstanceOf(byte[].class);
        assertThat(codec.decode(encoded, "topic orders")).isEqualTo(message);
    }

    @Test
    void theFormatNameIsCaseInsensitive() {
        ValueCodec codec = new ValueCodec(firstType, "Json");
        DynamicMessage decoded = codec.decode("{\"id\": \"a\"}", "topic orders");
        assertThat(decoded.getField(firstType.findFieldByName("id"))).isEqualTo("a");
    }
}
