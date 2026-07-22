package ai.pipestream.proto.connector;

import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The three framings a byte-oriented source can be handed: a pinned descriptor, opaque bytes,
 * and the Confluent frame. Every parse failure has to reach the pipeline as a
 * {@link SourceException} rather than as whatever the underlying codec threw.
 */
class MessageParserTest {

    @Test
    void forTypeParsesAPayloadAsThatMessageType() throws Exception {
        Descriptor type = docDescriptor();
        byte[] payload = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("doc_id"), "abc")
                .build()
                .toByteArray();

        Message parsed = MessageParser.forType(type).parse(payload);

        assertThat(parsed.getDescriptorForType().getFullName()).isEqualTo("ai.pipestream.test.Doc");
        assertThat(parsed.getField(type.findFieldByName("doc_id"))).isEqualTo("abc");
    }

    @Test
    void forTypeWrapsAnUnparsablePayloadInSourceException() throws Exception {
        Descriptor type = docDescriptor();
        // A field-1 length-delimited tag whose declared length runs past the end of the buffer.
        byte[] truncated = {0x0A, 0x7F, 0x01};

        assertThatThrownBy(() -> MessageParser.forType(type).parse(truncated))
                .isInstanceOf(SourceException.class)
                .hasMessage("payload is not a valid ai.pipestream.test.Doc")
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void bytesWrapsThePayloadVerbatimWithoutAssumingASchema() {
        byte[] payload = "not protobuf at all".getBytes(StandardCharsets.UTF_8);

        Message parsed = MessageParser.bytes().parse(payload);

        assertThat(parsed).isInstanceOf(BytesValue.class);
        assertThat(((BytesValue) parsed).getValue()).isEqualTo(ByteString.copyFrom(payload));
    }

    /** The default topic-aware overload delegates; only topic-relative parsers override it. */
    @Test
    void topicAwareParseDelegatesToThePayloadOnlyParseByDefault() {
        byte[] payload = {1, 2, 3};

        assertThat(MessageParser.bytes().parse("orders", payload))
                .isEqualTo(MessageParser.bytes().parse(payload));
    }

    @Test
    void confluentFromAUrlRefusesToParseWithoutATopic() {
        MessageParser parser = MessageParser.confluent("http://localhost:1");

        assertThatThrownBy(() -> parser.parse(new byte[]{0, 0, 0, 0, 1, 0}))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("the Confluent frame is topic-relative");
    }

    @Test
    void confluentFromAConfigMapRefusesToParseWithoutATopic() {
        MessageParser parser = MessageParser.confluent(
                Map.of(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, "http://localhost:1"));

        assertThatThrownBy(() -> parser.parse(new byte[]{0, 0, 0, 0, 1, 0}))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("the Confluent frame is topic-relative");
    }

    /**
     * The serde throws Kafka's SerializationException; a source must not leak that to a caller
     * that only knows about SourceException.
     */
    @Test
    void confluentWrapsAnUnreadableFrameInSourceExceptionNamingTheTopic() {
        MessageParser parser = MessageParser.confluent("http://localhost:1");
        byte[] notAFrame = {0x7F, 0x01};

        assertThatThrownBy(() -> parser.parse("orders", notAFrame))
                .isInstanceOf(SourceException.class)
                .hasMessage("payload on orders is not a readable frame")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    private static Descriptor docDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("parser_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("doc_id")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
