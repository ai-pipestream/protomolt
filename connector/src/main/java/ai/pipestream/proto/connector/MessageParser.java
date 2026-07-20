package ai.pipestream.proto.connector;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Turns a raw record payload into a protobuf message. A byte-oriented source hands each
 * payload to the parser its plan carries, so the source stays agnostic about how a topic
 * is framed.
 */
@FunctionalInterface
public interface MessageParser {

    /**
     * Parses one payload.
     *
     * @throws SourceException when the payload does not conform
     */
    Message parse(byte[] payload);

    /** Parses each payload as the given message type. */
    static MessageParser forType(Descriptor type) {
        return payload -> {
            try {
                return DynamicMessage.parseFrom(type, payload);
            } catch (InvalidProtocolBufferException e) {
                throw new SourceException("payload is not a valid " + type.getFullName(), e);
            }
        };
    }

    /** Wraps each payload verbatim in a {@link BytesValue}; no schema assumed. */
    static MessageParser bytes() {
        return payload -> BytesValue.of(ByteString.copyFrom(payload));
    }
}
