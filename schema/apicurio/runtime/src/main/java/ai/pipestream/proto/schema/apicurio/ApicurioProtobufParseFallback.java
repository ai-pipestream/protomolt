package ai.pipestream.proto.schema.apicurio;

import com.google.protobuf.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Strips the Apicurio Kafka protobuf wire-format prefix and parses the payload with a
 * concrete generated message type, recovering when the registry is unavailable but the
 * message type is known ahead of time.
 *
 * <p>Non-headers wire format:
 * {@code [0x00 magic][ID bytes][indexes?][Ref?][payload]}.
 */
public final class ApicurioProtobufParseFallback {

    public static final byte MAGIC_BYTE = 0x00;

    private final Class<? extends Message> messageType;
    private final Method parseFrom;
    private final int idSize;
    private final boolean readIndexes;
    private final boolean readTypeRef;

    public ApicurioProtobufParseFallback(
            Class<? extends Message> messageType,
            int idSize,
            boolean readIndexes,
            boolean readTypeRef) {
        this.messageType = Objects.requireNonNull(messageType, "messageType");
        if (com.google.protobuf.DynamicMessage.class.isAssignableFrom(messageType)) {
            throw new IllegalArgumentException(
                    "DynamicMessage requires a Descriptor; use a generated Message type");
        }
        try {
            this.parseFrom = messageType.getMethod("parseFrom", InputStream.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Class " + messageType.getName() + " has no parseFrom(InputStream)", e);
        }
        if (idSize < 0) {
            throw new IllegalArgumentException("idSize must be >= 0");
        }
        this.idSize = idSize;
        this.readIndexes = readIndexes;
        this.readTypeRef = readTypeRef;
    }

    /** Default Confluent/Apicurio content-id size of 4 bytes, with type-ref enabled. */
    public static ApicurioProtobufParseFallback forType(Class<? extends Message> messageType) {
        return new ApicurioProtobufParseFallback(messageType, 4, false, true);
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T parse(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            if (data.length > 0 && data[0] == MAGIC_BYTE) {
                int prefix = 1 + idSize;
                if (in.skip(prefix) != prefix) {
                    throw new IllegalStateException(
                            "Wire-format prefix truncated: expected " + prefix + " bytes");
                }
                if (readIndexes) {
                    skipMessageIndexes(in);
                }
                if (readTypeRef) {
                    skipDelimitedMessage(in);
                }
            }
            return (T) parseFrom.invoke(null, in);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(
                    "Fallback protobuf parsing failed for " + messageType.getName(), e);
        }
    }

    /** Skips Confluent-style message indexes (zigzag varint count + zigzag varints). */
    static void skipMessageIndexes(InputStream in) throws IOException {
        int count = readZigZagVarint(in);
        for (int i = 0; i < count; i++) {
            readZigZagVarint(in);
        }
    }

    /** Reads a zigzag-encoded varint (Confluent message indexes use zigzag encoding). */
    static int readZigZagVarint(InputStream in) throws IOException {
        int raw = readVarint(in);
        return (raw >>> 1) ^ -(raw & 1);
    }

    /** Skips a length-delimited protobuf message (Apicurio type Ref). */
    static void skipDelimitedMessage(InputStream in) throws IOException {
        int length = readVarint(in);
        if (length < 0) {
            throw new IOException("Negative delimited message length");
        }
        long skipped = in.skip(length);
        if (skipped != length) {
            throw new IOException("Truncated delimited message");
        }
    }

    static int readVarint(InputStream in) throws IOException {
        int result = 0;
        int shift = 0;
        while (shift < 32) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("Unexpected EOF reading varint");
            }
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        // Continuation past 32 bits: lengths and message indexes never need more, so this is
        // malformed framing. Fail like the first loop instead of silently discarding bytes.
        throw new IOException("Malformed varint: exceeds 32 bits");
    }
}
