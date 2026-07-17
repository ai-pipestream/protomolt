package ai.pipestream.proto.kafka.connect.iceberg;

import org.apache.kafka.connect.errors.DataException;

import java.util.Arrays;

/**
 * The Confluent protobuf wire format: a zero magic byte, a 4-byte schema id, a varint array of
 * message indexes (count first; a single zero varint means "first message"), then the message
 * bytes. This sink resolves the message type from configuration, so only the payload matters.
 *
 * <p>The index varints are <em>zigzag</em>-encoded: Confluent writes them with Kafka's
 * {@code ByteUtils.writeVarint}, which maps n to {@code (n << 1) ^ (n >> 31)}. Reading them as
 * plain unsigned varints happens to work for the single-zero-byte "first message" case and for
 * nothing else, so only a message declared first in its file would decode.</p>
 */
final class ConfluentFraming {

    private ConfluentFraming() {
    }

    /** The message bytes after the frame. */
    static byte[] payload(byte[] framed) {
        return Arrays.copyOfRange(framed, payloadOffset(framed), framed.length);
    }

    private static int payloadOffset(byte[] framed) {
        if (framed.length < 6 || framed[0] != 0) {
            throw new DataException("Not Confluent wire format: expected a zero magic byte "
                    + "and a schema id prefix");
        }
        int position = 5;
        long count = zigzag(readVarint(framed, position));
        position += varintLength(framed, position);
        if (count > 0) {
            if (count > 128) {
                throw new DataException("Implausible message-index count: " + count);
            }
            for (long i = 0; i < count; i++) {
                position += varintLength(framed, position);
            }
        }
        return position;
    }

    /** Undoes the zigzag mapping Kafka's ByteUtils.writeVarint applies. */
    private static long zigzag(long encoded) {
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    private static long readVarint(byte[] bytes, int position) {
        long value = 0;
        int shift = 0;
        while (position < bytes.length) {
            byte b = bytes[position++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 63) {
                break;
            }
        }
        throw new DataException("Malformed varint in the Confluent frame");
    }

    private static int varintLength(byte[] bytes, int position) {
        int length = 0;
        while (position + length < bytes.length) {
            if ((bytes[position + length++] & 0x80) == 0) {
                return length;
            }
        }
        throw new DataException("Malformed varint in the Confluent frame");
    }
}
