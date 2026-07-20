package ai.pipestream.proto.kafka.wire;

import com.google.protobuf.Descriptors.Descriptor;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * The Confluent protobuf wire format, written to its published spec rather than adapted from
 * anyone's implementation:
 *
 * <pre>
 *   [0x00 magic][4-byte big-endian schema id][message-index array][protobuf payload]
 * </pre>
 *
 * <p>The message-index array locates the message <em>within</em> the schema's file: the index of
 * the top-level type, then one index per nesting step. Its varints are <em>zigzag</em>-encoded,
 * mapping n to {@code (n << 1) ^ (n >> 31)}, which is what Kafka's {@code ByteUtils.writeVarint}
 * does. Zero is its own zigzag, so an unsigned reader agrees with the encoder on the
 * first-message case and on nothing else; reading these as plain unsigned varints is a bug that
 * only shows up once a topic carries a type that is not declared first in its file.</p>
 *
 * <p>The array has one special case: the single index {@code [0]} (by far the common one) is
 * written as a lone zero byte rather than a count followed by an index.</p>
 */
public final class ConfluentWireFormat {

    /** Every Confluent frame opens with this. */
    public static final byte MAGIC = 0;

    private static final int PREFIX_BYTES = 5;
    /** A frame claiming more indexes than this is not a frame we are reading correctly. */
    private static final int MAX_INDEXES = 128;

    private ConfluentWireFormat() {
    }

    /** Wraps payload bytes in a frame. */
    public static byte[] frame(int schemaId, List<Integer> messageIndex, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(PREFIX_BYTES + 8 + payload.length);
        out.write(MAGIC);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        if (messageIndex.size() == 1 && messageIndex.get(0) == 0) {
            out.write(0);
        } else {
            writeVarint(messageIndex.size(), out);
            for (int index : messageIndex) {
                writeVarint(index, out);
            }
        }
        out.writeBytes(payload);
        return out.toByteArray();
    }

    /** The schema id the frame was written with. */
    public static int schemaId(byte[] framed) {
        requireFrame(framed);
        return ((framed[1] & 0xFF) << 24) | ((framed[2] & 0xFF) << 16)
                | ((framed[3] & 0xFF) << 8) | (framed[4] & 0xFF);
    }

    /** The path to the message within its file. */
    public static List<Integer> messageIndex(byte[] framed) {
        requireFrame(framed);
        Cursor cursor = new Cursor(framed, PREFIX_BYTES);
        int count = (int) cursor.readVarint();
        if (count == 0) {
            return List.of(0);
        }
        if (count < 0 || count > MAX_INDEXES) {
            throw new ConfluentWireFormatException("Implausible message-index count: " + count);
        }
        List<Integer> path = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            path.add((int) cursor.readVarint());
        }
        return List.copyOf(path);
    }

    /** The message bytes after the frame. */
    public static byte[] payload(byte[] framed) {
        int offset = payloadOffset(framed);
        byte[] payload = new byte[framed.length - offset];
        System.arraycopy(framed, offset, payload, 0, payload.length);
        return payload;
    }

    /** Where the message bytes start. */
    public static int payloadOffset(byte[] framed) {
        requireFrame(framed);
        Cursor cursor = new Cursor(framed, PREFIX_BYTES);
        long count = cursor.readVarint();
        if (count < 0 || count > MAX_INDEXES) {
            throw new ConfluentWireFormatException("Implausible message-index count: " + count);
        }
        for (long i = 0; i < count; i++) {
            cursor.readVarint();
        }
        return cursor.position;
    }

    /**
     * The index path of a message within its file: the top-level type's index, then one index
     * per nesting step. This is what the frame carries instead of the type's name.
     */
    public static List<Integer> indexPath(Descriptor descriptor) {
        Deque<Integer> path = new ArrayDeque<>();
        Descriptor current = descriptor;
        while (current.getContainingType() != null) {
            Descriptor parent = current.getContainingType();
            path.addFirst(parent.getNestedTypes().indexOf(current));
            current = parent;
        }
        path.addFirst(current.getFile().getMessageTypes().indexOf(current));
        return List.copyOf(path);
    }

    /** The message an index path points at, or null when the path does not lead anywhere. */
    public static Descriptor messageAt(com.google.protobuf.Descriptors.FileDescriptor file,
                                       List<Integer> indexPath) {
        if (indexPath.isEmpty()) {
            return null;
        }
        List<Descriptor> level = file.getMessageTypes();
        Descriptor found = null;
        for (int index : indexPath) {
            if (index < 0 || index >= level.size()) {
                return null;
            }
            found = level.get(index);
            level = found.getNestedTypes();
        }
        return found;
    }

    private static void requireFrame(byte[] framed) {
        if (framed == null || framed.length < PREFIX_BYTES + 1 || framed[0] != MAGIC) {
            throw new ConfluentWireFormatException("Not Confluent wire format: expected a zero "
                    + "magic byte followed by a 4-byte schema id");
        }
    }

    /** Zigzag, then unsigned varint: byte for byte what Kafka's ByteUtils.writeVarint emits. */
    private static void writeVarint(int value, ByteArrayOutputStream out) {
        int encoded = (value << 1) ^ (value >> 31);
        while ((encoded & 0xFFFFFF80) != 0) {
            out.write((encoded & 0x7F) | 0x80);
            encoded >>>= 7;
        }
        out.write(encoded & 0x7F);
    }

    /** A read position that advances, so callers cannot disagree about how wide a varint was. */
    private static final class Cursor {

        private final byte[] bytes;
        private int position;

        private Cursor(byte[] bytes, int position) {
            this.bytes = bytes;
            this.position = position;
        }

        /** Reads one zigzag varint and advances past it. */
        private long readVarint() {
            long raw = 0;
            int shift = 0;
            while (position < bytes.length) {
                byte b = bytes[position++];
                raw |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return (raw >>> 1) ^ -(raw & 1);
                }
                shift += 7;
                if (shift > 63) {
                    break;
                }
            }
            throw new ConfluentWireFormatException("Malformed varint in the Confluent frame");
        }
    }
}
