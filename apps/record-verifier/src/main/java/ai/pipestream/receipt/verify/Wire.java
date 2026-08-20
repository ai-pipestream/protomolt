package ai.pipestream.receipt.verify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A protobuf wire reader with canonicality accounting, hand-rolled against the JDK so this
 * codebase shares nothing with the platform runtime. Parsing is as tolerant as the standard
 * parser — out-of-order fields, duplicates, and non-minimal varints all parse — but every
 * such tolerance is recorded, because the record format's identity is its canonical bytes:
 * a tolerated deviation is exactly what the reserialization-equality check refuses. Unknown
 * fields and wrong-wire-type occurrences of known fields are recorded by path, mirroring
 * the runtime's unknown-field walk.
 */
final class Wire {

    /** What a parse observed beyond the values: canonical deviations and unknown fields. */
    static final class Notes {
        final List<String> unknownFields = new ArrayList<>();
        final List<String> nonCanonical = new ArrayList<>();
    }

    /** Thrown for bytes the standard parser would also refuse outright. */
    static final class MalformedException extends Exception {
        MalformedException(String message) {
            super(message);
        }
    }

    private final byte[] data;
    private final Notes notes;
    private final String path;
    private int pos;
    private final int limit;
    private int lastFieldNumber;

    Wire(byte[] data, Notes notes, String path) {
        this(data, notes, path, 0, data.length);
    }

    private Wire(byte[] data, Notes notes, String path, int from, int to) {
        this.data = data;
        this.notes = notes;
        this.path = path;
        this.pos = from;
        this.limit = to;
    }

    boolean hasMore() {
        return pos < limit;
    }

    String path() {
        return path;
    }

    void noteNonCanonical(String reason) {
        notes.nonCanonical.add(path.isEmpty() ? reason : path + ": " + reason);
    }

    void noteUnknown(int fieldNumber) {
        notes.unknownFields.add((path.isEmpty() ? "" : path + ".") + fieldNumber);
    }

    /** Reads a tag, enforcing the ordering ledger; returns (number << 3 | wireType). */
    int readTag() throws MalformedException {
        long tag = readVarintRaw("tag");
        if (tag > 0xFFFFFFFFL || tag < 0) {
            throw new MalformedException(at("tag overflows"));
        }
        int number = (int) (tag >>> 3);
        if (number == 0) {
            throw new MalformedException(at("field number zero"));
        }
        return (int) tag;
    }

    /** Records ordering evidence: a descending field number is non-canonical. */
    void ordered(int number) {
        if (number < lastFieldNumber) {
            noteNonCanonical("field " + number + " appears after field " + lastFieldNumber);
        }
        lastFieldNumber = number;
    }

    /** Reads a varint, recording non-minimal encodings. */
    long readVarint(String field) throws MalformedException {
        int start = pos;
        long value = readVarintRaw(field);
        if (pos - start != minimalVarintLength(value)) {
            noteNonCanonical(field + " uses a non-minimal varint");
        }
        return value;
    }

    private long readVarintRaw(String field) throws MalformedException {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (pos >= limit) {
                throw new MalformedException(at(field + " is truncated"));
            }
            byte b = data[pos++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw new MalformedException(at(field + " varint is longer than ten bytes"));
    }

    private static int minimalVarintLength(long value) {
        int length = 1;
        while ((value & ~0x7FL) != 0) {
            value >>>= 7;
            length++;
        }
        return length;
    }

    /** Reads a length-delimited payload's bounds and returns them as a sub-reader. */
    Wire readLengthDelimited(String field) throws MalformedException {
        long length = readVarintRaw(field + " length");
        if (length < 0 || length > limit - pos) {
            throw new MalformedException(at(field + " length overruns the buffer"));
        }
        Wire slice = new Wire(data, notes, childPath(field), pos, pos + (int) length);
        pos += (int) length;
        return slice;
    }

    byte[] bytes() {
        byte[] copy = new byte[limit - pos];
        System.arraycopy(data, pos, copy, 0, copy.length);
        pos = limit;
        return copy;
    }

    /** The slice as a UTF-8 string; invalid UTF-8 is malformed, as the standard parser has it. */
    String utf8(String field) throws MalformedException {
        byte[] raw = bytes();
        String decoded = new String(raw, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), raw)) {
            throw new MalformedException(at(field + " is not valid UTF-8"));
        }
        return decoded;
    }

    /** Skips one value of {@code wireType}, including nested groups; used for unknowns. */
    void skip(int wireType, String field) throws MalformedException {
        switch (wireType) {
            case 0 -> readVarintRaw(field);
            case 1 -> advance(8, field);
            case 2 -> readLengthDelimited(field);
            case 3 -> skipGroup(field);
            case 4 -> throw new MalformedException(at(field + " has an unmatched group end"));
            case 5 -> advance(4, field);
            default -> throw new MalformedException(at(field + " has wire type " + wireType));
        }
    }

    private void skipGroup(String field) throws MalformedException {
        while (true) {
            int tag = readTag();
            int wireType = tag & 7;
            if (wireType == 4) {
                return;
            }
            skip(wireType, field);
        }
    }

    private void advance(int count, String field) throws MalformedException {
        if (limit - pos < count) {
            throw new MalformedException(at(field + " is truncated"));
        }
        pos += count;
    }

    void requireExhausted() throws MalformedException {
        if (pos != limit) {
            throw new MalformedException(at("trailing bytes"));
        }
    }

    private String childPath(String field) {
        return path.isEmpty() ? field : path + "." + field;
    }

    private String at(String reason) {
        return (path.isEmpty() ? "" : path + ": ") + reason + " at offset " + pos;
    }
}
