package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.Trailers;
import ai.protomolt.proto.asset.characterize.Trailers.EndOfCentralDirectory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * The members of a ZIP container, read out of the byte windows.
 *
 * <p>A ZIP keeps its index at the end, in a central directory written
 * immediately before the trailing record that points at it. That layout is
 * what makes a windowed reader possible: reach the record and the index is
 * usually reachable too, and the index carries every member's name without
 * a single seek into the body.
 *
 * <p>Reading a member's content needs the body, and only a member landing
 * inside a window can be read. That is not the limitation it sounds like
 * for identification, because the members that say what a document is are
 * written first: OOXML puts {@code [Content_Types].xml} at the front, and
 * the OpenDocument and EPUB specifications require an uncompressed
 * {@code mimetype} member as the very first entry.
 */
public final class ZipMembers {

    /** The signature of a central directory record, the bytes {@code 50 4B 01 02}. */
    private static final byte[] DIRECTORY_ENTRY = {'P', 'K', 0x01, 0x02};

    /** The fixed part of a central directory record. */
    private static final int DIRECTORY_ENTRY_BYTES = 46;

    /** The fixed part of a local member header. */
    private static final int LOCAL_HEADER_BYTES = 30;

    /** The compression method meaning the bytes are stored as they are. */
    public static final int STORED = 0;

    /** The compression method meaning the bytes are deflated. */
    public static final int DEFLATED = 8;

    /** How much of one member identification is willing to decompress. */
    public static final int MAX_MEMBER_BYTES = 1 << 20;

    /**
     * One member, as the central directory describes it.
     *
     * @param name the member's path inside the container
     * @param method the compression method
     * @param compressedBytes the stored size
     * @param originalBytes the size once decompressed
     * @param headerOffset the absolute offset of the member's local header
     */
    public record Member(String name, int method, long compressedBytes,
                         long originalBytes, long headerOffset) {
    }

    private ZipMembers() {
    }

    /**
     * Lists a container's members.
     *
     * @param bytes the content's windows
     * @return the members, or null when the index is not reachable — an
     *         archive that is truncated, or whose index lies outside the
     *         windows
     */
    public static List<Member> list(ByteWindows bytes) {
        EndOfCentralDirectory directory = Trailers.zipDirectory(bytes);
        if (!Trailers.directoryResident(bytes, directory)) {
            return null;
        }
        List<Member> members = new ArrayList<>();
        long at = directory.directoryOffset();
        long limit = at + directory.directoryBytes();
        while (at + DIRECTORY_ENTRY_BYTES <= limit) {
            if (!bytes.matchesAt(at, DIRECTORY_ENTRY)) {
                break;
            }
            int method = readShort(bytes, at + 10);
            long compressed = readInt(bytes, at + 20);
            long original = readInt(bytes, at + 24);
            int nameBytes = readShort(bytes, at + 28);
            int extraBytes = readShort(bytes, at + 30);
            int commentBytes = readShort(bytes, at + 32);
            long headerOffset = readInt(bytes, at + 42);
            byte[] name = bytes.slice(at + DIRECTORY_ENTRY_BYTES, nameBytes);
            if (name == null) {
                break;
            }
            members.add(new Member(new String(name, StandardCharsets.UTF_8),
                    method, compressed, original, headerOffset));
            at += DIRECTORY_ENTRY_BYTES + nameBytes + extraBytes + commentBytes;
        }
        return members;
    }

    /**
     * Reads one member's content.
     *
     * @param bytes the content's windows
     * @param member the member to read
     * @return the member's bytes, or null when they are outside the
     *         windows, are stored by a method this does not decompress, or
     *         are larger than {@link #MAX_MEMBER_BYTES}
     */
    public static byte[] read(ByteWindows bytes, Member member) {
        if (member.originalBytes() > MAX_MEMBER_BYTES
                || member.compressedBytes() > MAX_MEMBER_BYTES
                || member.compressedBytes() < 0) {
            return null;
        }
        // The local header repeats the name and may carry a different
        // amount of extra data than the index does, so the body's offset
        // is read from the header rather than assumed from the index.
        if (!bytes.observable(member.headerOffset(), LOCAL_HEADER_BYTES)) {
            return null;
        }
        int nameBytes = readShort(bytes, member.headerOffset() + 26);
        int extraBytes = readShort(bytes, member.headerOffset() + 28);
        long body = member.headerOffset() + LOCAL_HEADER_BYTES + nameBytes + extraBytes;
        byte[] stored = bytes.slice(body, (int) member.compressedBytes());
        if (stored == null) {
            return null;
        }
        if (member.method() == STORED) {
            return stored;
        }
        if (member.method() != DEFLATED) {
            return null;
        }
        return inflate(stored);
    }

    /** Raw deflate, bounded by what identification is willing to hold. */
    private static byte[] inflate(byte[] stored) {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(stored);
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, stored.length));
            byte[] buffer = new byte[8192];
            while (!inflater.finished()) {
                int produced = inflater.inflate(buffer);
                if (produced == 0) {
                    if (inflater.needsInput() || inflater.needsDictionary()) {
                        break;
                    }
                    continue;
                }
                if (out.size() + produced > MAX_MEMBER_BYTES) {
                    return null;
                }
                out.write(buffer, 0, produced);
            }
            return out.toByteArray();
        } catch (DataFormatException damaged) {
            // A member that will not decompress identifies nothing; the
            // container's other members still can.
            return null;
        } finally {
            inflater.end();
        }
    }

    private static int readShort(ByteWindows bytes, long offset) {
        int low = bytes.byteAt(offset);
        int high = bytes.byteAt(offset + 1);
        if (low == ByteWindows.NOT_OBSERVED || high == ByteWindows.NOT_OBSERVED) {
            return 0;
        }
        return low | (high << 8);
    }

    private static long readInt(ByteWindows bytes, long offset) {
        long value = 0;
        for (int i = 3; i >= 0; i--) {
            int part = bytes.byteAt(offset + i);
            if (part == ByteWindows.NOT_OBSERVED) {
                return 0;
            }
            value = (value << 8) | part;
        }
        return value;
    }
}
