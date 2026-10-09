package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Shared bounded canonical wire rules for internal schema evidence contracts. */
final class DocumentSchemaEvidenceCodec {
    static final int MAX_BYTES = 4 * 1024 * 1024;
    static final int MAX_WIRE_VALUES = 16384;
    static final int MAX_DEPTH = 16;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));

    private DocumentSchemaEvidenceCodec() {}

    record Encoded(ByteString bytes, String sha256) {}

    /** Borrowed bytes remain covered only while this owner is open. */
    static final class OwnedEncoded implements AutoCloseable {
        private Encoded encoded;
        private final DocumentAdmissionReservations.Lease lease;
        private OwnedEncoded(Encoded encoded, DocumentAdmissionReservations.Lease lease) {
            this.encoded = encoded; this.lease = lease;
        }
        synchronized Encoded value() {
            if (encoded == null) throw new IllegalStateException("Canonical encoding is closed");
            return encoded;
        }
        @Override public synchronized void close() {
            if (encoded == null) return;
            encoded = null; lease.close();
        }
    }

    static OwnedEncoded encodeOwned(Message path, DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(reservations);
        int size = measureAndValidate(path, control);
        var lease = Objects.requireNonNull(reservations.reserve(size), "reservation lease");
        boolean transferred = false;
        try {
            active(control);
            var encoded = encodeMeasured(path, size, control);
            var result = new OwnedEncoded(encoded, lease);
            transferred = true;
            return result;
        } finally {
            if (!transferred) lease.close();
        }
    }

    static Encoded encode(Message path, Runnable control) {
        int size = measureAndValidate(path, control);
        return encodeMeasured(path, size, control);
    }

    private static Encoded encodeMeasured(Message path, int size, Runnable control) {
        byte[] bytes = new byte[size];
        var output = CodedOutputStream.newInstance(bytes);
        try {
            write(path, output, control);
            output.checkNoSpaceLeft();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode schema evidence", failure);
        }
        // Exclusively owned array: output is finished and neither the array nor its
        // writer escapes. Avoid a second full serialized copy.
        var encoded = com.google.protobuf.UnsafeByteOperations.unsafeWrap(bytes);
        return new Encoded(encoded, digest(encoded, control));
    }

    static int measureAndValidate(Message path, Runnable control) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(control, "control");
        active(control);
        int size = measure(path, 0, new int[1], control);
        requireSize(size);
        VALIDATOR.validate(path).throwIfInvalid();
        active(control);
        return size;
    }

    static <T extends Message> T decode(String expectedCodec, String codec, int version, ByteString bytes, String sha256,
            com.google.protobuf.Descriptors.Descriptor descriptor, com.google.protobuf.Parser<T> parser,
            Runnable control) throws InvalidProtocolBufferException {
        return decodeInternal(expectedCodec, codec, version, bytes, sha256, descriptor, parser, null, control);
    }

    /** Budget only canonical comparison scratch; the input and parsed object remain caller-owned. */
    static <T extends Message> T decode(String expectedCodec, String codec, int version, ByteString bytes, String sha256,
            com.google.protobuf.Descriptors.Descriptor descriptor, com.google.protobuf.Parser<T> parser,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        return decodeInternal(expectedCodec, codec, version, bytes, sha256, descriptor, parser,
                Objects.requireNonNull(reservations), control);
    }

    private static <T extends Message> T decodeInternal(String expectedCodec, String codec, int version, ByteString bytes, String sha256,
            com.google.protobuf.Descriptors.Descriptor descriptor, com.google.protobuf.Parser<T> parser,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(control, "control");
        active(control);
        if (!expectedCodec.equals(codec) || version != 1)
            throw new IllegalArgumentException("unsupported schema evidence encoding");
        requireSize(bytes.size());
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || !digest(bytes, control).equals(sha256))
            throw new IllegalArgumentException("schema evidence digest mismatch");
        new MessageWireBudget(MAX_WIRE_VALUES, MAX_DEPTH, () -> active(control))
                .check(bytes, descriptor);
        var input = bytes.newCodedInput();
        input.setRecursionLimit(MAX_DEPTH);
        final T path;
        try { path = parser.parseFrom(input); }
        catch (IOException failure) {
            if (failure instanceof InvalidProtocolBufferException invalid) throw invalid;
            throw new InvalidProtocolBufferException(failure);
        }
        if (path.getDescriptorForType() != descriptor)
            throw new IllegalArgumentException("schema evidence parser differs from bounded descriptor");
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != bytes.size()) throw new InvalidProtocolBufferException("incomplete schema evidence decode");
        if (reservations == null) {
            var canonical = encode(path, control);
            if (!canonical.bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical schema evidence encoding");
        } else {
            try (var canonical = encodeOwned(path, reservations, control)) {
                if (!canonical.value().bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical schema evidence encoding");
            }
        }
        return path;
    }

    private static void requireSize(int size) {
        if (size < 1 || size > MAX_BYTES) throw new IllegalArgumentException("schema evidence byte bound exceeded");
    }

    private static int measure(Message message, int depth, int[] values, Runnable control) {
        active(control);
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("schema evidence depth bound exceeded");
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unknown schema evidence fields");
        long size = 0;
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.isExtension() || field.isPacked())
                throw new IllegalArgumentException("unsupported schema evidence field layout");
            var items = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            if (field.isMapField()) {
                requireStringMap(field);
                if (items.size() > MAX_WIRE_VALUES)
                    throw new IllegalArgumentException("schema evidence wire value bound exceeded");
                var keys = new java.util.HashSet<String>();
                long keyBytes = 0;
                for (var item : items) {
                    active(control);
                    var mapEntry = (Message) item;
                    String key = mapString(mapEntry, 1);
                    keyBytes += utf8Length(key, control);
                    if (keyBytes > MAX_BYTES)
                        throw new IllegalArgumentException("schema evidence byte bound exceeded");
                    if (!keys.add(key))
                        throw new IllegalArgumentException("duplicate schema evidence map key");
                }
            }
            for (var value : items) {
                active(control);
                if (++values[0] > MAX_WIRE_VALUES) throw new IllegalArgumentException("schema evidence wire value bound exceeded");
                int number = field.getNumber();
                size += switch (field.getType()) {
                    case MESSAGE -> {
                        int child = measure((Message) value, depth + 1, values, control);
                        if (field.isMapField()) {
                            var mapEntry = (Message) value;
                            for (int n = 1; n <= 2; n++) {
                                if (!mapEntry.hasField(mapEntry.getDescriptorForType().findFieldByNumber(n))
                                        && ++values[0] > MAX_WIRE_VALUES)
                                    throw new IllegalArgumentException("schema evidence wire value bound exceeded");
                            }
                            child = mapSize(mapEntry, control);
                        }
                        yield CodedOutputStream.computeTagSize(number) + CodedOutputStream.computeUInt32SizeNoTag(child) + child;
                    }
                    case STRING -> {
                        int length = utf8Length((String) value, control);
                        yield CodedOutputStream.computeTagSize(number) + CodedOutputStream.computeUInt32SizeNoTag(length) + length;
                    }
                    case BOOL -> CodedOutputStream.computeBoolSize(number, (Boolean) value);
                    case ENUM -> CodedOutputStream.computeEnumSize(number, ((EnumValueDescriptor) value).getNumber());
                    case UINT32 -> CodedOutputStream.computeUInt32Size(number, (Integer) value);
                    case UINT64 -> CodedOutputStream.computeUInt64Size(number, (Long) value);
                    case INT64 -> CodedOutputStream.computeInt64Size(number, (Long) value);
                    default -> throw new IllegalArgumentException("unsupported schema evidence scalar layout");
                };
                if (size > MAX_BYTES) throw new IllegalArgumentException("schema evidence byte bound exceeded");
            }
        }
        return (int) size;
    }

    static int utf8Length(String text, Runnable control) {
        long size = 0;
        for (int i = 0; i < text.length(); i++) {
            if ((i & 1023) == 0) active(control);
            char ch = text.charAt(i);
            if (ch < 128) size++;
            else if (ch < 2048) size += 2;
            else if (Character.isHighSurrogate(ch)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
                    throw new IllegalArgumentException("invalid schema evidence UTF-16");
                size += 4;
            } else if (Character.isLowSurrogate(ch)) throw new IllegalArgumentException("invalid schema evidence UTF-16");
            else size += 3;
            if (size > MAX_BYTES) throw new IllegalArgumentException("schema evidence byte bound exceeded");
        }
        return (int) size;
    }

    private static void requireStringMap(com.google.protobuf.Descriptors.FieldDescriptor field) {
        var type = field.getMessageType();
        if (type.getFields().size() != 2 || type.findFieldByNumber(1) == null || type.findFieldByNumber(2) == null
                || type.findFieldByNumber(1).getType() != com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING
                || type.findFieldByNumber(2).getType() != com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING)
            throw new IllegalArgumentException("unsupported schema evidence map layout");
    }

    private static String mapString(Message entry, int number) {
        return (String) entry.getField(entry.getDescriptorForType().findFieldByNumber(number));
    }

    private static int mapSize(Message entry, Runnable control) {
        int key = utf8Length(mapString(entry, 1), control);
        int value = utf8Length(mapString(entry, 2), control);
        // Both entry fields are explicit, including an empty value. Generated and
        // dynamic messages therefore have identical map bytes despite presence.
        return 2 + CodedOutputStream.computeUInt32SizeNoTag(key) + key
                + CodedOutputStream.computeUInt32SizeNoTag(value) + value;
    }

    // V1: ascending field numbers, repeated order preserved, shortest varints and
    // lengths, normal protobuf signed integer encoding, and semantic presence.
    // This deliberately does not depend on a runtime's deterministic-output flag.
    private static void write(Message message, CodedOutputStream output, Runnable control) throws IOException {
        var fields = message.getAllFields().entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().getNumber())).toList();
        for (var entry : fields) {
            var field = entry.getKey();
            var values = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            if (field.isMapField()) {
                // V1 string maps use unsigned UTF-8 key order, not insertion order
                // or a protobuf runtime's deterministic serialization convention.
                record MapItem(Message message, String key) {}
                var sorted = new java.util.ArrayList<MapItem>(values.size());
                for (var value : values) {
                    active(control);
                    var item = (Message) value;
                    sorted.add(new MapItem(item, mapString(item, 1)));
                }
                sorted.sort((a, b) -> compareUtf8Keys(a.key(), b.key(), control));
                for (var item : sorted) {
                    active(control);
                    output.writeTag(field.getNumber(), 2);
                    output.writeUInt32NoTag(mapSize(item.message(), control));
                    output.writeString(1, mapString(item.message(), 1));
                    output.writeString(2, mapString(item.message(), 2));
                }
                continue;
            }
            for (var value : values) {
                active(control);
                int number = field.getNumber();
                switch (field.getType()) {
                    case MESSAGE -> {
                        var child = (Message) value;
                        output.writeTag(number, 2);
                        // Nested string maps can normalize entry presence, so use
                        // the canonical length rather than the runtime wire size.
                        output.writeUInt32NoTag(measure(child, 0, new int[1], control));
                        write(child, output, control);
                    }
                    case STRING -> output.writeString(number, (String) value);
                    case BOOL -> output.writeBool(number, (Boolean) value);
                    case ENUM -> output.writeEnum(number, ((EnumValueDescriptor) value).getNumber());
                    case UINT32 -> output.writeUInt32(number, (Integer) value);
                    case UINT64 -> output.writeUInt64(number, (Long) value);
                    case INT64 -> output.writeInt64(number, (Long) value);
                    default -> throw new IllegalArgumentException("unsupported schema evidence scalar layout");
                }
            }
        }
    }

    /** Validated Unicode scalar order equals unsigned UTF-8 byte order, without key buffers. */
    static int compareUtf8Keys(String left, String right, Runnable control) {
        int a = 0, b = 0;
        while (a < left.length() && b < right.length()) {
            if ((a & 1023) == 0) active(control);
            int x = left.codePointAt(a), y = right.codePointAt(b);
            if (x != y) return Integer.compare(x, y);
            a += Character.charCount(x); b += Character.charCount(y);
        }
        return Integer.compare(left.length() - a, right.length() - b);
    }

    private static String digest(ByteString bytes, Runnable control) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) {
                active(control);
                digest.update(buffer);
            }
            active(control);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("schema evidence codec interrupted");
        control.run();
    }
}
