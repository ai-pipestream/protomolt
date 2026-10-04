package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
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

/** Single relative path codec. Does not prove candidate identity, access or retention. */
final class DocumentSchemaOccurrenceCodec {
    static final String CODEC = "repository-schema-occurrence";
    static final int VERSION = 1;
    static final int MAX_BYTES = 4 * 1024 * 1024;
    static final int MAX_WIRE_VALUES = 16384;
    static final int MAX_DEPTH = 16;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));

    private DocumentSchemaOccurrenceCodec() {}

    record Encoded(ByteString bytes, String sha256) {}

    static Encoded encode(RepositorySchemaOccurrencePath path, Runnable control) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(control, "control");
        active(control);
        int size = measure(path, 0, new int[1], control);
        requireSize(size);
        VALIDATOR.validate(path).throwIfInvalid();
        active(control);
        byte[] bytes = new byte[size];
        var output = CodedOutputStream.newInstance(bytes);
        try {
            write(path, output, control);
            output.checkNoSpaceLeft();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode schema occurrence", failure);
        }
        var encoded = ByteString.copyFrom(bytes);
        return new Encoded(encoded, digest(encoded, control));
    }

    static RepositorySchemaOccurrencePath decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(control, "control");
        active(control);
        if (!CODEC.equals(codec) || version != VERSION)
            throw new IllegalArgumentException("unsupported occurrence encoding");
        requireSize(bytes.size());
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || !digest(bytes, control).equals(sha256))
            throw new IllegalArgumentException("occurrence digest mismatch");
        new MessageWireBudget(MAX_WIRE_VALUES, MAX_DEPTH, () -> active(control))
                .check(bytes, RepositorySchemaOccurrencePath.getDescriptor());
        var input = bytes.newCodedInput();
        input.setRecursionLimit(MAX_DEPTH);
        final RepositorySchemaOccurrencePath path;
        try { path = RepositorySchemaOccurrencePath.parseFrom(input); }
        catch (IOException failure) {
            if (failure instanceof InvalidProtocolBufferException invalid) throw invalid;
            throw new InvalidProtocolBufferException(failure);
        }
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != bytes.size()) throw new InvalidProtocolBufferException("incomplete occurrence decode");
        var canonical = encode(path, control);
        if (!canonical.bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical occurrence encoding");
        return path;
    }

    private static void requireSize(int size) {
        if (size < 1 || size > MAX_BYTES) throw new IllegalArgumentException("occurrence byte bound exceeded");
    }

    private static int measure(Message message, int depth, int[] values, Runnable control) {
        active(control);
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("occurrence depth bound exceeded");
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unknown occurrence fields");
        long size = 0;
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.isExtension() || field.isMapField() || field.isPacked())
                throw new IllegalArgumentException("unsupported occurrence field layout");
            var items = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            for (var value : items) {
                active(control);
                if (++values[0] > MAX_WIRE_VALUES) throw new IllegalArgumentException("occurrence wire value bound exceeded");
                int number = field.getNumber();
                size += switch (field.getType()) {
                    case MESSAGE -> {
                        int child = measure((Message) value, depth + 1, values, control);
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
                    default -> throw new IllegalArgumentException("unsupported occurrence scalar layout");
                };
                if (size > MAX_BYTES) throw new IllegalArgumentException("occurrence byte bound exceeded");
            }
        }
        return (int) size;
    }

    private static int utf8Length(String text, Runnable control) {
        long size = 0;
        for (int i = 0; i < text.length(); i++) {
            if ((i & 1023) == 0) active(control);
            char ch = text.charAt(i);
            if (ch < 128) size++;
            else if (ch < 2048) size += 2;
            else if (Character.isHighSurrogate(ch)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
                    throw new IllegalArgumentException("invalid occurrence UTF-16");
                size += 4;
            } else if (Character.isLowSurrogate(ch)) throw new IllegalArgumentException("invalid occurrence UTF-16");
            else size += 3;
            if (size > MAX_BYTES) throw new IllegalArgumentException("occurrence byte bound exceeded");
        }
        return (int) size;
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
            for (var value : values) {
                active(control);
                int number = field.getNumber();
                switch (field.getType()) {
                    case MESSAGE -> {
                        var child = (Message) value;
                        output.writeTag(number, 2);
                        output.writeUInt32NoTag(child.getSerializedSize());
                        write(child, output, control);
                    }
                    case STRING -> output.writeString(number, (String) value);
                    case BOOL -> output.writeBool(number, (Boolean) value);
                    case ENUM -> output.writeEnum(number, ((EnumValueDescriptor) value).getNumber());
                    case UINT32 -> output.writeUInt32(number, (Integer) value);
                    case UINT64 -> output.writeUInt64(number, (Long) value);
                    case INT64 -> output.writeInt64(number, (Long) value);
                    default -> throw new IllegalArgumentException("unsupported occurrence scalar layout");
                }
            }
        }
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
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("occurrence codec interrupted");
        control.run();
    }
}
