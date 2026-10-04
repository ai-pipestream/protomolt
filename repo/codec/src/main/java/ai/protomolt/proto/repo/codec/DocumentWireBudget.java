package ai.protomolt.proto.repo.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.concurrent.CancellationException;

/** Counts allocation-driving wire occurrences before decoding; not a JVM heap estimator. */
final class DocumentWireBudget {
    private final long limit;
    private final int maxDepth;
    private final Runnable control;
    private long values;

    DocumentWireBudget(long limit, int maxDepth, Runnable control) {
        this.limit = limit; this.maxDepth = maxDepth; this.control = control;
    }

    /** Reuse the same budget for every fragment. No field payload is copied or materialized. */
    void check(ByteString bytes, Descriptor descriptor) throws InvalidProtocolBufferException {
        var input = bytes.newCodedInput();
        try {
            scan(input, descriptor, 0, 0);
            if (input.getTotalBytesRead() != bytes.size()) throw malformed("Incomplete fragment");
        } catch (InvalidProtocolBufferException invalid) { throw invalid; }
        catch (IOException failure) { throw new InvalidProtocolBufferException(failure); }
    }

    private void scan(CodedInputStream input, Descriptor descriptor, int depth, int endGroup) throws IOException {
        if (depth > maxDepth) throw malformed("Document wire depth exceeds bound");
        while (true) {
            active();
            int tag = input.readTag();
            if (tag == 0) {
                if (endGroup != 0) throw malformed("Unterminated group");
                return;
            }
            int number = WireFormat.getTagFieldNumber(tag);
            int wire = WireFormat.getTagWireType(tag);
            if (wire == WireFormat.WIRETYPE_END_GROUP) {
                if (endGroup != number) throw malformed("Unexpected end group");
                return;
            }
            charge(1);
            var field = descriptor == null ? null : descriptor.findFieldByNumber(number);
            switch (wire) {
                case WireFormat.WIRETYPE_VARINT -> input.readRawVarint64();
                case WireFormat.WIRETYPE_FIXED64 -> input.skipRawBytes(8);
                case WireFormat.WIRETYPE_FIXED32 -> input.skipRawBytes(4);
                case WireFormat.WIRETYPE_START_GROUP -> scan(input,
                        field != null && field.getType() == FieldDescriptor.Type.GROUP ? field.getMessageType() : null,
                        depth + 1, number);
                case WireFormat.WIRETYPE_LENGTH_DELIMITED -> {
                    int length = input.readRawVarint32();
                    if (length < 0) throw malformed("Negative field length");
                    if (field != null && field.getType() == FieldDescriptor.Type.MESSAGE) {
                        int previous = input.pushLimit(length);
                        try {
                            scan(input, field.getMessageType(), depth + 1, 0);
                            if (input.getBytesUntilLimit() != 0) throw malformed("Truncated message");
                        } finally { input.popLimit(previous); }
                    } else if (field != null && field.isRepeated() && field.isPackable()) {
                        packed(input, field, length);
                    } else input.skipRawBytes(length);
                }
                default -> throw malformed("Invalid wire type");
            }
        }
    }

    private void packed(CodedInputStream input, FieldDescriptor field, int length) throws IOException {
        int width = switch (field.getType()) {
            case DOUBLE, FIXED64, SFIXED64 -> 8;
            case FLOAT, FIXED32, SFIXED32 -> 4;
            default -> 0;
        };
        if (width != 0) {
            if (length % width != 0) throw malformed("Misaligned packed scalar field");
            charge(length / width);
            input.skipRawBytes(length);
            return;
        }
        int previous = input.pushLimit(length);
        try {
            while (input.getBytesUntilLimit() > 0) {
                active();
                charge(1);
                input.readRawVarint64();
            }
        } finally { input.popLimit(previous); }
    }

    private void charge(long count) {
        if (count > limit - values) throw new IllegalArgumentException("Document wire value count exceeds bound");
        values += count;
    }

    private void active() {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Document wire scan interrupted");
        control.run();
    }

    private static InvalidProtocolBufferException malformed(String message) { return new InvalidProtocolBufferException(message); }
}
