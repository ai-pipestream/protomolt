package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Bounded descriptor linking and wire decoding over closed retained {@code FileDescriptorSet}
 * bytes for {@link RepositorySink} source reads. No live schema registry is consulted.
 */
final class RepositorySinkDescriptors {
    static final int MAX_DESCRIPTOR_BYTES = 16 * 1024 * 1024;
    static final int MAX_DESCRIPTOR_FILES = 512;
    static final int MAX_DESCRIPTOR_DEPENDENCIES = 4096;
    static final int MAX_DESCRIPTOR_IMPORT_DEPTH = 64;
    static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(
                    MAX_DESCRIPTOR_BYTES,
                    MAX_DESCRIPTOR_FILES,
                    MAX_DESCRIPTOR_DEPENDENCIES,
                    MAX_DESCRIPTOR_IMPORT_DEPTH);
    static final long MAX_WIRE_VALUES = 2_000_000L;
    static final int MAX_WIRE_DEPTH = 64;
    static final int STREAM_READ_BUFFER_BYTES = 8192;

    private RepositorySinkDescriptors() {}

    static Descriptor linkDescriptor(
            ByteString descriptorBytes,
            String messageType,
            String descriptorsField,
            String typeField) {
        List<FileDescriptor> linked;
        try {
            linked = ClosedDescriptorSet.load(descriptorBytes, DESCRIPTOR_LIMITS);
        } catch (ClosedDescriptorSet.LimitExceededException limit) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription(descriptorsField + ": " + limit.getMessage())
                    .asRuntimeException();
        } catch (IllegalArgumentException invalid) {
            throw ArchiveErrors.invalid(descriptorsField, invalid.getMessage());
        }
        for (FileDescriptor file : linked) {
            Descriptor found = findDescriptorInFile(file, messageType);
            if (found != null) {
                return found;
            }
        }
        throw ArchiveErrors.invalid(
                typeField, "message type '" + messageType + "' is not present in descriptor set");
    }

    static Descriptor findDescriptorInFile(FileDescriptor file, String fullName) {
        ArrayDeque<Descriptor> queue = new ArrayDeque<>(file.getMessageTypes());
        while (!queue.isEmpty()) {
            Descriptor current = queue.removeFirst();
            if (current.getFullName().equals(fullName)) {
                return current;
            }
            queue.addAll(current.getNestedTypes());
        }
        return null;
    }

    static String fingerprintOfRawSet(ByteString descriptorBytes, String field) {
        try {
            return DescriptorFingerprints.fingerprint(FileDescriptorSet.parseFrom(descriptorBytes));
        } catch (InvalidProtocolBufferException invalid) {
            throw ArchiveErrors.invalid(field, "invalid FileDescriptorSet wire bytes");
        }
    }

    static Message decodeMessage(
            ByteString wireBytes,
            Descriptor descriptor,
            RendererProvider.Cancellation cancellation) {
        MessageWireBudget budget = new MessageWireBudget(MAX_WIRE_VALUES, MAX_WIRE_DEPTH, () -> {
            if (cancellation.cancelled() || Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Wire scan cancelled");
            }
        });
        try {
            budget.check(wireBytes, descriptor);
            return DynamicMessage.parseFrom(descriptor, wireBytes);
        } catch (CancellationException cancelled) {
            throw Status.CANCELLED
                    .withDescription("Cancelled while decoding protobuf message")
                    .withCause(cancelled)
                    .asRuntimeException();
        } catch (MessageWireBudget.LimitExceededException limit) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription(limit.getMessage())
                    .asRuntimeException();
        } catch (InvalidProtocolBufferException malformed) {
            throw ArchiveErrors.invalid(
                    descriptor.getFullName(),
                    "wire bytes do not decode cleanly: " + malformed.getMessage());
        }
    }

    static ByteString readBoundedBytes(InputStream in, int maxBytes, String label)
            throws IOException {
        ByteString.Output out = ByteString.newOutput();
        byte[] buffer = new byte[STREAM_READ_BUFFER_BYTES];
        int read;
        int total = 0;
        while ((read = in.read(buffer)) >= 0) {
            if (read > maxBytes - total) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription(
                                label
                                        + " exceeds maximum sink input budget of "
                                        + maxBytes
                                        + " bytes")
                        .asRuntimeException();
            }
            out.write(buffer, 0, read);
            total += read;
        }
        return out.toByteString();
    }
}
