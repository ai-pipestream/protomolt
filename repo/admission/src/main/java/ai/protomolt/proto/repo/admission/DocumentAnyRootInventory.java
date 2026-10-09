package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.codec.DocumentFragmentConfinement;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Strict discovery of Document structured_data and parser shapes; never resolves Any.value. */
final class DocumentAnyRootInventory {
    private static final String DOCUMENT_FINGERPRINT = DescriptorFingerprints.fingerprint(
            DescriptorFingerprints.closure(Document.getDescriptor()));

    record Limits(int maxBytes, long maxWireValues, int maxDepth, int maxRoots, int maxParserEntries) {
        Limits {
            if (maxBytes < 1 || maxWireValues < 1 || maxDepth < 1 || maxDepth > 100 || maxRoots < 1 || maxParserEntries < 1)
                throw new IllegalArgumentException("positive root inventory limits required; depth at most 100");
        }
    }
    static final class LimitExceeded extends IllegalArgumentException {
        LimitExceeded(String message) { super(message); }
    }

    record Root(List<DocumentSchemaOccurrences.Step> access, Any envelope) {
        Root { access = List.copyOf(access); }
    }
    record Result(DocumentPublicationSlot slot, String layoutPolicy, DocumentSchemaBinding containerSchema,
            ByteString original, String fragmentSha256, List<Root> roots) {
        Result { roots = List.copyOf(roots); }
    }

    private DocumentAnyRootInventory() {}

    static Result inspect(DocumentSchemaBinding container, DocumentPublicationSlot slot, ByteString bytes,
            String expectedDocId, Limits limits, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(container); Objects.requireNonNull(slot); Objects.requireNonNull(bytes);
        Objects.requireNonNull(limits); Objects.requireNonNull(control);
        active(control);
        if (!container.type().getFullName().equals(Document.getDescriptor().getFullName())
                || !container.condition().getDescriptorFingerprint().equals(DOCUMENT_FINGERPRINT))
            throw new IllegalArgumentException("containing schema differs from supported Document layout");
        if (!slot.getUnknownFields().asMap().isEmpty() || !slot.getSubKey().isEmpty()
                || (slot.getPart() != DocumentPart.DOCUMENT_PART_CORE && slot.getPart() != DocumentPart.DOCUMENT_PART_PARSED))
            throw new IllegalArgumentException("root inventory requires a CORE or PARSED slot");
        if (bytes.size() > limits.maxBytes()) throw new LimitExceeded("root inventory byte bound exceeded");
        new MessageWireBudget(limits.maxWireValues(), limits.maxDepth(), () -> active(control)).check(bytes, container.type());
        var input = bytes.newCodedInput();
        input.setRecursionLimit(limits.maxDepth());
        final DynamicMessage document;
        try { document = DynamicMessage.parseFrom(container.type(), input); }
        catch (IOException failure) {
            if (failure instanceof InvalidProtocolBufferException invalid) throw invalid;
            throw new InvalidProtocolBufferException(failure);
        }
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != bytes.size()) throw new InvalidProtocolBufferException("incomplete root inventory decode");
        active(control);
        DocumentFragmentConfinement.requireConfined(document, slot.getPart(), expectedDocId);
        known(document);
        var roots = new ArrayList<Root>();
        if (slot.getPart() == DocumentPart.DOCUMENT_PART_CORE) {
            var field = document.getDescriptorForType().findFieldByNumber(4);
            if (document.hasField(field)) add(roots, List.of(new DocumentSchemaOccurrences.Field(4)),
                    (Message) document.getField(field), limits);
        } else {
            var field = document.getDescriptorForType().findFieldByNumber(5);
            if (document.getRepeatedFieldCount(field) > limits.maxParserEntries())
                throw new LimitExceeded("parser entry bound exceeded");
            var keys = new HashSet<String>();
            for (int i = 0; i < document.getRepeatedFieldCount(field); i++) {
                active(control);
                var entry = (Message) document.getRepeatedField(field, i);
                known(entry);
                String key = (String) value(entry, 1);
                if (!keys.add(key)) throw new IllegalArgumentException("duplicate parser key in retained fragment");
                var parser = (Message) value(entry, 2);
                known(parser);
                if (!has(parser, 7)) continue;
                var parsed = (Message) value(parser, 7);
                known(parsed);
                if (has(parsed, 1)) add(roots, List.of(new DocumentSchemaOccurrences.Field(5),
                        new DocumentSchemaOccurrences.MapKey(com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING, key),
                        new DocumentSchemaOccurrences.Field(7), new DocumentSchemaOccurrences.Field(1)),
                        (Message) value(parsed, 1), limits);
            }
        }
        var hash = sha256(bytes, control);
        active(control);
        return new Result(slot, DocumentFragmentConfinement.POLICY_ID, container, bytes, hash, roots);
    }

    private static void add(ArrayList<Root> roots, List<DocumentSchemaOccurrences.Step> access, Message any, Limits limits) {
        known(any);
        if (roots.size() >= limits.maxRoots()) throw new LimitExceeded("root count bound exceeded");
        // Preserve the effective envelope. No definition lookup or payload parsing occurs.
        roots.add(new Root(access, Any.newBuilder().setTypeUrl((String) value(any, 1))
                .setValue((ByteString) value(any, 2)).build()));
    }
    private static boolean has(Message value, int field) { return value.hasField(value.getDescriptorForType().findFieldByNumber(field)); }
    private static Object value(Message value, int field) { return value.getField(value.getDescriptorForType().findFieldByNumber(field)); }
    private static void known(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unknown fields on root access path");
    }
    private static String sha256(ByteString bytes, Runnable control) {
        try {
            var hash = java.security.MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) { active(control); hash.update(buffer); }
            return HexFormat.of().formatHex(hash.digest());
        } catch (java.security.NoSuchAlgorithmException failure) { throw new IllegalStateException("SHA-256 unavailable", failure); }
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("root inventory interrupted");
        control.run();
    }
}
