package ai.protomolt.proto.repo.archive;

import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.decodeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.linkDescriptor;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.typeNameFromUrl;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceKeyType;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceMapKey;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorRequest;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorResponse;
import ai.protomolt.proto.repo.v2.OpaqueOccurrence;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.ResolvedOccurrence;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Resolves an occurrence field path inside a decoded document-part fragment for {@link
 * RepositorySink}, unpacking {@code google.protobuf.Any} values against the schema retained on the
 * source document.
 */
final class RepositorySinkOccurrences {
    static final int MAX_OCCURRENCE_STEPS = 128;
    static final String ANY_FULL_NAME = "google.protobuf.Any";

    private final DocumentArchive archive;

    RepositorySinkOccurrences(
            DocumentArchive archive) {
        this.archive = Objects.requireNonNull(archive, "archive");
    }

    ResolvedOccurrenceValue resolveOccurrenceInFragment(
            NodeAddress docAddress,
            long resolvedRevision,
            PartSelector partSelector,
            String fieldPath,
            Descriptor rootDescriptor,
            Message rootMessage,
            String rootDescriptorSha256,
            RevisionValidation validation,
            RendererProvider.Cancellation cancellation) {
        List<PathSegment> segments = parseOccurrenceFieldPath(fieldPath);
        Descriptor curDesc = rootDescriptor;
        Message curMsg = rootMessage;
        String curDescriptorSha256 = rootDescriptorSha256;
        List<RepositorySchemaOccurrenceStep> navSteps = new ArrayList<>();

        for (int i = 0; i < segments.size(); i++) {
            if (ANY_FULL_NAME.equals(curDesc.getFullName())) {
                UnpackedAny unpacked = unpackRetainedAny(
                        docAddress,
                        resolvedRevision,
                        partSelector,
                        fieldPath,
                        curMsg,
                        navSteps,
                        validation,
                        cancellation);
                curDesc = unpacked.descriptor();
                curMsg = unpacked.message();
                curDescriptorSha256 = unpacked.descriptorSha256();
            }

            PathSegment seg = segments.get(i);
            FieldDescriptor fd = curDesc.findFieldByName(seg.fieldName());
            if (fd == null) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "unknown field '" + seg.fieldName() + "' on " + curDesc.getFullName());
            }
            if (fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "field '" + seg.fieldName() + "' is not a protobuf message field");
            }
            navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                    .setFieldNumber(fd.getNumber())
                    .build());

            if (fd.isMapField()) {
                if (seg.subscript() == null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "map field '" + seg.fieldName() + "' requires a [key] subscript");
                }
                FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
                FieldDescriptor valFd = fd.getMessageType().findFieldByNumber(2);
                if (valFd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "map field '" + seg.fieldName() + "' values are not protobuf messages");
                }
                ParsedMapKey parsedKey = parseMapKey(seg.subscript(), keyFd, fieldPath);
                navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                        .setMapKey(parsedKey.stepKey())
                        .build());
                Message matchedEntry = null;
                @SuppressWarnings("unchecked")
                List<Message> entries = (List<Message>) curMsg.getField(fd);
                for (Message entry : entries) {
                    if (Objects.equals(entry.getField(keyFd), parsedKey.javaKey())) {
                        matchedEntry = entry;
                    }
                }
                if (matchedEntry == null) {
                    throw ArchiveErrors.notFound(
                            "Occurrence map key '"
                                    + seg.subscript()
                                    + "' is absent at '"
                                    + fieldPath
                                    + "' on revision "
                                    + resolvedRevision);
                }
                curMsg = (Message) matchedEntry.getField(valFd);
                curDesc = valFd.getMessageType();
            } else if (fd.isRepeated()) {
                if (seg.subscript() == null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "repeated field '" + seg.fieldName() + "' requires an [index] subscript");
                }
                int idx = parseNonNegativeIndex(seg.subscript(), fieldPath);
                int count = curMsg.getRepeatedFieldCount(fd);
                if (idx >= count) {
                    throw ArchiveErrors.notFound(
                            "Occurrence index "
                                    + idx
                                    + " out of bounds (size "
                                    + count
                                    + ") at '"
                                    + fieldPath
                                    + "' on revision "
                                    + resolvedRevision);
                }
                navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                        .setRepeatedIndex(idx)
                        .build());
                curMsg = (Message) curMsg.getRepeatedField(fd, idx);
                curDesc = fd.getMessageType();
            } else {
                if (seg.subscript() != null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "singular field '" + seg.fieldName() + "' does not take a subscript");
                }
                if (!curMsg.hasField(fd)) {
                    throw ArchiveErrors.notFound(
                            "Occurrence field '"
                                    + fieldPath
                                    + "' is not set on revision "
                                    + resolvedRevision);
                }
                curMsg = (Message) curMsg.getField(fd);
                curDesc = fd.getMessageType();
            }
        }

        if (ANY_FULL_NAME.equals(curDesc.getFullName())) {
            UnpackedAny unpacked = unpackRetainedAny(
                    docAddress,
                    resolvedRevision,
                    partSelector,
                    fieldPath,
                    curMsg,
                    navSteps,
                    validation,
                    cancellation);
            return new ResolvedOccurrenceValue(
                    unpacked.descriptor(),
                    unpacked.message(),
                    unpacked.valueBytes(),
                    unpacked.descriptorSha256());
        }

        ByteString deterministicPayload =
                ByteString.copyFrom(MessageFingerprints.deterministicBytes(curMsg));
        return new ResolvedOccurrenceValue(
                curDesc, curMsg, deterministicPayload, curDescriptorSha256);
    }

    UnpackedAny unpackRetainedAny(
            NodeAddress docAddress,
            long resolvedRevision,
            PartSelector partSelector,
            String fieldPath,
            Message anyMessage,
            List<RepositorySchemaOccurrenceStep> navSteps,
            RevisionValidation validation,
            RendererProvider.Cancellation cancellation) {
        FieldDescriptor typeUrlField = anyMessage.getDescriptorForType().findFieldByNumber(1);
        FieldDescriptor valueField = anyMessage.getDescriptorForType().findFieldByNumber(2);
        String typeUrl = (String) anyMessage.getField(typeUrlField);
        ByteString valueBytes = (ByteString) anyMessage.getField(valueField);

        for (OpaqueOccurrence opaque : validation.getOpaqueOccurrencesList()) {
            if (opaque.getOccurrence().getPart().equals(partSelector)
                    && stripAnyBoundaries(opaque.getOccurrence().getStepsList()).equals(navSteps)) {
                throw ArchiveErrors.failedPrecondition(
                        "Occurrence '"
                                + fieldPath
                                + "' on revision "
                                + resolvedRevision
                                + " was admitted as opaque ("
                                + opaque.getTypeUrl()
                                + ") without a retained schema and cannot be decoded");
            }
        }

        ResolvedOccurrence matched = null;
        for (ResolvedOccurrence candidate : validation.getResolvedOccurrencesList()) {
            if (!candidate.getOccurrence().getPart().equals(partSelector)) {
                continue;
            }
            if (stripAnyBoundaries(candidate.getOccurrence().getStepsList()).equals(navSteps)
                    && candidate.getSchema().getTypeUrl().equals(typeUrl)) {
                matched = candidate;
                break;
            }
        }
        if (matched == null) {
            throw ArchiveErrors.failedPrecondition(
                    "Occurrence '"
                            + fieldPath
                            + "' ("
                            + typeUrl
                            + ") on revision "
                            + resolvedRevision
                            + " has no retained schema binding in RevisionValidation");
        }

        RetainedSourceSchema occSchema = fetchRetainedSchemaFromDocument(
                docAddress, resolvedRevision, matched.getSchema());
        Descriptor occDesc = linkDescriptor(
                occSchema.descriptorSetBytes(),
                occSchema.messageType(),
                "retained_occurrence_schema",
                "retained_occurrence_type");
        Message decoded = decodeMessage(valueBytes, occDesc, cancellation);
        return new UnpackedAny(occDesc, decoded, valueBytes, occSchema.descriptorSha256());
    }

    static List<RepositorySchemaOccurrenceStep> stripAnyBoundaries(
            List<RepositorySchemaOccurrenceStep> steps) {
        List<RepositorySchemaOccurrenceStep> filtered = new ArrayList<>(steps.size());
        for (RepositorySchemaOccurrenceStep step : steps) {
            if (!step.hasAnyBoundary()) {
                filtered.add(step);
            }
        }
        return filtered;
    }

    RetainedSourceSchema fetchRetainedSchemaFromDocument(
            NodeAddress docAddress,
            long revision,
            RepositorySchemaAssetReference reference) {
        GetSchemaDescriptorResponse resp = archive.documents().getSchemaDescriptor(
                GetSchemaDescriptorRequest.newBuilder()
                        .setAddress(docAddress)
                        .setRevision(DocumentArchive.revision(revision))
                        .setDescriptorSha256(reference.getDescriptorSha256())
                        .build());
        String messageType = typeNameFromUrl(reference.getTypeUrl());
        return new RetainedSourceSchema(
                messageType, resp.getDescriptorSet(), resp.getDescriptorSha256());
    }

    static List<PathSegment> parseOccurrenceFieldPath(String path) {
        String trimmed = path.trim();
        if (trimmed.isEmpty()) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "must not be blank");
        }
        List<PathSegment> segments = new ArrayList<>();
        int i = 0;
        int n = trimmed.length();
        while (i < n) {
            if (segments.size() >= MAX_OCCURRENCE_STEPS) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "exceeds maximum step count of " + MAX_OCCURRENCE_STEPS);
            }
            int nameStart = i;
            while (i < n) {
                char c = trimmed.charAt(i);
                if (c == '.' || c == '[') {
                    break;
                }
                if (!isIdentifierChar(c)) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "invalid character '" + c + "' in field path '" + path + "'");
                }
                i++;
            }
            if (i == nameStart) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "empty field segment in '" + path + "'");
            }
            String fieldName = trimmed.substring(nameStart, i);
            String subscript = null;
            if (i < n && trimmed.charAt(i) == '[') {
                i++;
                int subStart = i;
                boolean inQuotes = false;
                while (i < n) {
                    char c = trimmed.charAt(i);
                    if (c == '\\' && inQuotes && i + 1 < n) {
                        i += 2;
                        continue;
                    }
                    if (c == '"') {
                        inQuotes = !inQuotes;
                    } else if (c == ']' && !inQuotes) {
                        break;
                    }
                    i++;
                }
                if (i >= n || trimmed.charAt(i) != ']') {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "unterminated '[' in '" + path + "'");
                }
                subscript = trimmed.substring(subStart, i);
                i++;
            }
            segments.add(new PathSegment(fieldName, subscript));
            if (i < n) {
                if (trimmed.charAt(i) != '.') {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "expected '.' after segment in '" + path + "'");
                }
                i++;
                if (i == n) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "trailing '.' in '" + path + "'");
                }
            }
        }
        return List.copyOf(segments);
    }

    static boolean isIdentifierChar(char c) {
        return (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c == '_';
    }

    static int parseNonNegativeIndex(String raw, String path) {
        if (raw.isEmpty()) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "empty repeated index in '" + path + "'");
        }
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "negative repeated index in '" + path + "'");
            }
            return value;
        } catch (NumberFormatException invalid) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "invalid repeated index '" + raw + "' in '" + path + "'");
        }
    }

    static ParsedMapKey parseMapKey(String raw, FieldDescriptor keyFd, String path) {
        RepositoryOccurrenceMapKey.Builder step = RepositoryOccurrenceMapKey.newBuilder();
        try {
            return switch (keyFd.getType()) {
                case STRING -> {
                    String unquoted = unquoteMapKey(raw);
                    yield new ParsedMapKey(
                            unquoted,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING)
                                    .setStringValue(unquoted)
                                    .build());
                }
                case BOOL -> {
                    if (!"true".equals(raw) && !"false".equals(raw)) {
                        throw new IllegalArgumentException("invalid bool key");
                    }
                    boolean val = Boolean.parseBoolean(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL)
                                    .setBoolValue(val)
                                    .build());
                }
                case INT32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT32)
                                    .setSignedValue(val)
                                    .build());
                }
                case SINT32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT32)
                                    .setSignedValue(val)
                                    .build());
                }
                case SFIXED32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED32)
                                    .setSignedValue(val)
                                    .build());
                }
                case UINT32 -> {
                    int val = Integer.parseUnsignedInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT32)
                                    .setUnsignedValue(Integer.toUnsignedLong(val))
                                    .build());
                }
                case FIXED32 -> {
                    int val = Integer.parseUnsignedInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED32)
                                    .setUnsignedValue(Integer.toUnsignedLong(val))
                                    .build());
                }
                case INT64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT64)
                                    .setSignedValue(val)
                                    .build());
                }
                case SINT64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64)
                                    .setSignedValue(val)
                                    .build());
                }
                case SFIXED64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED64)
                                    .setSignedValue(val)
                                    .build());
                }
                case UINT64 -> {
                    long val = Long.parseUnsignedLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT64)
                                    .setUnsignedValue(val)
                                    .build());
                }
                case FIXED64 -> {
                    long val = Long.parseUnsignedLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED64)
                                    .setUnsignedValue(val)
                                    .build());
                }
                default -> throw new IllegalArgumentException("unsupported map key type");
            };
        } catch (IllegalArgumentException badKey) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "invalid map key '" + raw + "' in '" + path + "'");
        }
    }

    static String unquoteMapKey(String raw) {
        if (raw.length() >= 2 && raw.charAt(0) == '"' && raw.charAt(raw.length() - 1) == '"') {
            StringBuilder out = new StringBuilder(raw.length() - 2);
            for (int i = 1; i < raw.length() - 1; i++) {
                char c = raw.charAt(i);
                if (c == '\\' && i + 1 < raw.length() - 1) {
                    i++;
                    out.append(raw.charAt(i));
                } else {
                    out.append(c);
                }
            }
            return out.toString();
        }
        return raw;
    }

    record PathSegment(String fieldName, String subscript) {}

    record ParsedMapKey(Object javaKey, RepositoryOccurrenceMapKey stepKey) {}

    record UnpackedAny(
            Descriptor descriptor,
            Message message,
            ByteString valueBytes,
            String descriptorSha256) {}

    record ResolvedOccurrenceValue(
            Descriptor descriptor,
            Message message,
            ByteString payloadBytes,
            String retainedDescriptorSha256) {}

    record RetainedSourceSchema(
            String messageType, ByteString descriptorSetBytes, String descriptorSha256) {}
}
