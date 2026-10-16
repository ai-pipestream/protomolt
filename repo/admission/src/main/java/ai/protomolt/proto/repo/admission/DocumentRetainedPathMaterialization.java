package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * One recorded typed path through retained Any boundaries. No registry or admission
 * validator. The host authenticates the path's membership in revision evidence,
 * exact fragment/ordinal and associations, and owns all input/result lifetimes.
 */
final class DocumentRetainedPathMaterialization {
    private DocumentRetainedPathMaterialization() {}
    /** Aggregate boundary value bytes/decodes; wire values are bounded by maxBoundaries * perBoundary.maxWireValues. */
    record Limits(long maxDecodedBytes, int maxBoundaries, int maxReferences, DocumentAnyMaterialization.Limits perBoundary) {
        Limits {
            if (maxDecodedBytes < 1 || maxBoundaries < 1 || maxBoundaries > 101 || maxReferences < 1)
                throw new IllegalArgumentException("positive decoded byte bound and at most 101 boundaries required");
            Objects.requireNonNull(perBoundary);
        }
    }
    /** Refusal before a target is located cannot return a target envelope or a partial view. */
    static final class LimitExceeded extends IllegalArgumentException {
        LimitExceeded(String message) { super(message); }
    }

    static DocumentAnyMaterialization.Decoded read(int ordinal, DocumentAnyRootInventory.Result inventory,
            DocumentSchemaRootLocator root, RepositorySchemaOccurrencePath path,
            List<DocumentSchemaAdmission.Reference> references,
            DocumentRetainedSchemaAssets retained, Limits limits, Runnable control) {
        Objects.requireNonNull(path); Objects.requireNonNull(references); Objects.requireNonNull(limits);
        Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("retained path read interrupted");
            control.run();
        };
        active.run();
        // Stored path bytes already passed their canonical codec/digest gate in the host.
        DocumentSchemaEvidenceCodec.measureAndValidate(path, active);
        if (references.size() > limits.maxReferences()) throw new LimitExceeded("retained path reference count exceeds bound");
        var associations = new java.util.HashMap<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference>();
        for (var reference : references) {
            active.run();
            var key = new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256());
            var previous = associations.putIfAbsent(key, reference);
            if (previous != null && !previous.equals(reference)) throw lost("conflicting retained schema associations");
        }
        Object current = null;
        FieldDescriptor collection = null;
        DocumentAnyMaterialization.Decoded last = null;
        long remainingBytes = limits.maxDecodedBytes();
        int boundaries = 0;
        for (int i = 0; i < path.getStepsCount(); i++) {
            active.run();
            var step = path.getSteps(i);
            switch (step.getStepCase()) {
                case ANY_BOUNDARY -> {
                    if (++boundaries > limits.maxBoundaries()) throw new LimitExceeded("retained path boundary count exceeds bound");
                    var boundary = step.getAnyBoundary();
                    if (boundary.getValueSizeBytes() < 0 || boundary.getValueSizeBytes() > remainingBytes)
                        throw new LimitExceeded("retained path aggregate value bytes exceed bound");
                    var key = new DocumentPayloadCheck.SchemaKey(boundary.getTypeUrl(), boundary.getResolved().getArtifactSha256());
                    var reference = associations.get(key);
                    if (reference == null) throw lost("recorded boundary has no retained schema association");
                    final DocumentAnyMaterialization.View view;
                    if (i == 0) {
                        view = DocumentRetainedRootMaterialization.read(ordinal, inventory, root, boundary,
                                DocumentAnyMaterialization.Mode.MATERIALIZE_IF_AVAILABLE, reference, retained, limits.perBoundary(), active);
                    } else {
                        var envelope = envelope(current);
                        var occurrence = new DocumentSchemaAdmission.Selection(ordinal, root, boundary.getTypeUrl(),
                                path.getStepsList().subList(0, i), boundary.getValueSha256(), boundary.getValueSizeBytes());
                        view = DocumentRetainedAnyMaterialization.read(occurrence, envelope, boundary,
                                DocumentAnyMaterialization.Mode.MATERIALIZE_IF_AVAILABLE, reference, retained, limits.perBoundary(), active);
                    }
                    if (!(view instanceof DocumentAnyMaterialization.Decoded decoded)) {
                        if (view instanceof DocumentAnyMaterialization.Failed failed
                                && failed.failure().reason() == DocumentAnyMaterialization.Reason.RESOURCE_LIMIT)
                            throw new LimitExceeded("retained boundary materialization exceeds configured resources");
                        throw new IllegalStateException("retained decoder returned an unexpected non-decoded outcome");
                    }
                    remainingBytes -= decoded.original().getValue().size();
                    last = decoded; current = decoded.value(); collection = null;
                }
                case FIELD_NUMBER -> {
                    var message = message(current);
                    if (message.getDescriptorForType().getFullName().equals("google.protobuf.Any"))
                        throw lost("Any envelope must be crossed using its recorded boundary");
                    var field = message.getDescriptorForType().findFieldByNumber(step.getFieldNumber());
                    if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE)
                        throw lost("recorded path does not select a message field");
                    if (!field.isRepeated() && !message.hasField(field)) throw lost("recorded path selects an absent message field");
                    current = message.getField(field);
                    collection = field.isRepeated() ? field : null;
                }
                case REPEATED_INDEX -> {
                    if (collection == null || collection.isMapField() || !(current instanceof List<?> values)
                            || step.getRepeatedIndex() < 0 || step.getRepeatedIndex() >= values.size())
                        throw lost("recorded repeated index does not select a value");
                    current = values.get(step.getRepeatedIndex()); collection = null;
                }
                case MAP_KEY -> {
                    if (collection == null || !collection.isMapField() || !(current instanceof List<?> values))
                        throw lost("recorded map key does not follow a map field");
                    try { DocumentPayloadCheck.requireMapEntry(collection.getMessageType()); }
                    catch (IllegalArgumentException failure) {
                        throw new DocumentRetainedSchemaAssets.DataLoss("retained map entry descriptor is unsupported", failure);
                    }
                    var keyField = collection.getMessageType().findFieldByNumber(1);
                    var valueField = collection.getMessageType().findFieldByNumber(2);
                    if (keyField == null || valueField == null || valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE)
                        throw lost("recorded map path has unsupported entry shape");
                    var keys = new HashSet<Object>();
                    Object selected = null;
                    for (var item : values) {
                        active.run();
                        var entry = message(item);
                        Object key = entry.getField(keyField);
                        if (!keys.add(key)) throw lost("duplicate map keys make the recorded path ambiguous");
                        var projected = DocumentSchemaOccurrenceProjection.mapKey(new DocumentSchemaOccurrences.MapKey(keyField.getType(), key));
                        if (projected.equals(step.getMapKey())) {
                            if (!entry.hasField(valueField)) throw lost("recorded map path selects an absent message value");
                            selected = entry.getField(valueField);
                        }
                    }
                    if (selected == null) throw lost("recorded map key does not select a value");
                    current = selected; collection = null;
                }
                case STEP_NOT_SET -> throw lost("recorded path has no step kind");
            }
        }
        active.run();
        if (last == null) throw lost("recorded path has no Any boundary");
        return last;
    }

    private static Message message(Object value) {
        if (!(value instanceof Message message)) throw lost("recorded path requires a message");
        if (!message.getUnknownFields().asMap().isEmpty()) throw lost("unknown fields on retained access path");
        return message;
    }
    private static Any envelope(Object value) {
        var message = message(value);
        var type = message.getDescriptorForType();
        var url = type.findFieldByNumber(1);
        var bytes = type.findFieldByNumber(2);
        if (!type.getFullName().equals("google.protobuf.Any") || type.getFields().size() != 2
                || url == null || bytes == null || !url.getName().equals("type_url") || !bytes.getName().equals("value")
                || url.getType() != FieldDescriptor.Type.STRING || bytes.getType() != FieldDescriptor.Type.BYTES
                || url.isRepeated() || bytes.isRepeated() || url.isRequired() || bytes.isRequired()
                || url.hasDefaultValue() || bytes.hasDefaultValue()
                || url.getContainingOneof() != null || bytes.getContainingOneof() != null)
            throw lost("recorded path does not reach a supported Any envelope");
        return Any.newBuilder().setTypeUrl((String) message.getField(url)).setValue((ByteString) message.getField(bytes)).build();
    }
    private static DocumentRetainedSchemaAssets.DataLoss lost(String message) {
        return new DocumentRetainedSchemaAssets.DataLoss(message);
    }
}
