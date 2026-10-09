package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;

/**
 * Schema metadata bound to exact descriptor bytes. This does not authenticate
 * compiler claims, verify source retention, authorize access or admit a payload.
 * Checks apply to the parsed metadata tree, not its original wire encoding.
 * A payload consumer must match the exact metadata type URL before using this binding.
 */
final class DocumentSchemaAssetBinding {
    private static final int MAX_METADATA_BYTES = 512 * 1024;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private final RepositorySchemaAsset metadata;
    private final DocumentSchemaBinding schema;

    private DocumentSchemaAssetBinding(RepositorySchemaAsset metadata, DocumentSchemaBinding schema) {
        this.metadata = metadata;
        this.schema = schema;
    }

    RepositorySchemaAsset metadata() { return metadata; }
    DocumentSchemaBinding schema() { return schema; }

    /** The trusted host must separately verify tool identities and retained source assets. */
    static DocumentSchemaAssetBinding bind(RepositorySchemaAsset metadata, ByteString artifact,
            ClosedDescriptorSet.Limits limits, Runnable control) {
        return bind(metadata, artifact, limits, null, control);
    }

    static DocumentSchemaAssetBinding bind(RepositorySchemaAsset metadata, ByteString artifact,
            ClosedDescriptorSet.Limits limits, DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(control, "control");
        control.run();
        if (metadata.getSerializedSize() > MAX_METADATA_BYTES) {
            throw new IllegalArgumentException("schema asset metadata exceeds byte limit");
        }
        rejectUnknownFields(metadata, control);
        if (!VALIDATOR.validate(metadata).valid()) {
            throw new IllegalArgumentException("invalid schema asset metadata");
        }
        control.run();
        var schema = DocumentSchemaBinding.bind(metadata.getSchema(), artifact, limits, reservations, control);
        if (!schema.artifactSha256().equals(metadata.getArtifactSha256())) {
            throw new IllegalArgumentException("schema asset exact artifact digest mismatch");
        }
        control.run();
        return new DocumentSchemaAssetBinding(metadata, schema);
    }

    private static void rejectUnknownFields(Message root, Runnable control) {
        var pending = new ArrayDeque<Message>();
        pending.add(root);
        while (!pending.isEmpty()) {
            control.run();
            var value = pending.removeFirst();
            if (!value.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("unsupported schema asset metadata fields");
            }
            for (var entry : value.getAllFields().entrySet()) {
                if (entry.getKey().getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
                if (entry.getKey().isRepeated()) {
                    for (var element : (List<?>) entry.getValue()) pending.add((Message) element);
                } else {
                    pending.add((Message) entry.getValue());
                }
            }
        }
    }
}
