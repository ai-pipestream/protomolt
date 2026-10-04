package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/** Attempt-local assets; failed preparation never returns a partial proof or writes retained storage. */
final class DocumentSchemaDefinitions {
    private static final int MIB = 1024 * 1024;
    private static DocumentPayloadCheck.SchemaKey key(DocumentSchemaAdmission.Definition definition) {
        return new DocumentPayloadCheck.SchemaKey(definition.metadata().getTypeUrl(), definition.metadata().getArtifactSha256());
    }
    private final DocumentSchemaAdmission.Limits limits;
    private final Runnable control;
    private final DocumentAdmissionResources resources;
    final Map<String, ByteString> artifacts = new HashMap<>();
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Definition> selected = new HashMap<>();
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAssetBinding> bindings = new HashMap<>();
    final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaBinding> schemas = new HashMap<>();
    final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference> references = new LinkedHashMap<>();
    private final DocumentRetainedSchemaAssets retained;
    private long bytes;

    DocumentSchemaDefinitions(DocumentSchemaAdmission.Limits limits, DocumentAdmissionResources resources, Runnable control) {
        this.limits = limits; this.resources = resources; this.control = control;
        retained = new DocumentRetainedSchemaAssets(hash -> Optional.ofNullable(artifacts.get(hash)),
                new DocumentRetainedSchemaAssets.Limits(limits.maxBindings(), limits.maxRetainedBytes(),
                        new ClosedDescriptorSet.Limits(16 * MIB, 256, 4096, 64)), resources);
    }

    DocumentSchemaAssetBinding select(DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Selection request) {
        control.run();
        var definition = resolver.select(request);
        control.run();
        if (definition == null) throw new IllegalArgumentException("unresolved Any schema definition: " + request.typeUrl());
        if (!definition.metadata().getTypeUrl().equals(request.typeUrl()))
            throw new IllegalArgumentException("resolved schema asset type URL mismatch");
        return add(definition);
    }

    DocumentSchemaAssetBinding add(DocumentSchemaAdmission.Definition definition) {
        control.run();
        var key = key(definition);
        var previous = selected.get(key);
        if (previous != null) {
            if (!previous.equals(definition)) throw new IllegalArgumentException("conflicting definition for one schema identity");
            return bindings.get(key);
        }
        if (selected.size() >= limits.maxBindings()) throw new IllegalArgumentException("schema binding count exceeds limit");
        if (definition.descriptors().isEmpty() || definition.descriptors().size() > 16 * MIB
                || definition.source().filter(value -> value.isEmpty() || value.size() > 16 * MIB).isPresent())
            throw new IllegalArgumentException("definition artifact exceeds byte limit");
        var compilation = definition.metadata().getCompilation();
        if (compilation.hasSourceArtifactSha256() != definition.source().isPresent())
            throw new IllegalArgumentException("source bytes differ from metadata presence");
        var sourceHash = compilation.hasSourceArtifactSha256() ? Optional.of(compilation.getSourceArtifactSha256()) : Optional.<String>empty();
        var encoded = encodeMetadata(definition);
        var reference = new DocumentSchemaAdmission.Reference(key.typeUrl(), key.artifactSha256(),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), sourceHash);
        var descriptors = put(key.artifactSha256(), definition.descriptors(), true);
        var source = sourceHash.map(hash -> put(hash, definition.source().orElseThrow(), true));
        var bound = retained.resolve(new DocumentRetainedSchemaAssets.Reference(reference.typeUrl(), reference.descriptorSha256(),
                reference.metadataCodec(), reference.metadataVersion(), reference.metadataSha256(), reference.sourceSha256()), control);
        selected.put(key, new DocumentSchemaAdmission.Definition(definition.metadata(), descriptors, source));
        bindings.put(key, bound); schemas.put(key, bound.schema()); references.put(key, reference);
        return bound;
    }

    private DocumentSchemaAssetCodec.Encoded encodeMetadata(DocumentSchemaAdmission.Definition definition) {
        if (resources == null) {
            var encoded = DocumentSchemaAssetCodec.encode(definition.metadata(), control);
            put(encoded.sha256(), encoded.bytes(), false);
            return encoded;
        }
        var owner = DocumentSchemaAssetCodec.encodeOwned(definition.metadata(), resources, control);
        boolean transferred = false;
        try {
            var encoded = owner.value();
            var bytes = put(encoded.sha256(), encoded.bytes(), false);
            if (bytes == encoded.bytes()) {
                resources.retain(owner);
                transferred = true;
            }
            return new DocumentSchemaAssetCodec.Encoded(bytes, encoded.sha256());
        } finally {
            if (!transferred) owner.close();
        }
    }

    private ByteString put(String digest, ByteString value, boolean copy) {
        control.run();
        var prior = artifacts.get(digest);
        if (prior != null) {
            if (!prior.equals(value)) throw new IllegalArgumentException("conflicting bytes for artifact digest");
            return prior;
        }
        if (artifacts.size() >= 64 || value.size() > limits.maxRetainedBytes() - bytes)
            throw new IllegalArgumentException("retained artifact union exceeds limit");
        var retainedValue = resources != null && copy ? resources.copy(value, control) : value;
        artifacts.put(digest, retainedValue); bytes += value.size();
        return retainedValue;
    }
}
