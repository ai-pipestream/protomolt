package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Rebuilds a definition from the artifacts a revision retained for it (requirements
 * §9, §15.14). There is no registry, latest-version lookup or compiler fallback:
 * the reference's canonical metadata, descriptor closure and optional source are
 * read from the retained store and checked against their recorded digests.
 */
public final class DocumentRetainedDefinitions {
    private static final int MIB = 1024 * 1024;

    private DocumentRetainedDefinitions() {}

    /** The retained artifact store. Empty is authoritative absence; failures to reach it propagate. */
    @FunctionalInterface public interface Reader {
        Optional<ByteString> read(String sha256);
    }

    /**
     * @param reference the retained association recorded on a revision
     * @param reader the retained artifact store
     * @return the definition, with bytes that match every recorded digest
     * @throws DocumentSchemaAdmission.DataLoss when an artifact is absent, corrupt or inconsistent
     */
    public static DocumentSchemaAdmission.Definition definition(RepositorySchemaAssetReference reference, Reader reader) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(reader, "reader");
        var read = new HashMap<String, ByteString>();
        DocumentRetainedSchemaAssets.Reader once = hash -> {
            var cached = read.get(hash);
            if (cached != null) return Optional.of(cached);
            var bytes = reader.read(hash);
            bytes.ifPresent(value -> read.put(hash, value));
            return bytes;
        };
        var assets = new DocumentRetainedSchemaAssets(once, new DocumentRetainedSchemaAssets.Limits(1, 64L * MIB,
                new ClosedDescriptorSet.Limits(16 * MIB, 256, 4096, 64)));
        var bound = assets.resolve(new DocumentRetainedSchemaAssets.Reference(reference.getTypeUrl(),
                reference.getDescriptorSha256(), reference.getMetadataCodec(), reference.getMetadataVersion(),
                reference.getMetadataSha256(), reference.hasSourceSha256() ? Optional.of(reference.getSourceSha256())
                        : Optional.empty()), () -> {});
        var source = reference.hasSourceSha256() ? Optional.of(read.get(reference.getSourceSha256())) : Optional.<ByteString>empty();
        return new DocumentSchemaAdmission.Definition(bound.metadata(), read.get(reference.getDescriptorSha256()), source);
    }
}
