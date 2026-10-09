package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.RuntimeVersion;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Properties;

/** Immutable V1 container definition from the installed contract artifact, never request metadata. */
public final class BuiltinDocumentSchema {
    private static final String COMPILER_RESOURCE="/META-INF/protomolt/repo-proto/compiler.properties";
    private BuiltinDocumentSchema() {}

    /** Complete generated descriptor closure and producer-reported compiler artifact identity. */
    public static DocumentSchemaAdmission.Definition definition() { return Holder.DEFINITION; }

    private static final class Holder {
        private static final DocumentSchemaAdmission.Definition DEFINITION=load();
    }

    private static DocumentSchemaAdmission.Definition load() {
        var properties=new Properties();
        try (var input=Document.class.getResourceAsStream(COMPILER_RESOURCE)) {
            if (input==null) throw new IllegalStateException("Document contract compiler identity is missing");
            properties.load(input);
        } catch (IOException failure) { throw new UncheckedIOException("Cannot read document compiler identity",failure); }
        var descriptor=Document.getDescriptor();
        var closure=DescriptorFingerprints.closure(descriptor);
        var bytes=closure.toByteString();
        var compilation=SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED)
                .setKnownCompiler(SchemaCompilerDetails.newBuilder().setTool(SchemaToolIdentity.newBuilder()
                        .setName(required(properties,"compiler.name")).setVersion(required(properties,"compiler.version"))))
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("com.google.protobuf:protobuf-java")
                        .setVersion(loadedRuntimeVersion()));
        var metadata=RepositorySchemaAsset.newBuilder().setTypeUrl("type.googleapis.com/"+descriptor.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(descriptor.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(compilation).build();
        DocumentSchemaAssetCodec.encode(metadata,() -> {});
        return new DocumentSchemaAdmission.Definition(metadata,bytes,Optional.empty());
    }

    private static String required(Properties properties,String key) {
        var value=properties.getProperty(key);
        if (value==null || value.isBlank()) throw new IllegalStateException("Document compiler identity lacks "+key);
        return value;
    }

    private static String loadedRuntimeVersion() {
        // Direct references to ConstantValue fields would report this module's compile-time dependency.
        try {
            return RuntimeVersion.class.getField("MAJOR").get(null)+"."
                    +RuntimeVersion.class.getField("MINOR").get(null)+"."
                    +RuntimeVersion.class.getField("PATCH").get(null)
                    +RuntimeVersion.class.getField("SUFFIX").get(null);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Cannot identify the loaded protobuf runtime",failure);
        }
    }
}
