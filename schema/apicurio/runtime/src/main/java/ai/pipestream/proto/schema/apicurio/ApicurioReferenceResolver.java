package ai.pipestream.proto.schema.apicurio;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.ParsedSchemaImpl;
import io.apicurio.registry.resolver.SchemaParser;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Recursively resolves Apicurio registry artifact references so a protobuf artifact whose
 * {@code .proto} imports other registered artifacts can be parsed.
 *
 * <p>Per the protobuf artifact convention, a registry reference's {@code name} is the exact
 * import string used in the referencing {@code .proto} (e.g. {@code common.proto} for
 * {@code import "common.proto";}). The resolved dependency map handed to
 * {@link SchemaParser#parseSchema(byte[], Map)} is keyed by that import path; nested
 * (transitive) references are attached via {@link ParsedSchema#getSchemaReferences()} with
 * their {@link ParsedSchema#referenceName() reference name} set, which the Apicurio
 * {@code ProtobufSchemaParser} flattens into the wire-schema dependency map.</p>
 *
 * <p>Package-private; the {@link ArtifactSource} seam exists so resolution (cycle guard,
 * transitive/diamond graphs, missing references) is unit-testable without a live registry.</p>
 */
final class ApicurioReferenceResolver {

    /** Version expression selecting the latest version on the default branch. */
    static final String LATEST_VERSION_EXPRESSION = "branch=latest";

    /** Minimal view of the registry used by reference resolution. */
    interface ArtifactSource {

        /** Raw content bytes of an artifact version, or {@code null} if not found. */
        byte[] content(String groupId, String artifactId, String versionExpression) throws Exception;

        /** Outbound references of an artifact version (empty when none). */
        List<Reference> references(String groupId, String artifactId, String versionExpression) throws Exception;
    }

    /**
     * A registry artifact reference. {@code name} is the import path appearing in the
     * referencing .proto file.
     */
    record Reference(String name, String groupId, String artifactId, String version) {
    }

    /**
     * The root artifact itself has no content in the registry. Distinct from the generic
     * {@link IllegalStateException}s raised for unresolved <em>references</em> or cycles, so
     * callers can treat "artifact not found" differently from "artifact exists but is broken".
     */
    static final class ArtifactNotFoundException extends IllegalStateException {
        ArtifactNotFoundException(String message) {
            super(message);
        }
    }

    private final ArtifactSource source;
    private final SchemaParser<ProtobufSchema, ?> schemaParser;

    ApicurioReferenceResolver(ArtifactSource source, SchemaParser<ProtobufSchema, ?> schemaParser) {
        this.source = Objects.requireNonNull(source, "source");
        this.schemaParser = Objects.requireNonNull(schemaParser, "schemaParser");
    }

    /**
     * Fetches an artifact version and parses it with all of its registry references
     * (recursively) resolved.
     *
     * @throws IllegalStateException when the artifact or one of its (transitive) references
     *         cannot be fetched, or when a reference cycle is detected; the message names the
     *         unresolved import
     */
    ProtobufSchema resolveAndParse(String groupId, String artifactId, String versionExpression)
            throws Exception {
        byte[] bytes = source.content(groupId, artifactId, versionExpression);
        if (bytes == null) {
            throw new ArtifactNotFoundException(
                    "Artifact content not found for " + coordinate(groupId, artifactId, versionExpression));
        }
        Map<String, ParsedSchema<ProtobufSchema>> resolved = resolveReferences(
                groupId, artifactId, versionExpression, new LinkedHashSet<>(), new HashMap<>());
        return schemaParser.parseSchema(bytes, resolved);
    }

    /**
     * Resolves the direct references of one artifact version into a map keyed by import path.
     * Each entry carries its own nested references, so transitive imports resolve too.
     *
     * @param inProgress coordinates currently being resolved on this path (cycle guard)
     * @param memo already-resolved artifact versions, so shared references in diamond-shaped
     *        graphs are fetched and parsed only once
     */
    private Map<String, ParsedSchema<ProtobufSchema>> resolveReferences(
            String groupId, String artifactId, String versionExpression,
            Set<String> inProgress, Map<String, ParsedSchema<ProtobufSchema>> memo) throws Exception {

        String coordinate = coordinate(groupId, artifactId, versionExpression);
        if (!inProgress.add(coordinate)) {
            throw new IllegalStateException("Cyclic artifact reference detected: "
                    + String.join(" -> ", inProgress) + " -> " + coordinate);
        }
        try {
            List<Reference> references = source.references(groupId, artifactId, versionExpression);
            Map<String, ParsedSchema<ProtobufSchema>> resolved = new LinkedHashMap<>();
            for (Reference reference : references) {
                String importPath = reference.name();
                String refGroup = reference.groupId() != null ? reference.groupId() : "default";
                String refVersion = reference.version() != null
                        ? reference.version() : LATEST_VERSION_EXPRESSION;
                String refCoordinate = coordinate(refGroup, reference.artifactId(), refVersion);

                ParsedSchema<ProtobufSchema> parsedRef = memo.get(refCoordinate);
                if (parsedRef == null) {
                    if (inProgress.contains(refCoordinate)) {
                        throw new IllegalStateException("Cyclic artifact reference detected: "
                                + String.join(" -> ", inProgress) + " -> " + refCoordinate);
                    }
                    byte[] refBytes;
                    try {
                        refBytes = source.content(refGroup, reference.artifactId(), refVersion);
                    } catch (Exception e) {
                        throw new IllegalStateException("Unresolved reference '" + importPath
                                + "' (" + refCoordinate + "): " + e.getMessage(), e);
                    }
                    if (refBytes == null) {
                        throw new IllegalStateException("Unresolved reference '" + importPath
                                + "' (" + refCoordinate + "): artifact content not found");
                    }
                    Map<String, ParsedSchema<ProtobufSchema>> nested = resolveReferences(
                            refGroup, reference.artifactId(), refVersion, inProgress, memo);
                    ProtobufSchema refSchema = schemaParser.parseSchema(refBytes, nested);
                    parsedRef = new ParsedSchemaImpl<ProtobufSchema>()
                            .setParsedSchema(refSchema)
                            .setRawSchema(refBytes)
                            .setReferenceName(importPath)
                            .setSchemaReferences(new ArrayList<>(nested.values()));
                    memo.put(refCoordinate, parsedRef);
                }
                resolved.putIfAbsent(importPath, parsedRef);
            }
            return resolved;
        } finally {
            inProgress.remove(coordinate);
        }
    }

    private static String coordinate(String groupId, String artifactId, String versionExpression) {
        return groupId + "/" + artifactId + "@" + versionExpression;
    }
}
