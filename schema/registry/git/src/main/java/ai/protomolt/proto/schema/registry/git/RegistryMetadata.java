package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.CompatibilityModes;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.StoredSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Package-private metadata JSON transformations for {@link GitSchemaRegistryStore}:
 * the per-version metadata document, the global registry document, and the
 * checked reads of both.
 *
 * <p>Pure document/model transformations only: content comes from committed
 * objects ({@link RegistryGitObjects}), and locking and commits stay in
 * {@link GitSchemaRegistryStore}, so the JSON shapes here are the store's
 * persisted formats and must not drift.
 */
final class RegistryMetadata {

    private RegistryMetadata() {}

    /** The {@code v<N>.json} metadata document for one stored schema version. */
    static String metadataJson(ObjectMapper json, StoredSchema stored) {
        ObjectNode node = json.createObjectNode()
                .put("globalId", stored.globalId())
                .put("contentHash", stored.contentHash());
        ArrayNode array = node.putArray("references");
        for (SchemaReference reference : stored.references()) {
            array.addObject()
                    .put("name", reference.name())
                    .put("subject", reference.subject())
                    .put("version", reference.version());
        }
        return node.toPrettyString();
    }

    /** The global {@code registry.json} document: compatibility mode plus id counter. */
    static String registryJson(ObjectMapper json, String globalMode, int nextGlobalId) {
        ObjectNode node = json.createObjectNode()
                .put("compatibility", globalMode)
                .put("nextGlobalId", nextGlobalId);
        return node.toPrettyString();
    }

    /** Reads one metadata document from committed bytes, requiring a JSON object at its root. */
    static JsonNode readMetadata(ObjectMapper json, String source, byte[] bytes) {
        try {
            JsonNode node = json.readTree(new String(bytes, StandardCharsets.UTF_8));
            if (node == null || !node.isObject()) {
                throw new RegistryStoreException("Expected a metadata object in " + source);
            }
            return node;
        } catch (IOException e) {
            throw new RegistryStoreException("Cannot read registry metadata " + source, e);
        }
    }

    /** The validated {@code compatibility} value of a metadata document. */
    static String readCompatibility(JsonNode node, String source) {
        JsonNode value = node.path("compatibility");
        if (!value.isTextual() || !CompatibilityModes.isValid(value.textValue())) {
            throw new RegistryStoreException("Invalid compatibility policy in " + source);
        }
        return value.textValue();
    }

    /** The schema references recorded in one version's metadata document. */
    static List<SchemaReference> referencesOf(JsonNode meta) {
        List<SchemaReference> references = new ArrayList<>();
        for (JsonNode reference : meta.path("references")) {
            references.add(new SchemaReference(
                    reference.path("name").asText(),
                    reference.path("subject").asText(),
                    reference.path("version").asInt()));
        }
        return references;
    }

    /** One stored schema version assembled from its schema text and metadata document. */
    static StoredSchema storedSchema(String subject, int version, String schemaText, JsonNode meta) {
        return new StoredSchema(subject, version,
                meta.path("globalId").asInt(), schemaText, referencesOf(meta),
                meta.path("contentHash").asText());
    }
}
