package ai.pipestream.proto.mcp;

import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Adapts a {@link SchemaRegistryStore} to MCP resources, so an agent can browse subjects and
 * read schema versions without spending tool calls.
 *
 * <p>URIs: {@code protomolt://registry/subjects} lists every subject;
 * {@code protomolt://registry/subjects/{subject}} is one subject's version index plus its
 * latest schema; {@code protomolt://registry/subjects/{subject}/versions/{n}} is one exact
 * version. Subject names are URL-encoded in URIs, matching the registry's own storage
 * encoding. All contents are JSON documents.</p>
 */
public final class RegistryResources implements McpResources {

    private static final String ROOT = "protomolt://registry/subjects";

    private final SchemaRegistryStore store;

    public RegistryResources(SchemaRegistryStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** The resource index: the subjects list plus one entry per subject. */
    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode resources = mapper.createArrayNode();
        ObjectNode index = resources.addObject();
        index.put("uri", ROOT);
        index.put("name", "subjects");
        index.put("description", "All subjects in the schema registry, with the global compatibility mode");
        index.put("mimeType", "application/json");
        for (String subject : store.subjects()) {
            ObjectNode entry = resources.addObject();
            entry.put("uri", subjectUri(subject));
            entry.put("name", subject);
            entry.put("description", "Version index and latest schema for subject " + subject);
            entry.put("mimeType", "application/json");
        }
        return resources;
    }

    @Override
    public Page page(ObjectMapper mapper, String cursor, int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        int offset = parseOffset(cursor);
        List<String> subjects = store.subjects();
        int total = subjects.size() + 1; // the registry root is the first resource
        if (offset > total) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        int end = Math.min(offset + pageSize, total);
        ArrayNode page = mapper.createArrayNode();
        for (int i = offset; i < end; i++) {
            if (i == 0) {
                resource(page, ROOT, "subjects",
                        "All subjects in the schema registry, with the global compatibility mode");
            } else {
                String subject = subjects.get(i - 1);
                resource(page, subjectUri(subject), subject,
                        "Version index and latest schema for subject " + subject);
            }
        }
        return new Page(page, end < total ? Integer.toString(end) : null);
    }

    /** The contents of one resource, or empty when the URI is not served. */
    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        if (ROOT.equals(uri)) {
            return Optional.of(contents(mapper, uri, subjectsIndex(mapper)));
        }
        if (!uri.startsWith(ROOT + "/")) {
            return Optional.empty();
        }
        String rest = uri.substring(ROOT.length() + 1);
        int slash = rest.indexOf('/');
        if (slash < 0) {
            String subject = decode(rest);
            return subjectDocument(mapper, subject)
                    .map(doc -> contents(mapper, uri, doc));
        }
        String subject = decode(rest.substring(0, slash));
        String tail = rest.substring(slash + 1);
        if (!tail.startsWith("versions/")) {
            return Optional.empty();
        }
        int version;
        try {
            version = Integer.parseInt(tail.substring("versions/".length()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return store.version(subject, version)
                .map(schema -> contents(mapper, uri, schemaDocument(mapper, schema)));
    }

    private ObjectNode subjectsIndex(ObjectMapper mapper) {
        ObjectNode doc = mapper.createObjectNode();
        ArrayNode subjects = doc.putArray("subjects");
        store.subjects().forEach(subjects::add);
        doc.put("globalCompatibilityMode", store.globalCompatibilityMode());
        return doc;
    }

    private Optional<ObjectNode> subjectDocument(ObjectMapper mapper, String subject) {
        var versions = store.versions(subject);
        if (versions.isEmpty()) {
            return Optional.empty();
        }
        ObjectNode doc = mapper.createObjectNode();
        doc.put("subject", subject);
        ArrayNode list = doc.putArray("versions");
        versions.forEach(list::add);
        store.compatibilityMode(subject)
                .ifPresent(mode -> doc.put("compatibilityMode", mode));
        store.latest(subject)
                .ifPresent(latest -> doc.set("latest", schemaDocument(mapper, latest)));
        return Optional.of(doc);
    }

    private ObjectNode schemaDocument(ObjectMapper mapper, StoredSchema schema) {
        ObjectNode doc = mapper.createObjectNode();
        doc.put("subject", schema.subject());
        doc.put("version", schema.version());
        doc.put("globalId", schema.globalId());
        doc.put("schemaText", schema.schemaText());
        ArrayNode references = doc.putArray("references");
        schema.references().forEach(ref -> {
            ObjectNode node = references.addObject();
            node.put("name", ref.name());
            node.put("subject", ref.subject());
            node.put("version", ref.version());
        });
        return doc;
    }

    private ObjectNode contents(ObjectMapper mapper, String uri, ObjectNode document) {
        ObjectNode contents = mapper.createObjectNode();
        contents.put("uri", uri);
        contents.put("mimeType", "application/json");
        contents.put("text", document.toString());
        return contents;
    }

    private static String subjectUri(String subject) {
        return ROOT + "/" + URLEncoder.encode(subject, StandardCharsets.UTF_8);
    }

    private static int parseOffset(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        try {
            int offset = Integer.parseInt(cursor);
            if (offset < 0) {
                throw new NumberFormatException();
            }
            return offset;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
    }

    private static void resource(ArrayNode resources, String uri, String name, String description) {
        ObjectNode entry = resources.addObject();
        entry.put("uri", uri);
        entry.put("name", name);
        entry.put("description", description);
        entry.put("mimeType", "application/json");
    }

    private static String decode(String encoded) {
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }
}
