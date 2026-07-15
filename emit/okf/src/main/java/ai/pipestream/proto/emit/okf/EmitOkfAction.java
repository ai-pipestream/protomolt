package ai.pipestream.proto.emit.okf;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.emit.Bundle;
import ai.pipestream.proto.emit.Bundles;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Base64;

/**
 * The {@code emit-okf} verb: render a schema as an Open Knowledge Format (OKF v0.1) bundle —
 * markdown concept documents with YAML frontmatter for every message, enum, and service,
 * cross-linked into a knowledge graph, with schema tables carrying the descriptions and
 * sensitivity classes declared in the contract. The response returns the bundle inline (a
 * path-to-markdown map plus the same files as one base64 zip); delivery to a directory or a
 * git repository is the caller's move through the emit sinks. No destination ever rides in
 * the request.
 */
public final class EmitOkfAction implements ProtoAction {

    @Override
    public String name() {
        return "emit-okf";
    }

    @Override
    public String description() {
        return "Renders a schema as an Open Knowledge Format (OKF v0.1) bundle: one markdown "
                + "concept document per message, enum, and service with YAML frontmatter, "
                + "schema tables (field descriptions and sensitivity classes from the "
                + "contract's metadata annotations), cross-links between types, and index "
                + "files. Returns the files inline plus a base64 zip of the whole bundle - "
                + "the knowledge-graph form agents and data catalogs consume.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        ObjectNode schemaProperty = properties.putObject("schema");
        schemaProperty.put("type", "object");
        schemaProperty.put("description", "The schema to render: {type} for a registered "
                + "type, {sources} for inline .proto files, or {descriptorSetBase64}.");
        properties.putObject("title")
                .put("type", "string")
                .put("description", "Heading of the bundle's root index.md; default "
                        + "'Schema knowledge bundle'.");
        schema.putArray("required").add("schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema resolved = SchemaResolver.resolve(input, "schema", context);
        JsonNode titleNode = input.get("title");
        String title = titleNode != null && titleNode.isTextual() ? titleNode.asText() : null;

        Bundle bundle = new OkfRenderer().render(resolved.files(),
                new OkfRenderer.Options(title, null));

        ObjectNode result = context.objectMapper().createObjectNode();
        result.put("ok", true);
        result.put("fileCount", bundle.size());
        ObjectNode files = result.putObject("files");
        bundle.forEach((path, content) -> files.put(path,
                new String(content, java.nio.charset.StandardCharsets.UTF_8)));
        result.put("zipBase64", Base64.getEncoder().encodeToString(Bundles.zip(bundle)));
        return result;
    }
}
