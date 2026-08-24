package ai.pipestream.proto.emit.okf;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.emit.Bundle;
import ai.pipestream.proto.emit.Bundles;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.Descriptors.Descriptor;
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
public final class EmitOkfAction implements JsonAction {

    @Override
    public String name() {
        return "emit-okf";
    }

    @Override
    public String requiredScope() {
        return Scopes.ARTIFACT_ACCESS;
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
    public Descriptor requestType() {
        return CatalogContract.request("EmitOkfRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("EmitOkfResponse");
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
