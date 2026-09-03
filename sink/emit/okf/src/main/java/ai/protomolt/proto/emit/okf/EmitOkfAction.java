package ai.protomolt.proto.emit.okf;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.emit.Bundle;
import ai.protomolt.proto.emit.Bundles;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;

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
public final class EmitOkfAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema resolved = SchemaResolver.resolve(input, "schema", context);
        // An omitted title arrives as the empty string, which the renderer reads as none.
        String title = Fields.string(input, "title");

        Bundle bundle = new OkfRenderer().render(resolved.files(),
                new OkfRenderer.Options(title.isEmpty() ? null : title, null));

        Reply result = Reply.of(responseType())
                .set("ok", true)
                .set("fileCount", bundle.size());
        bundle.forEach((path, content) -> result.append("files")
                .set("key", path)
                .set("value", new String(content, StandardCharsets.UTF_8))
                .build());
        return result.set("zipBase64",
                Base64.getEncoder().encodeToString(Bundles.zip(bundle))).build();
    }
}
