package ai.pipestream.proto.gather.git;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSource;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * The {@code gather-git} verb: pull {@code .proto} sources straight from a git repository,
 * compile them, and return both the source texts (registerable as registry subjects) and the
 * compiled descriptor set (usable as the {@code schema} input to every other verb). This is
 * the fallback lane for services that publish their contract in git instead of enabling
 * reflection.
 */
public final class GatherGitAction implements ProtoAction {

    private final Path cacheRoot;

    /** Clone caches under the library default ({@code ~/.cache/protomolt/gather/git}). */
    public GatherGitAction() {
        this(null);
    }

    /**
     * Clone caches under {@code cacheRoot} (plus the standard per-repo hash) — the
     * operator's choice of disk location, never the caller's: cache placement is server
     * configuration, not request input.
     */
    public GatherGitAction(Path cacheRoot) {
        this.cacheRoot = cacheRoot;
    }

    @Override
    public String name() {
        return "gather-git";
    }

    @Override
    public String description() {
        return "Gathers .proto sources from a git repository (branch, tag, or commit) and "
                + "compiles them: returns the source texts keyed by import path plus a base64 "
                + "descriptor set usable as the 'schema' input to the other verbs. The lane for "
                + "services that publish their contract in git rather than enabling reflection.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("repo")
                .put("type", "string")
                .put("description", "Git repository URL (https or ssh), e.g. "
                        + "'https://github.com/kserve/open-inference-protocol.git'.");
        properties.putObject("ref")
                .put("type", "string")
                .put("description", "Branch, tag, or commit SHA; default 'main'.");
        ObjectNode paths = properties.putObject("paths");
        paths.put("type", "array");
        paths.put("description", "Specific .proto files or directories to gather, relative "
                + "to 'subdir'; default: every .proto under 'subdir'.");
        paths.putObject("items").put("type", "string");
        properties.putObject("subdir")
                .put("type", "string")
                .put("description", "Directory within the repository that import paths are "
                        + "relative to; default 'proto'. Use '.' for the repository root.");
        schema.putArray("required").add("repo");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) {
        ObjectNode result = context.objectMapper().createObjectNode();
        JsonNode repoNode = input.get("repo");
        if (repoNode == null || !repoNode.isTextual() || repoNode.asText().isBlank()) {
            result.put("ok", false);
            result.put("error", "'repo' must be a non-empty git URL");
            return result;
        }
        GitProtoGatherer.Builder builder = GitProtoGatherer.builder()
                .repo(repoNode.asText());
        if (cacheRoot != null) {
            builder.cacheRoot(cacheRoot);
        }
        JsonNode ref = input.get("ref");
        if (ref != null && ref.isTextual() && !ref.asText().isBlank()) {
            builder.ref(ref.asText());
        }
        JsonNode subdir = input.get("subdir");
        if (subdir != null && subdir.isTextual() && !subdir.asText().isBlank()) {
            builder.subdir(subdir.asText());
        }
        JsonNode pathsNode = input.get("paths");
        if (pathsNode != null && pathsNode.isArray() && !pathsNode.isEmpty()) {
            List<String> paths = new ArrayList<>();
            pathsNode.forEach(p -> paths.add(p.asText()));
            builder.paths(paths);
        }

        ProtoSourceSet gathered;
        try {
            gathered = builder.build().gather();
        } catch (Exception e) {
            result.put("ok", false);
            result.put("error", "Gather failed: " + e.getMessage());
            return result;
        }
        if (gathered.paths().isEmpty()) {
            result.put("ok", false);
            result.put("error", "No .proto files found at the given ref/subdir/paths");
            return result;
        }
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(gathered);
        } catch (Exception e) {
            result.put("ok", false);
            result.put("error", "Gathered sources do not compile: " + e.getMessage());
            return result;
        }
        result.put("ok", true);
        ArrayNode files = result.putArray("files");
        ObjectNode sources = result.putObject("sources");
        for (ProtoSource source : gathered.sources()) {
            files.add(source.path());
            sources.put(source.path(), source.content());
        }
        result.put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray()));
        return result;
    }
}
