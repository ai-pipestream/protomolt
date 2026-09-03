package ai.protomolt.proto.acquire.gather.git;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSource;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
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
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Gathers .proto sources from a git repository (branch, tag, or commit) and "
                + "compiles them: returns the source texts keyed by import path plus a base64 "
                + "descriptor set usable as the 'schema' input to the other verbs. The lane for "
                + "services that publish their contract in git rather than enabling reflection.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("GatherGitRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("GatherGitResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context)
            throws ActionException {
        // repo is required by the request message, so a blank one is refused before here.
        GitProtoGatherer.Builder builder = GitProtoGatherer.builder()
                .repo(Fields.string(input, "repo"));
        if (cacheRoot != null) {
            builder.cacheRoot(cacheRoot);
        }
        // An omitted ref, subdir or path list arrives as its zero, which is what "leave the
        // gatherer's own default in place" already meant.
        String ref = Fields.string(input, "ref");
        if (!ref.isBlank()) {
            builder.ref(ref);
        }
        String subdir = Fields.string(input, "subdir");
        if (!subdir.isBlank()) {
            builder.subdir(subdir);
        }
        List<String> paths = Fields.strings(input, "paths");
        if (!paths.isEmpty()) {
            builder.paths(paths);
        }

        ProtoSourceSet gathered;
        try {
            gathered = builder.build().gather();
        } catch (Exception e) {
            return refusal("Gather failed: " + e.getMessage());
        }
        if (gathered.paths().isEmpty()) {
            return refusal("No .proto files found at the given ref/subdir/paths");
        }
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(gathered);
        } catch (Exception e) {
            return refusal("Gathered sources do not compile: " + e.getMessage());
        }
        Reply result = Reply.of(responseType()).set("ok", true);
        for (ProtoSource source : gathered.sources()) {
            result.add("files", source.path());
            result.append("sources")
                    .set("key", source.path())
                    .set("value", source.content())
                    .build();
        }
        return result.set("descriptorSetBase64",
                Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray()))
                .build();
    }

    /** A gather that could not complete, reported as a result rather than as an error. */
    private Message refusal(String error) {
        return Reply.of(responseType()).set("ok", false).set("error", error).build();
    }
}
