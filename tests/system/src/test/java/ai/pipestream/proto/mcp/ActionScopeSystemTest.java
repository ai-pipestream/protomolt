package ai.pipestream.proto.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The route-to-scope table of record for the shipped catalogs, and the matrix the
 * authorization-scopes chapter promises: every action refuses a caller holding everything
 * except its scope, and dispatches for a caller holding exactly that scope.
 */
class ActionScopeSystemTest {

    /** The full catalog's scope assignment; changing a scope is a deliberate edit here. */
    private static final Map<String, List<String>> FULL_ASSIGNMENT = Map.of(
            Scopes.SCHEMA_READ, List.of(
                    "check-compat", "check-rules", "compile", "compile-workflow",
                    "diff-schemas", "eval-cel", "extract-metadata", "gather-git",
                    "generate-stubs", "infer-schema", "join-messages", "list-types",
                    "map-message", "mask-message", "merge-schemas", "render-index-mappings",
                    "render-json-schema", "render-prompt", "suggest-mappings",
                    "synthesize-shape", "validate-message"),
            Scopes.SERVICE_INVOKE, List.of(
                    "complete-step", "get-job", "grpc-invoke", "inference-describe-model",
                    "inference-generate", "inference-list-models", "list-jobs", "reflect",
                    "service-inspect", "service-invoke", "service-list", "service-refresh",
                    "service-register"),
            Scopes.WORKFLOW_RUN, List.of(
                    "check-workflow", "evaluate-work-record", "export-work-record",
                    "promote-workflow", "record-workflow-run", "replay-workflow",
                    "run-workflow", "submit-workflow", "verify-work-record"),
            Scopes.ARTIFACT_ACCESS, List.of("emit-okf"));

    @Test
    void theFullCatalogScopeTableIsExactlyTheOneOfRecord() throws Exception {
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        Map<String, Set<String>> actual = new TreeMap<>();
        for (String name : catalog.names()) {
            actual.computeIfAbsent(catalog.get(name).requiredScope(), s -> new HashSet<>())
                    .add(name);
        }
        Map<String, Set<String>> expected = new TreeMap<>();
        FULL_ASSIGNMENT.forEach((scope, names) -> expected.put(scope, Set.copyOf(names)));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void everyShippedActionDeclaresAScopeFromTheVocabulary() throws Exception {
        ActionContext context = ActionContext.create();
        Map<String, ActionCatalog> catalogs = new LinkedHashMap<>();
        catalogs.put("defaults", ActionCatalog.defaults(context));
        catalogs.put("standaloneMcp", McpMain.catalog(context));
        catalogs.put("full", ProtoMoltCatalog.full(context));
        for (Map.Entry<String, ActionCatalog> entry : catalogs.entrySet()) {
            for (String name : entry.getValue().names()) {
                String scope = entry.getValue().get(name).requiredScope();
                assertThat(Scopes.VOCABULARY)
                        .as(entry.getKey() + " action " + name)
                        .contains(scope);
            }
        }
    }

    @Test
    void everyActionRefusesWithoutItsScopeAndDispatchesWithIt() throws Exception {
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        for (String name : catalog.names()) {
            String required = catalog.get(name).requiredScope();

            Set<String> allBut = new HashSet<>(Scopes.VOCABULARY);
            allBut.remove(required);
            ActionException denied = catchThrowableOfType(ActionException.class, () ->
                    catalog.execute(name, JsonNodeFactory.instance.objectNode(),
                            Caller.scoped("probe", allBut)));
            assertThat(denied.code()).as(name + " without " + required)
                    .isEqualTo("permission-denied");
            assertThat(denied.getMessage()).as(name).contains(required).contains("probe");

            try {
                catalog.execute(name, JsonNodeFactory.instance.objectNode(),
                        Caller.scoped("probe", Set.of(required)));
            } catch (ActionException dispatched) {
                assertThat(dispatched.code()).as(name + " holding " + required)
                        .isNotEqualTo("permission-denied");
            }
        }
    }

    @Test
    void aScopedListingServesExactlyTheCallersTable() throws Exception {
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        for (Map.Entry<String, List<String>> entry : FULL_ASSIGNMENT.entrySet()) {
            Set<String> listed = new HashSet<>();
            catalog.list(Caller.scoped("probe", Set.of(entry.getKey())))
                    .forEach(node -> listed.add(node.path("name").asText()));
            assertThat(listed).as(entry.getKey())
                    .isEqualTo(Set.copyOf(entry.getValue()));
        }
    }
}
