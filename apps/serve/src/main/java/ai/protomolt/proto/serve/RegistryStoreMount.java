package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.schema.registry.git.RegistryWorkflowVersionRepository;
import ai.protomolt.proto.workflow.WorkflowRepository;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

/**
 * The registry family of the serve wiring: the git-backed schema registry store (with the
 * demo mode's unnamed temp-directory registry), the workflow repositories derived from it,
 * and the Confluent-protocol registry server that serves it. The store must exist before the
 * catalog is assembled so run-workflow resolves stored workflow names; the server starts
 * before HTTP so the console's same-origin proxy knows the port it bridges to.
 */
final class RegistryStoreMount {

    private RegistryStoreMount() {
    }

    /**
     * Opens the registry store at {@code registryGit}, or null when no registry is mounted.
     * Demo mode always has a registry: an unnamed one lives in a temp directory.
     */
    static GitSchemaRegistryStore openStore(Path registryGit, boolean demo) {
        if (registryGit == null && demo) {
            try {
                registryGit = java.nio.file.Files.createTempDirectory("protomolt-demo-registry");
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create the demo registry directory", e);
            }
        }
        return registryGit == null
                ? null
                : GitSchemaRegistryStore.builder().repositoryDir(registryGit).build();
    }

    /** The workflow repository reading stored workflow documents out of the registry store. */
    static WorkflowRepository workflowRepository(GitSchemaRegistryStore store) {
        return workflowRepository(store::workflow);
    }

    /**
     * The workflow repository over any name-to-document lookup. A stored document that is not
     * a valid JSON object fails the lookup loudly rather than serving partial input.
     */
    static WorkflowRepository workflowRepository(Function<String, Optional<String>> lookup) {
        com.fasterxml.jackson.databind.ObjectMapper json =
                new com.fasterxml.jackson.databind.ObjectMapper();
        return name -> lookup.apply(name).map(text -> {
            com.fasterxml.jackson.databind.JsonNode node;
            try {
                node = json.readTree(text);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not valid JSON", e);
            }
            if (!(node instanceof com.fasterxml.jackson.databind.node.ObjectNode workflow)) {
                throw new IllegalStateException(
                        "Stored workflow '" + name + "' is not a JSON object");
            }
            return workflow;
        });
    }

    /** The version repository tracking workflow documents through the registry store. */
    static WorkflowVersionRepository workflowVersions(GitSchemaRegistryStore store) {
        return new RegistryWorkflowVersionRepository(store);
    }

    /**
     * Builds the registry server for the store. The registry listener honors the same bind
     * address and shared secret as every other surface - one process, one security boundary.
     * The caller starts it and keeps the bound port.
     */
    static SchemaRegistryServer server(String host, int port, String apiToken,
                                       GitSchemaRegistryStore store, ActionCatalog catalog,
                                       CallerResolver callers) {
        return new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults()
                        .withHost(host)
                        .withPort(port)
                        .withApiToken(apiToken),
                store, catalog, callers);
    }
}
