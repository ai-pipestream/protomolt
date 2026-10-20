package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.mcp.CompositeResources;
import ai.protomolt.proto.mcp.DelegationResources;
import ai.protomolt.proto.mcp.McpServer;
import ai.protomolt.proto.mcp.RegistryResources;
import ai.protomolt.proto.mcp.ServiceProfileResources;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;

/**
 * The MCP surface of the serve wiring: the catalog as MCP tools over streamable HTTP, with the
 * registry and service-profile resources composed in when those stores are mounted and the
 * delegation resources always present.
 */
final class McpMount {

    private McpMount() {
    }

    /** Builds the MCP server over the catalog and returns its HTTP handler. */
    static McpHttpHandler mount(ActionCatalog catalog, GitSchemaRegistryStore store,
                                ServiceProfileRepository serviceProfiles, DelegationBridge bridge,
                                String apiToken, CallerResolver callers, String version) {
        McpServer mcp = new McpServer(catalog,
                CompositeResources.of(
                        store != null ? new RegistryResources(store) : null,
                        serviceProfiles != null
                                ? new ServiceProfileResources(serviceProfiles, store) : null,
                        new DelegationResources(bridge)),
                "protomolt", version);
        return new McpHttpHandler(mcp, apiToken, callers);
    }
}
