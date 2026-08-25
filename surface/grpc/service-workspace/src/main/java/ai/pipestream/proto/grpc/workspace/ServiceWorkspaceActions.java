package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.registry.SchemaRegistryStore;

import java.util.Objects;

/** Registers the service-workspace verbs over one repository and channel policy. */
public final class ServiceWorkspaceActions {

    private ServiceWorkspaceActions() {
    }

    /** Registers profile registration, inspection, refresh, listing, and invocation. */
    public static ActionCatalog register(ActionCatalog catalog, ServiceProfileRepository repository) {
        return register(catalog, repository, null, ChannelFactory.standard());
    }

    /** Registers the workspace verbs with explicit channel construction for hosts and tests. */
    public static ActionCatalog register(ActionCatalog catalog, ServiceProfileRepository repository,
                                         ChannelFactory channels) {
        return register(catalog, repository, null, channels);
    }

    /** Registers workspace verbs backed by the host's schema registry. */
    public static ActionCatalog register(ActionCatalog catalog, ServiceProfileRepository repository,
                                         SchemaRegistryStore registry, ChannelFactory channels) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(channels, "channels");
        // A stored profile's methods become live verbs the moment register or
        // refresh lands, so the workbench never advertises a verb that is not there.
        ProfileStored reflected = profile -> ReflectedServiceActions.register(
                catalog, profile, repository, registry, channels);
        return catalog
                .register(new ServiceRegisterAction(repository, registry, channels, reflected))
                .register(new ServiceListAction(repository))
                .register(new ServiceInspectAction(repository, registry))
                .register(new ServiceRefreshAction(repository, registry, channels, reflected))
                .register(new ServiceInvokeAction(repository, registry, channels));
    }
}
