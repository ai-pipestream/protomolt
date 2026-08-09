package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;

import java.util.Objects;

/** Registers the four service-workspace verbs over one repository and channel policy. */
public final class ServiceWorkspaceActions {

    private ServiceWorkspaceActions() {
    }

    /** Registers service-register, service-list, service-inspect, and service-refresh. */
    public static ActionCatalog register(ActionCatalog catalog, ServiceProfileRepository repository) {
        return register(catalog, repository, ChannelFactory.standard());
    }

    /** Registers the workspace verbs with explicit channel construction for hosts and tests. */
    public static ActionCatalog register(ActionCatalog catalog, ServiceProfileRepository repository,
                                         ChannelFactory channels) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(channels, "channels");
        return catalog
                .register(new ServiceRegisterAction(repository, channels))
                .register(new ServiceListAction(repository))
                .register(new ServiceInspectAction(repository))
                .register(new ServiceRefreshAction(repository, channels));
    }
}
