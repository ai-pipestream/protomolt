package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDirectoryServiceProto;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryServiceOuterClass;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceOuterClass;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchStatusServiceOuterClass;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceOuterClass;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The gRPC surface of the serve wiring: the services whose verbs this server wired beside
 * ProtoMolt's own, and the server start itself, including the launch-input message bound.
 */
final class GrpcMount {

    private GrpcMount() {
    }

    /**
     * The services whose verbs this server wired, beside ProtoMolt's own.
     *
     * <p>Listed by what was actually started rather than by what is on the classpath: the
     * mount publishes only methods the catalog can serve, and naming a service it did not
     * wire would rely on that filter rather than saying so here. Every surface (gRPC, REST,
     * OpenAPI) publishes this same list so a verb is never reachable on one surface and
     * missing from another.
     */
    static List<ServiceDescriptor> contributedServices(boolean meshCluster,
            boolean workflowAuthoring, boolean workflowPreparation) {
        List<ServiceDescriptor> services = new ArrayList<>();
        // Fully qualified: the generated holder and the registrar share a simple name.
        services.add(ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                .findServiceByName("DelegationService"));
        if (meshCluster) {
            services.add(ClusterDirectoryServiceProto.getDescriptor()
                    .findServiceByName("ClusterDirectoryService"));
        }
        if (workflowAuthoring) {
            services.add(GetAcceptedWorkflowRequest.getDescriptor().getFile()
                    .findServiceByName("WorkflowAuthoringService"));
            services.add(WorkflowLaunchInputServiceOuterClass
                    .getDescriptor().findServiceByName("WorkflowLaunchInputService"));
            services.add(WorkflowLaunchStatusServiceOuterClass
                    .getDescriptor().findServiceByName("WorkflowLaunchStatusService"));
            services.add(WorkflowAuthoringEntryServiceOuterClass
                    .getDescriptor().findServiceByName("WorkflowAuthoringEntryService"));
        }
        if (workflowPreparation) {
            services.add(WorkflowPreparationServiceOuterClass.getDescriptor()
                    .findServiceByName("WorkflowPreparationService"));
            services.add(WorkflowAuthorTaskMount.service());
        }
        return services;
    }

    /**
     * Starts the gRPC server with reflection over the catalog. Launch input permits 4 MiB of
     * JSON plus acceptance metadata; native contract validation enforces the individual field
     * bounds.
     */
    static ProtoMoltGrpcServer start(ProtoMoltServe.Options options, ActionCatalog catalog,
                                     CallerResolver callers, List<ServiceDescriptor> contributed,
                                     Map<String, Map<String, String>> authorTaskBindings,
                                     boolean workflowAuthoring) {
        return ProtoMoltGrpcServer.start(options.host(), options.grpcPort(), catalog,
                options.apiToken(), callers, contributed, authorTaskBindings,
                (workflowAuthoring ? 8 : 4) * 1024 * 1024);
    }
}
