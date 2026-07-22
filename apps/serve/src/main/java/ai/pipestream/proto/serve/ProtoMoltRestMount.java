package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.service.CatalogBridge;
import ai.pipestream.proto.grpc.service.ProtoMoltServiceSchema;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.MalformedRequestException;
import ai.pipestream.proto.rest.ProtoRestInvocationException;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.Locale;

/**
 * Registers every {@code ProtoMoltService} RPC as a JSON/REST method:
 * {@code POST /grpc-json/ProtoMoltService/{Method}} with the same envelopes as the gRPC
 * surface, described by the generated OpenAPI document.
 */
public final class ProtoMoltRestMount {

    private ProtoMoltRestMount() {
    }

    /** Mounts every verb over {@code catalog} into {@code registry}, no token. */
    public static void register(ProtoRestMethodRegistry registry, ActionCatalog catalog) {
        register(registry, catalog, null);
    }

    /**
     * Mounts every verb over {@code catalog} into {@code registry}. A non-null
     * {@code apiToken} requirement is attached to every method (enforced by the gateway's
     * validator and declared in the generated OpenAPI document).
     */
    public static void register(ProtoRestMethodRegistry registry, ActionCatalog catalog,
                                ApiTokenRequirement apiToken) {
        ServiceDescriptor service = ProtoMoltServiceSchema.service();
        for (MethodDescriptor method : service.getMethods()) {
            registry.register(service, method, request -> {
                try {
                    return CatalogBridge.execute(catalog, method, request);
                } catch (ActionException e) {
                    if ("internal-error".equals(e.code().toLowerCase(Locale.ROOT))) {
                        throw new ProtoRestInvocationException(
                                e.code() + ": " + e.getMessage(), e);
                    }
                    // Client-repairable action failures map to 400 with the stable code.
                    throw new MalformedRequestException(e.code() + ": " + e.getMessage(), e);
                }
            }, apiToken);
        }
    }
}
