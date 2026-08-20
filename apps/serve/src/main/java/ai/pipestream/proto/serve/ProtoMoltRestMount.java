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
        register(registry, catalog, apiToken, null);
    }

    /**
     * Mounts with caller resolution: {@code callers} turns the request headers into the
     * caller each dispatch runs as, so a policy principal is scope-checked on REST exactly
     * as on gRPC. A missing scope is 403 with the named refusal; 401 keeps meaning "not
     * authenticated".
     */
    public static void register(ProtoRestMethodRegistry registry, ActionCatalog catalog,
                                ApiTokenRequirement apiToken,
                                java.util.function.Function<java.util.Map<String, String>,
                                        ai.pipestream.proto.actions.Caller> callers) {
        ServiceDescriptor service = ProtoMoltServiceSchema.service();
        for (MethodDescriptor method : service.getMethods()) {
            registry.register(service, method,
                    callers == null
                            ? request -> dispatch(catalog, method, request,
                                    ai.pipestream.proto.actions.Caller.operator())
                            : new ScopedInvoker(catalog, method, callers),
                    apiToken);
        }
    }

    /** A header-aware invoker; the plain path refuses rather than silently widening. */
    private record ScopedInvoker(ActionCatalog catalog, MethodDescriptor method,
                                 java.util.function.Function<java.util.Map<String, String>,
                                         ai.pipestream.proto.actions.Caller> callers)
            implements java.util.function.Function<com.google.protobuf.Message,
                    com.google.protobuf.Message>,
            ai.pipestream.proto.rest.ProtoRestContextInvoker {

        @Override
        public com.google.protobuf.Message apply(com.google.protobuf.Message request) {
            throw new ProtoRestInvocationException(
                    "caller resolution requires the header-aware invocation path");
        }

        @Override
        public com.google.protobuf.Message invoke(com.google.protobuf.Message request,
                java.util.Map<String, String> headers, java.util.Map<String, String> query) {
            ai.pipestream.proto.actions.Caller caller = callers.apply(headers);
            if (caller == null) {
                throw new ai.pipestream.proto.rest.UnauthorizedProtoRestException(
                        "Invalid API token");
            }
            return dispatch(catalog, method, request, caller);
        }
    }

    private static com.google.protobuf.Message dispatch(ActionCatalog catalog,
            MethodDescriptor method, com.google.protobuf.Message request,
            ai.pipestream.proto.actions.Caller caller) {
        try {
            return CatalogBridge.execute(catalog, method, request, caller);
        } catch (ActionException e) {
            String code = e.code().toLowerCase(Locale.ROOT);
            if ("internal-error".equals(code)) {
                throw new ProtoRestInvocationException(e.code() + ": " + e.getMessage(), e);
            }
            if ("permission-denied".equals(code)) {
                throw new ai.pipestream.proto.rest.ForbiddenProtoRestException(
                        e.code() + ": " + e.getMessage(), e);
            }
            // Client-repairable action failures map to 400 with the stable code.
            throw new MalformedRequestException(e.code() + ": " + e.getMessage(), e);
        }
    }
}
