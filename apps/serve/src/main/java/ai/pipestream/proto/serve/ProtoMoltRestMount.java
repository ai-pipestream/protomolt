package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.service.CatalogBridge;
import ai.pipestream.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.pipestream.proto.http.rest.ApiTokenRequirement;
import ai.pipestream.proto.http.rest.ForbiddenProtoRestException;
import ai.pipestream.proto.http.rest.MalformedRequestException;
import ai.pipestream.proto.http.rest.ProtoRestContextInvoker;
import ai.pipestream.proto.http.rest.ProtoRestInvocationException;
import ai.pipestream.proto.http.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.http.rest.UnauthorizedProtoRestException;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
                                Function<Map<String, String>, Caller> callers) {
        register(registry, catalog, apiToken, callers, List.of());
    }

    /**
     * Mounts the ProtoMolt service and every service in {@code contributed}.
     *
     * <p>A verb is not only declared on ProtoMolt's own service: the families that contribute
     * verbs declare them on services of their own, and until those are mounted the same verb
     * is reachable over gRPC and as an MCP tool but missing from REST and from the generated
     * OpenAPI document.
     *
     * <p>A method is mounted only when the catalog holds a verb of its name. The services here
     * describe what a build could serve; the catalog is what this process actually did wire,
     * and publishing a REST method for a verb that is not mounted would advertise a route that
     * answers unknown-action.
     */
    public static void register(ProtoRestMethodRegistry registry, ActionCatalog catalog,
                                ApiTokenRequirement apiToken,
                                Function<Map<String, String>, Caller> callers,
                                Collection<ServiceDescriptor> contributed) {
        Map<String, String> byContract = verbsByContract(catalog);
        List<ServiceDescriptor> services = new ArrayList<>();
        services.add(ProtoMoltServiceSchema.service());
        services.addAll(contributed);
        for (ServiceDescriptor service : services) {
            for (MethodDescriptor method : service.getMethods()) {
                String verb = byContract.get(contractOf(
                        method.getInputType().getFullName(),
                        method.getOutputType().getFullName()));
                if (verb == null) {
                    continue;
                }
                registry.register(service, method,
                        callers == null
                                ? request -> dispatch(catalog, verb, method, request,
                                        Caller.operator())
                                : new ScopedInvoker(catalog, verb, method, callers),
                        apiToken);
            }
        }
    }

    /**
     * The verb behind each request/response pair the catalog can serve.
     *
     * <p>Matching by contract rather than by name is what lets a contributed service be
     * mounted at all: its RPCs are named for the service and its verbs for the operator, so
     * {@code AcceptTask} and {@code delegation-accept} are the same thing under two names and
     * only the messages say so.
     *
     * <p>It also settles what to publish. The services describe what a build could serve; the
     * catalog is what this process wired. A method with no verb behind it is left off rather
     * than advertised as a route that answers unknown-action.
     */
    private static Map<String, String> verbsByContract(ActionCatalog catalog) {
        Map<String, String> byContract = new LinkedHashMap<>();
        for (String name : catalog.names()) {
            try {
                ProtoAction action = catalog.get(name);
                byContract.putIfAbsent(contractOf(action.requestType().getFullName(),
                        action.responseType().getFullName()), name);
            } catch (ActionException e) {
                // The catalog just named it, so it cannot be unknown; nothing to recover.
                throw new IllegalStateException(e);
            }
        }
        return byContract;
    }

    private static String contractOf(String request, String response) {
        return request + " -> " + response;
    }

    /** A header-aware invoker; the plain path refuses rather than silently widening. */
    private record ScopedInvoker(ActionCatalog catalog, String verb, MethodDescriptor method,
                                 Function<Map<String, String>, Caller> callers)
            implements Function<Message, Message>, ProtoRestContextInvoker {

        @Override
        public Message apply(Message request) {
            throw new ProtoRestInvocationException(
                    "caller resolution requires the header-aware invocation path");
        }

        @Override
        public Message invoke(Message request,
                Map<String, String> headers, Map<String, String> query) {
            Caller caller = callers.apply(headers);
            if (caller == null) {
                throw new UnauthorizedProtoRestException("Invalid API token");
            }
            return dispatch(catalog, verb, method, request, caller);
        }
    }

    private static Message dispatch(ActionCatalog catalog, String verb,
            MethodDescriptor method, Message request, Caller caller) {
        try {
            return CatalogBridge.execute(catalog, verb, method, request, caller);
        } catch (ActionException e) {
            String code = e.code().toLowerCase(Locale.ROOT);
            if ("internal-error".equals(code)) {
                throw new ProtoRestInvocationException(e.code() + ": " + e.getMessage(), e);
            }
            if ("permission-denied".equals(code)) {
                throw new ForbiddenProtoRestException(e.code() + ": " + e.getMessage(), e);
            }
            // Client-repairable action failures map to 400 with the stable code.
            throw new MalformedRequestException(e.code() + ": " + e.getMessage(), e);
        }
    }
}
