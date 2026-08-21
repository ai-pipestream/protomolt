package ai.pipestream.proto.authz.grpc;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.ScopeBudgets;
import ai.pipestream.proto.actions.Scopes;
import com.google.protobuf.MessageLite;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The per-service scope table for a hand-built gRPC surface (the serving roles): every mounted
 * service declares the scope its methods require, method entries override their service, and
 * openly-served plumbing (health, reflection) is named explicitly. A scoped caller reaching a
 * service the table does not name is refused by name — a newly mounted service is never
 * silently open. Runs behind {@link ApiTokenServerInterceptor}, reading the caller it
 * resolved; a call with no resolved caller (an open server) passes with process authority.
 */
public final class ScopeServerInterceptor implements ServerInterceptor {

    private final Map<String, String> serviceScopes;
    private final Map<String, String> methodScopes;
    private final Set<String> openServices;
    private final ScopeBudgets budgets = new ScopeBudgets();

    /**
     * @param serviceScopes required scope by full service name
     * @param methodScopes overrides by full method name ({@code pkg.Service/Method})
     * @param openServices full service names served under authentication alone
     */
    public ScopeServerInterceptor(Map<String, String> serviceScopes,
                                  Map<String, String> methodScopes,
                                  Set<String> openServices) {
        for (String scope : serviceScopes.values()) {
            requireKnown(scope);
        }
        for (String scope : methodScopes.values()) {
            requireKnown(scope);
        }
        this.serviceScopes = Map.copyOf(serviceScopes);
        this.methodScopes = Map.copyOf(methodScopes);
        this.openServices = Set.copyOf(openServices);
    }

    private static void requireKnown(String scope) {
        if (!Scopes.VOCABULARY.contains(scope)) {
            throw new IllegalArgumentException("unknown scope '" + scope
                    + "'; the vocabulary is " + String.join(", ", Scopes.VOCABULARY));
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Caller caller = CallerContexts.current();
        if (caller.unrestricted()) {
            return next.startCall(call, headers);
        }
        String method = call.getMethodDescriptor().getFullMethodName();
        String service = call.getMethodDescriptor().getServiceName();
        if (openServices.contains(service)) {
            return next.startCall(call, headers);
        }
        String scope = methodScopes.get(method);
        if (scope == null) {
            scope = serviceScopes.get(service);
        }
        if (scope == null) {
            call.close(Status.PERMISSION_DENIED.withDescription("no scope is declared for "
                    + Objects.toString(service, method)
                    + "; a scoped caller cannot call it"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        if (!caller.holds(scope)) {
            call.close(Status.PERMISSION_DENIED.withDescription("caller '" + caller.name()
                    + "' does not hold '" + scope + "', which " + method + " requires"),
                    new Metadata());
            return new ServerCall.Listener<>() { };
        }
        // The caller's budget spends once per request. Without a payload cap that is
        // decided here; with one, the whole consult waits for the message so payload
        // and rate are one spend.
        Caller.Budget budget = caller.budgets().get(scope);
        if (budget == null) {
            return next.startCall(call, headers);
        }
        String budgetScope = scope;
        if (budget.maxPayloadBytes() == 0) {
            Optional<String> refusal = budgets.refuse(caller, budgetScope, -1);
            if (refusal.isPresent()) {
                call.close(Status.RESOURCE_EXHAUSTED.withDescription(refusal.get()),
                        new Metadata());
                return new ServerCall.Listener<>() { };
            }
            return next.startCall(call, headers);
        }
        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                delegate) {
            private boolean refused;

            @Override
            public void onMessage(ReqT message) {
                long size = message instanceof MessageLite proto
                        ? proto.getSerializedSize() : -1;
                Optional<String> refusal = budgets.refuse(caller, budgetScope, size);
                if (refusal.isPresent()) {
                    refused = true;
                    call.close(Status.RESOURCE_EXHAUSTED
                            .withDescription(refusal.get()), new Metadata());
                    return;
                }
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!refused) {
                    super.onHalfClose();
                }
            }

            @Override
            public void onCancel() {
                if (!refused) {
                    super.onCancel();
                }
            }
        };
    }
}
