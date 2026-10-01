package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Matches contributed RPCs to mounted catalog verbs by their wire contracts. */
public final class ContractActionBindings {
    private ContractActionBindings() { }

    public static Map<MethodDescriptor, String> mounted(ActionCatalog catalog,
                                                        ServiceDescriptor service) {
        return mounted(catalog, service, Map.of());
    }

    /** Explicit service bindings are complete and checked against the declared RPC types. */
    public static Map<MethodDescriptor, String> mounted(ActionCatalog catalog,
            ServiceDescriptor service, Map<String, Map<String, String>> explicitBindings) {
        Map<String, String> explicit = explicitBindings.get(service.getFullName());
        if (explicit != null) {
            Set<String> methods = service.getMethods().stream().map(MethodDescriptor::getName)
                    .collect(Collectors.toSet());
            if (!explicit.keySet().equals(methods)) {
                throw new IllegalStateException("Incomplete explicit bindings for "
                        + service.getFullName());
            }
            Map<MethodDescriptor, String> mounted = new LinkedHashMap<>();
            for (MethodDescriptor method : service.getMethods()) {
                String verb = explicit.get(method.getName());
                try {
                    ProtoAction action = catalog.get(verb);
                    if (ai.protomolt.proto.grpc.workspace.ReflectedServiceActions.isReflected(action)) {
                        throw new IllegalStateException("Explicit binding names a reflected proxy for "
                                + service.getFullName() + "/" + method.getName());
                    }
                    if (!action.requestType().getFullName().equals(method.getInputType().getFullName())
                            || !action.responseType().getFullName().equals(method.getOutputType().getFullName())) {
                        throw new IllegalStateException("Explicit binding type differs for "
                                + service.getFullName() + "/" + method.getName());
                    }
                } catch (ActionException missing) {
                    throw new IllegalStateException("Explicit binding action is absent for "
                            + service.getFullName() + "/" + method.getName(), missing);
                }
                mounted.put(method, verb);
            }
            return Collections.unmodifiableMap(mounted);
        }
        Set<String> reserved = explicitBindings.values().stream()
                .flatMap(binding -> binding.values().stream()).collect(Collectors.toSet());
        Map<String, String> byContract = new LinkedHashMap<>();
        for (String name : catalog.names()) {
            if (reserved.contains(name)) continue;
            try {
                ProtoAction action = catalog.get(name);
                // A saved profile can describe this very server. Its proxy remains
                // available as a catalog verb, but cannot replace a local RPC or
                // make local binding ambiguous when the profile is restored.
                if (ai.protomolt.proto.grpc.workspace.ReflectedServiceActions.isReflected(action)) {
                    continue;
                }
                String key = contract(action.requestType().getFullName(),
                        action.responseType().getFullName());
                String previous = byContract.putIfAbsent(key, name);
                if (previous != null && !previous.equals(name)) {
                    throw new IllegalStateException("Ambiguous catalog contract for " + key);
                }
            } catch (ActionException e) {
                throw new IllegalStateException("Catalog lost registered action " + name, e);
            }
        }
        Map<MethodDescriptor, String> mounted = new LinkedHashMap<>();
        for (MethodDescriptor method : service.getMethods()) {
            String verb = byContract.get(contract(method.getInputType().getFullName(),
                    method.getOutputType().getFullName()));
            if (verb != null) mounted.put(method, verb);
        }
        return Collections.unmodifiableMap(mounted);
    }

    private static String contract(String request, String response) {
        return request + " -> " + response;
    }
}
