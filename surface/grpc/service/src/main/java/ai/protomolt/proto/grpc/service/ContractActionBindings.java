package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;

/** Matches contributed RPCs to mounted catalog verbs by their wire contracts. */
public final class ContractActionBindings {
    private ContractActionBindings() { }

    public static Map<MethodDescriptor, String> mounted(ActionCatalog catalog,
                                                        ServiceDescriptor service) {
        Map<String, String> byContract = new LinkedHashMap<>();
        for (String name : catalog.names()) {
            try {
                ProtoAction action = catalog.get(name);
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
