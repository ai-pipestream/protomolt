package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.grpc.workspace.ReflectedServiceActions;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.util.Map;

/** Full-platform policy excludes restored remote proxies from local RPC binding. */
public final class ContractActionBindings {
    private ContractActionBindings() { }
    public static Map<MethodDescriptor, String> mounted(ActionCatalog catalog, ServiceDescriptor service) {
        return mounted(catalog, service, Map.of());
    }
    public static Map<MethodDescriptor, String> mounted(ActionCatalog catalog, ServiceDescriptor service,
            Map<String, Map<String, String>> explicitBindings) {
        return ai.protomolt.proto.grpc.adapter.ContractActionBindings.mounted(catalog, service,
                explicitBindings, action -> !ReflectedServiceActions.isReflected(action));
    }
}
