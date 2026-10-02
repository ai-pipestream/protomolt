package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.grpc.adapter.GrpcActionService;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import io.grpc.ServerServiceDefinition;
import java.util.LinkedHashMap;
import java.util.Map;

/** Full platform service inventory over the reusable gRPC action adapter. */
public final class ProtoMoltGrpcService {
    private ProtoMoltGrpcService() { }
    public static ServerServiceDefinition definition(ActionCatalog catalog) {
        var service = ProtoMoltServiceSchema.service();
        Map<String, String> bindings = new LinkedHashMap<>();
        service.getMethods().forEach(method -> bindings.put(method.getName(), CatalogBridge.actionName(method)));
        return GrpcActionService.bind(catalog, service, bindings,
                GrpcActionService.MissingActionPolicy.EXPOSE_UNIMPLEMENTED);
    }
    public static ServerServiceDefinition contributed(ActionCatalog catalog, ServiceDescriptor service) {
        return contributed(catalog, service, Map.of());
    }
    public static ServerServiceDefinition contributed(ActionCatalog catalog, ServiceDescriptor service,
            Map<String, Map<String, String>> explicitBindings) {
        var mounted = ContractActionBindings.mounted(catalog, service, explicitBindings);
        if (mounted.size() != service.getMethods().size()) {
            throw new IllegalStateException("Cannot mount partial gRPC service " + service.getFullName());
        }
        Map<String, String> bindings = new LinkedHashMap<>();
        mounted.forEach((method, verb) -> bindings.put(method.getName(), verb));
        return GrpcActionService.bind(catalog, service, bindings);
    }
}
