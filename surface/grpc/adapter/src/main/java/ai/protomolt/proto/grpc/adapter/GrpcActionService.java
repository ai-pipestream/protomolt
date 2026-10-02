package ai.protomolt.proto.grpc.adapter;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import io.grpc.protobuf.ProtoUtils;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import io.grpc.stub.ServerCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Binds a supplied action catalog and protobuf service descriptor without platform assembly. */
public final class GrpcActionService {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcActionService.class);

    private GrpcActionService() {
    }

    /** Explicit compatibility policy for fixed service inventories. */
    public enum MissingActionPolicy { REJECT, EXPOSE_UNIMPLEMENTED }

    /** Bind a complete unary service to explicitly named catalog actions. */
    public static ServerServiceDefinition bind(ActionCatalog catalog, ServiceDescriptor service,
            Map<String, String> bindings) {
        return bind(catalog, service, bindings, MissingActionPolicy.REJECT);
    }

    /** Missing actions are allowed only when the caller explicitly exposes UNIMPLEMENTED RPCs. */
    public static ServerServiceDefinition bind(ActionCatalog catalog, ServiceDescriptor service,
            Map<String, String> bindings, MissingActionPolicy missingActionPolicy) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(service, "service");
        Objects.requireNonNull(bindings, "bindings");
        Objects.requireNonNull(missingActionPolicy, "missingActionPolicy");
        var names = service.getMethods().stream().map(MethodDescriptor::getName)
                .collect(java.util.stream.Collectors.toSet());
        if (!bindings.keySet().equals(names)) {
            throw new IllegalArgumentException("Bindings must cover exactly " + service.getFullName());
        }
        Map<MethodDescriptor, String> resolved = new LinkedHashMap<>();
        for (MethodDescriptor method : service.getMethods()) {
            if (method.isClientStreaming() || method.isServerStreaming()) {
                throw new IllegalArgumentException("Only unary catalog RPCs are supported: " + method.getFullName());
            }
            String verb = Objects.requireNonNull(bindings.get(method.getName()), "action name");
            if (verb.isBlank()) throw new IllegalArgumentException("Action name must not be blank");
            try {
                var action = catalog.get(verb);
                if (!action.requestType().getFullName().equals(method.getInputType().getFullName())
                        || !action.responseType().getFullName().equals(method.getOutputType().getFullName())) {
                    throw new IllegalArgumentException("Binding type differs for " + method.getFullName());
                }
            } catch (ActionException missing) {
                if (missingActionPolicy == MissingActionPolicy.REJECT) {
                    throw new IllegalArgumentException("Binding action is absent: " + verb, missing);
                }
            }
            resolved.put(method, verb);
        }
        FileDescriptor file = service.getFile();

        // The grpc descriptors must be the same instances in the service descriptor and the
        // bound methods, so build them once.
        Map<MethodDescriptor, io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage>> methods =
                new LinkedHashMap<>();
        for (MethodDescriptor method : service.getMethods()) {
            methods.put(method, io.grpc.MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName(service.getFullName(), method.getName()))
                    .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getInputType())))
                    .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getOutputType())))
                    .build());
        }

        io.grpc.ServiceDescriptor.Builder grpcService =
                io.grpc.ServiceDescriptor.newBuilder(service.getFullName())
                        .setSchemaDescriptor((ProtoFileDescriptorSupplier) () -> file);
        methods.values().forEach(grpcService::addMethod);

        ServerServiceDefinition.Builder definition =
                ServerServiceDefinition.builder(grpcService.build());
        methods.forEach((method, grpcMethod) -> definition.addMethod(grpcMethod,
                ServerCalls.asyncUnaryCall(handler(catalog, resolved.get(method), method))));
        return definition.build();
    }

    private static ServerCalls.UnaryMethod<DynamicMessage, DynamicMessage> handler(
            ActionCatalog catalog, String verb, MethodDescriptor method) {
        return (request, responseObserver) -> {
            try {
                DynamicMessage response = CatalogBridge.execute(catalog, verb, method, request,
                        CallerContexts.current());
                if (!CatalogContract.inspect(response).valid()) {
                    responseObserver.onError(Status.DATA_LOSS
                            .withDescription("invalid-upstream-response: " + method.getName())
                            .asRuntimeException());
                    return;
                }
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (ActionException e) {
                responseObserver.onError(CatalogBridge.toStatus(e));
            } catch (RuntimeException e) {
                LOG.error("{} failed", method.getFullName(), e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription("internal-error: " + method.getName() + " failed")
                        .asRuntimeException());
            }
        };
    }
}
