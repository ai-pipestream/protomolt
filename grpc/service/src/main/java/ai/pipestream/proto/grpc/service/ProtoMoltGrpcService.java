package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
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

/**
 * Binds an {@link ActionCatalog} as the {@code ai.pipestream.protomolt.v1.ProtoMoltService}
 * gRPC service: one unary handler per verb over {@link DynamicMessage}s, with the compiled
 * file descriptor attached so server reflection lists the service like any stub-generated one.
 */
public final class ProtoMoltGrpcService {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoMoltGrpcService.class);

    private ProtoMoltGrpcService() {
    }

    /** The service bound over {@code catalog}, ready for {@code ServerBuilder.addService}. */
    public static ServerServiceDefinition definition(ActionCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog");
        FileDescriptor file = ProtoMoltServiceSchema.file();
        ServiceDescriptor service = ProtoMoltServiceSchema.service();

        // The grpc descriptors must be the same instances in the service descriptor and the
        // bound methods, so build them once.
        Map<MethodDescriptor, io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage>> methods =
                new LinkedHashMap<>();
        for (MethodDescriptor method : service.getMethods()) {
            methods.put(method, DynamicGrpcCalls.methodDescriptor(method));
        }

        io.grpc.ServiceDescriptor.Builder grpcService =
                io.grpc.ServiceDescriptor.newBuilder(service.getFullName())
                        .setSchemaDescriptor((ProtoFileDescriptorSupplier) () -> file);
        methods.values().forEach(grpcService::addMethod);

        ServerServiceDefinition.Builder definition =
                ServerServiceDefinition.builder(grpcService.build());
        methods.forEach((method, grpcMethod) -> definition.addMethod(grpcMethod,
                ServerCalls.asyncUnaryCall(handler(catalog, method))));
        return definition.build();
    }

    private static ServerCalls.UnaryMethod<DynamicMessage, DynamicMessage> handler(
            ActionCatalog catalog, MethodDescriptor method) {
        return (request, responseObserver) -> {
            try {
                DynamicMessage response = CatalogBridge.execute(catalog, method, request);
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
