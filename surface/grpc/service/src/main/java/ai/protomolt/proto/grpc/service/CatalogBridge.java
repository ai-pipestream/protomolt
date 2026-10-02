package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageOrBuilder;

/** Compatibility facade for the reusable gRPC action bridge. */
public final class CatalogBridge {
    private CatalogBridge() { }

    /** The catalog action name for an RPC: {@code ListTypes} becomes {@code list-types}. */
    public static String actionName(MethodDescriptor method) {
        return ai.protomolt.proto.grpc.adapter.CatalogBridge.actionName(method);
    }

    /** Dispatches with process authority. */
    public static DynamicMessage execute(ActionCatalog catalog, MethodDescriptor method,
            MessageOrBuilder request) throws ActionException {
        return ai.protomolt.proto.grpc.adapter.CatalogBridge.execute(catalog, method, request);
    }

    /** Dispatches with the supplied caller's scopes. */
    public static DynamicMessage execute(ActionCatalog catalog, MethodDescriptor method,
            MessageOrBuilder request, Caller caller) throws ActionException {
        return ai.protomolt.proto.grpc.adapter.CatalogBridge.execute(catalog, method, request, caller);
    }

    /** Dispatches to an explicitly named catalog action. */
    public static DynamicMessage execute(ActionCatalog catalog, String verb, MethodDescriptor method,
            MessageOrBuilder request, Caller caller) throws ActionException {
        return ai.protomolt.proto.grpc.adapter.CatalogBridge.execute(catalog, verb, method, request, caller);
    }

    /** Converts the action failure into the established gRPC status and trailers. */
    public static io.grpc.StatusRuntimeException toStatus(ActionException failure) {
        return ai.protomolt.proto.grpc.adapter.CatalogBridge.toStatus(failure);
    }

    public static final io.grpc.Metadata.Key<String> ERROR_CODE_KEY =
            ai.protomolt.proto.grpc.adapter.CatalogBridge.ERROR_CODE_KEY;
    public static final io.grpc.Metadata.Key<byte[]> ERROR_DETAILS_KEY =
            ai.protomolt.proto.grpc.adapter.CatalogBridge.ERROR_DETAILS_KEY;
}
