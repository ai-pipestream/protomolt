package ai.protomolt.proto.http.rest;

public final class ServiceNotFoundException extends ProtoRestException {
    public ServiceNotFoundException(String serviceName) {
        super("Service not found: " + serviceName);
    }
}
