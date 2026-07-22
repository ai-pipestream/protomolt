package ai.pipestream.proto.rest;

public class ServiceNotFoundException extends ProtoRestException {
    public ServiceNotFoundException(String serviceName) {
        super("Service not found: " + serviceName);
    }
}
