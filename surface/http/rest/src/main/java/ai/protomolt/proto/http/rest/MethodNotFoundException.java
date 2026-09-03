package ai.protomolt.proto.http.rest;

public final class MethodNotFoundException extends ProtoRestException {
    public MethodNotFoundException(String serviceName, String methodName) {
        super("Method not found: " + serviceName + "/" + methodName);
    }
}
