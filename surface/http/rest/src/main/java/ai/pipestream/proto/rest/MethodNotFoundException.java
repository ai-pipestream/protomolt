package ai.pipestream.proto.rest;

public class MethodNotFoundException extends ProtoRestException {
    public MethodNotFoundException(String serviceName, String methodName) {
        super("Method not found: " + serviceName + "/" + methodName);
    }
}
