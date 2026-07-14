package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The ProtoMolt service schema, compiled from its own {@code .proto} source at class load —
 * the service is defined in the format it manages and served without generated stubs.
 */
public final class ProtoMoltServiceSchema {

    /** Import path of the service definition on the classpath. */
    public static final String RESOURCE_PATH = "ai/pipestream/protomolt/v1/protomolt_service.proto";

    /** Fully qualified service name. */
    public static final String SERVICE_FULL_NAME = "ai.pipestream.protomolt.v1.ProtoMoltService";

    private static final class Holder {
        static final String SOURCE = readSource();
        static final FileDescriptor FILE = compile(SOURCE);
    }

    private ProtoMoltServiceSchema() {
    }

    /** The compiled file descriptor. */
    public static FileDescriptor file() {
        return Holder.FILE;
    }

    /** The service descriptor. */
    public static ServiceDescriptor service() {
        return Holder.FILE.findServiceByName("ProtoMoltService");
    }

    /** The raw {@code .proto} text, e.g. for registering the service's own schema. */
    public static String protoSource() {
        return Holder.SOURCE;
    }

    private static String readSource() {
        try (InputStream in = ProtoMoltServiceSchema.class.getClassLoader()
                .getResourceAsStream(RESOURCE_PATH)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + RESOURCE_PATH);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + RESOURCE_PATH, e);
        }
    }

    private static FileDescriptor compile(String source) {
        try {
            CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                    .add(RESOURCE_PATH, source, "protomolt-grpc-service")
                    .build());
            return compiled.descriptorFor(RESOURCE_PATH).orElseThrow(
                    () -> new IllegalStateException("Compiled set is missing " + RESOURCE_PATH));
        } catch (Exception e) {
            throw new IllegalStateException("The bundled service definition failed to compile", e);
        }
    }
}
