package ai.pipestream.proto.grpc.service.contract;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The ProtoMolt service schema, compiled from its own {@code .proto} source at class load —
 * the service is defined in the format it manages and served without generated stubs.
 */
public final class ProtoMoltServiceSchema {

    /** Import path of the service definition on the classpath. */
    public static final String RESOURCE_PATH = "ai/pipestream/proto/grpc/service/v1/protomolt_service.proto";

    /**
     * Contracts the service definition imports, in the same import paths it names them by.
     *
     * <p>The service is compiled from source rather than bound to generated stubs, so every
     * first-party file it imports must be readable from the classpath as text. Modules owning
     * these contracts add their {@code src/main/proto} directory to the jar's resources for
     * exactly this reason. Well-known types are supplied by the compiler and are not listed
     * here.
     */
    private static final List<String> IMPORTED_RESOURCE_PATHS = List.of(
            "ai/pipestream/proto/grpc/profile/v1/service_profile.proto",
            "ai/pipestream/proto/validate/v1/validate.proto",
            "ai/pipestream/proto/grpc/workflow/v1/grpc_workflow.proto",
            "ai/pipestream/proto/inference/v1/inference.proto",
            "ai/pipestream/proto/inference/v1/structured.proto",
            "ai/pipestream/proto/meta/v1/metadata.proto",
            "ai/pipestream/proto/prompt/v1/prompt.proto");

    /** Fully qualified service name. */
    public static final String SERVICE_FULL_NAME = "ai.pipestream.proto.grpc.service.v1.ProtoMoltService";

    private static final class Holder {
        static final String SOURCE = readSource();
        static final Map<String, String> SOURCES = readSources(SOURCE);
        static final FileDescriptor FILE = compile(SOURCES);
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

    /** The raw {@code .proto} text of the service definition itself. */
    public static String protoSource() {
        return Holder.SOURCE;
    }

    /**
     * Every {@code .proto} source needed to compile the service definition, keyed by the import
     * path each file is named by.
     *
     * <p>The service definition imports first-party contracts, so its own text is not sufficient
     * to compile it. A caller handing these sources to a compiler, registering the service's
     * schema, or invoking the service reflectively needs the whole set rather than the entry
     * file alone. Well-known types are supplied by the compiler and are not included.
     */
    public static Map<String, String> protoSources() {
        return Holder.SOURCES;
    }

    private static String readSource() {
        return readResource(RESOURCE_PATH);
    }

    private static String readResource(String path) {
        try (InputStream in = ProtoMoltServiceSchema.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + path, e);
        }
    }

    private static Map<String, String> readSources(String source) {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put(RESOURCE_PATH, source);
        for (String imported : IMPORTED_RESOURCE_PATHS) {
            sources.put(imported, readResource(imported));
        }
        return Collections.unmodifiableMap(sources);
    }

    private static FileDescriptor compile(Map<String, String> sources) {
        try {
            ProtoSourceSet.Builder set = ProtoSourceSet.builder();
            sources.forEach((path, text) -> set.add(path, text, "protomolt-grpc-service"));
            CompiledProtos compiled = new ProtoSourceCompiler().compile(set.build());
            return compiled.descriptorFor(RESOURCE_PATH).orElseThrow(
                    () -> new IllegalStateException("Compiled set is missing " + RESOURCE_PATH));
        } catch (Exception e) {
            throw new IllegalStateException("The bundled service definition failed to compile", e);
        }
    }
}
