package ai.pipestream.proto.validate.conformance;

import ai.pipestream.proto.validate.ProtoValidator;
import build.buf.validate.ValidateProto;
import buf.validate.conformance.harness.Harness.TestConformanceRequest;
import buf.validate.conformance.harness.Harness.TestConformanceResponse;
import buf.validate.conformance.harness.Harness.TestResult;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;

import java.util.Map;

/**
 * Executor entry point for buf's {@code protovalidate-conformance} binary.
 *
 * <p>Reads a single {@link TestConformanceRequest} from stdin, validates every case, and writes a
 * {@link TestConformanceResponse} to stdout — the language-agnostic protocol the conformance runner
 * speaks. Invoke as:
 *
 * <pre>{@code
 * protovalidate-conformance --expected_failures=<file> \
 *     java -jar pipestream-proto-tools-protobuf-validation-conformance-all.jar
 * }</pre>
 *
 * <p>The request is parsed with a registry that knows the {@code buf.validate} option extensions, so
 * the rule annotations embedded in the request's {@code fdset} survive into the linked descriptors
 * and drive validation.
 */
public final class ConformanceMain {

    private ConformanceMain() {
    }

    public static void main(String[] args) throws Exception {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidateProto.registerAllExtensions(registry);

        TestConformanceRequest request = TestConformanceRequest.parseFrom(System.in, registry);
        TestConformanceResponse response = process(request, registry);
        response.writeTo(System.out);
        System.out.flush();
    }

    static TestConformanceResponse process(TestConformanceRequest request, ExtensionRegistry registry) {
        TestConformanceResponse.Builder response = TestConformanceResponse.newBuilder();
        Map<String, Descriptor> types;
        PredefinedRules predefined;
        try {
            DescriptorSets.Linked linked = DescriptorSets.link(request.getFdset());
            types = linked.types();
            predefined = PredefinedRules.from(linked.files(), registry);
        } catch (Exception e) {
            // Without descriptors nothing can run; report the failure per case so the runner sees it.
            String message = "failed to link fdset: " + e.getMessage();
            for (String name : request.getCasesMap().keySet()) {
                response.putResults(name, TestResult.newBuilder().setCompilationError(message).build());
            }
            return response.build();
        }

        ConformanceRunner runner = new ConformanceRunner(ProtoValidator.create(), predefined, true);
        for (Map.Entry<String, Any> entry : request.getCasesMap().entrySet()) {
            response.putResults(entry.getKey(), runCase(runner, types, entry.getValue(), registry));
        }
        return response.build();
    }

    private static TestResult runCase(
            ConformanceRunner runner, Map<String, Descriptor> types, Any any, ExtensionRegistry registry) {
        String typeName = typeName(any.getTypeUrl());
        Descriptor descriptor = types.get(typeName);
        if (descriptor == null) {
            return TestResult.newBuilder()
                    .setCompilationError("no descriptor for message type " + typeName)
                    .build();
        }
        try {
            DynamicMessage message = DynamicMessage.parseFrom(descriptor, any.getValue(), registry);
            return runner.run(message);
        } catch (Exception e) {
            return TestResult.newBuilder().setRuntimeError(String.valueOf(e.getMessage())).build();
        }
    }

    private static String typeName(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }
}
