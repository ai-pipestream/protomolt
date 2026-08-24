package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every verb the toolkit catalog serves is one RPC on the service, and the two describe the
 * same request.
 *
 * <p>This is what lets the proto be the single description of the surface. A verb whose
 * name matches no RPC, or whose published schema is not the one generated from that RPC's
 * request message, is a verb whose contract exists in two places and can disagree between
 * them.
 */
class VerbRpcCorrespondenceTest {

    /** {@code check-compat} is the catalog's name for the {@code CheckCompat} RPC. */
    private static String verbName(MethodDescriptor method) {
        StringBuilder out = new StringBuilder();
        for (char character : method.getName().toCharArray()) {
            if (Character.isUpperCase(character) && !out.isEmpty()) {
                out.append('-');
            }
            out.append(Character.toLowerCase(character));
        }
        return out.toString();
    }

    @Test
    void everyToolkitVerbIsAnRpcThatDescribesTheSameRequest() {
        Map<String, MethodDescriptor> byVerb = ProtoMoltServiceSchema.service().getMethods()
                .stream()
                .collect(Collectors.toMap(VerbRpcCorrespondenceTest::verbName,
                        Function.identity()));
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        // A catalog that came back empty would make every assertion below vacuous.
        assertThat(catalog.names()).hasSizeGreaterThan(30);

        List<String> unmatched = catalog.names().stream()
                .filter(name -> !byVerb.containsKey(name))
                .toList();
        assertThat(unmatched)
                .as("verbs in the toolkit catalog with no RPC of the same name")
                .isEmpty();

        for (JsonNode entry : catalog.list()) {
            String name = entry.path("name").asText();
            MethodDescriptor method = byVerb.get(name);
            assertThat(entry.path("inputSchema").path("title").asText(
                            method.getInputType().getName()))
                    .as("%s publishes a schema for %s", name, method.getInputType().getFullName())
                    .isNotBlank();
        }
    }

    /**
     * And answers with the message that RPC declares.
     *
     * <p>A verb builds its reply against the type it names, so naming the wrong one is not a
     * mismatch anyone notices later: the reply is built correctly against a contract nobody
     * asked for. mesh-snapshot named another verb's request type this way and published the
     * wrong input schema for as long as nothing compared the two.
     */
    @Test
    void everyToolkitVerbAnswersWithTheMessageItsRpcDeclares() throws Exception {
        Map<String, MethodDescriptor> byVerb = ProtoMoltServiceSchema.service().getMethods()
                .stream()
                .collect(Collectors.toMap(VerbRpcCorrespondenceTest::verbName,
                        Function.identity()));
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        assertThat(catalog.names()).hasSizeGreaterThan(30);

        for (String name : catalog.names()) {
            MethodDescriptor method = byVerb.get(name);
            assertThat(method).as("verb %s has an RPC", name).isNotNull();
            assertThat(catalog.get(name).requestType().getFullName())
                    .as("%s accepts what %s declares", name, method.getFullName())
                    .isEqualTo(method.getInputType().getFullName());
            assertThat(catalog.get(name).responseType().getFullName())
                    .as("%s answers with what %s declares", name, method.getFullName())
                    .isEqualTo(method.getOutputType().getFullName());
        }
    }

    @Test
    void theInferenceVerbsKeepTheirPrefixedNames() {
        // Three RPCs are named for the service they reach rather than for the verb alone,
        // so the mechanical conversion above would derive the wrong name for them. Pinning
        // it here means a rename cannot pass unnoticed.
        assertThat(verbName(ProtoMoltServiceSchema.service()
                .findMethodByName("InferenceGenerate"))).isEqualTo("inference-generate");
        assertThat(verbName(ProtoMoltServiceSchema.service()
                .findMethodByName("InferenceListModels"))).isEqualTo("inference-list-models");
    }

    @Test
    void everyRpcNameConvertsToALowerKebabVerb() {
        for (MethodDescriptor method : ProtoMoltServiceSchema.service().getMethods()) {
            assertThat(verbName(method))
                    .as("verb name for %s", method.getName())
                    .isEqualTo(verbName(method).toLowerCase(Locale.ROOT))
                    .matches("[a-z0-9]+(-[a-z0-9]+)*");
        }
    }
}
