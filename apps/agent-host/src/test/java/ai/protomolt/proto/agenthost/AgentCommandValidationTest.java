package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the validator-path rendering, whose segment walk replaced
 * {@code split("\\.")} with a non-regex loop.
 */
class AgentCommandValidationTest {

    @Test
    void rendersValidatorPathsInJsonSpelling() {
        assertThat(AgentCommandValidation.jsonPath("worker_id")).isEqualTo("workerId");
        assertThat(AgentCommandValidation.jsonPath("task.worker_id")).isEqualTo("task.workerId");
        assertThat(AgentCommandValidation.jsonPath("items[2].sub_field"))
                .isEqualTo("items[2].subField");
        assertThat(AgentCommandValidation.jsonPath("plain")).isEqualTo("plain");
        assertThat(AgentCommandValidation.jsonPath("")).isEqualTo("");
    }

    @Test
    void matchesTheReplacedSplitOnRepresentativePaths() {
        // The oracle renders through the regex split the production code no longer uses.
        for (String path : new String[] {"", "a", "a.b", "a.b.", "a..b", ".a", "x_y.z_w[0]",
                "items[1].deep.nested_name", "."}) {
            assertThat(AgentCommandValidation.jsonPath(path))
                    .as("path '%s'", path)
                    .isEqualTo(oracle(path));
        }
    }

    /** The original implementation, verbatim, as the comparison oracle. */
    private static String oracle(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : path.split("\\.")) {
            if (out.length() > 0) {
                out.append('.');
            }
            int bracket = segment.indexOf('[');
            String name = bracket < 0 ? segment : segment.substring(0, bracket);
            boolean capitalize = false;
            for (int i = 0; i < name.length(); i++) {
                char character = name.charAt(i);
                if (character == '_') {
                    capitalize = true;
                } else if (capitalize) {
                    out.append(Character.toUpperCase(character));
                    capitalize = false;
                } else {
                    out.append(character);
                }
            }
            if (bracket >= 0) {
                out.append(segment.substring(bracket));
            }
        }
        return out.toString();
    }

    @Test
    void deliverableTypeRegistriesAreMemoizedPerContract() {
        DeliverableContract contract = DeliverableFixture.contract();
        TypeRegistry first = AgentCommandValidation.deliverableTypes(contract);
        assertThat(first.find(DeliverableFixture.TYPE_NAME)).isNotNull();
        assertThat(AgentCommandValidation.deliverableTypes(contract)).isSameAs(first);
        // An equal but distinctly built contract names the same memoized registry.
        DeliverableContract rebuilt = DeliverableContract.newBuilder()
                .setDescriptorSet(contract.getDescriptorSet())
                .setTypeName(contract.getTypeName())
                .build();
        assertThat(rebuilt).isNotSameAs(contract).isEqualTo(contract);
        assertThat(AgentCommandValidation.deliverableTypes(rebuilt)).isSameAs(first);
        assertThat(AgentCommandValidation.deliverableTypes(null))
                .isSameAs(TypeRegistry.getEmptyTypeRegistry());
    }

    @Test
    void anUnlinkableContractMemoizesTheEmptyRegistryFallback() {
        DeliverableContract broken = DeliverableContract.newBuilder()
                .setDescriptorSet(ByteString.copyFromUtf8("not a descriptor set"))
                .setTypeName("missing.v1.Nothing")
                .build();
        TypeRegistry first = AgentCommandValidation.deliverableTypes(broken);
        assertThat(first.find("missing.v1.Nothing")).isNull();
        assertThat(AgentCommandValidation.deliverableTypes(broken)).isSameAs(first);
    }

    @Test
    void theDeliverableRegistryCacheEvictsTheEldestBeyondItsBudget() {
        // More distinct contracts than the budget: retention stays at the budget.
        // Each variant type name is a distinct contract key; the descriptor set does
        // not carry it, so every variant memoizes the empty-registry fallback.
        for (int i = 0; i < AgentCommandValidation.MAX_DELIVERABLE_REGISTRIES + 64; i++) {
            AgentCommandValidation.deliverableTypes(DeliverableContract.newBuilder()
                    .setDescriptorSet(DeliverableFixture.contract().getDescriptorSet())
                    .setTypeName(DeliverableFixture.TYPE_NAME + ".variant" + i)
                    .build());
        }
        assertThat(AgentCommandValidation.cachedDeliverableRegistries())
                .isEqualTo(AgentCommandValidation.MAX_DELIVERABLE_REGISTRIES);

        // Eviction rebuilds instead of corrupting: an evicted contract parses afresh,
        // and a fresh registry over a real contract still resolves its type.
        TypeRegistry rebuilt = AgentCommandValidation.deliverableTypes(DeliverableFixture.contract());
        assertThat(rebuilt.find(DeliverableFixture.TYPE_NAME)).isNotNull();
        assertThat(AgentCommandValidation.deliverableTypes(DeliverableFixture.contract()))
                .isSameAs(rebuilt);
    }

    @Test
    void theSharedParserStillRefusesUnknownMembersAndBadJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode unknown = mapper.createObjectNode().put("notAField", "x");
        assertThatThrownBy(() -> AgentCommandValidation.validateRequest(
                "delegation-cancel", unknown, 3, Map.of()))
                .isInstanceOf(ModelReplyException.class)
                .hasMessageContaining("command 3 delegation-cancel contains unknown field 'notAField'");
        ObjectNode mistyped = mapper.createObjectNode();
        mistyped.putObject("taskId");
        assertThatThrownBy(() -> AgentCommandValidation.validateRequest(
                "delegation-cancel", mistyped, 4, Map.of()))
                .isInstanceOf(ModelReplyException.class)
                .hasMessageContaining("command 4 delegation-cancel is not a CancelTaskRequest");
    }

    @Test
    void theSharedParserStillRendersValidatorViolationsVerbatim() {
        ObjectNode arguments = new ObjectMapper().createObjectNode().put("taskId", 42);
        assertThatThrownBy(() -> AgentCommandValidation.validateRequest(
                "delegation-cancel", arguments, 4, Map.of()))
                .isInstanceOf(ModelReplyException.class)
                .hasMessage("command 4 delegation-cancel: taskId value must be a valid UUID;"
                        + " reason field is required");
    }
}
