package ai.protomolt.proto.workflow;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.cel.CelMappingRule;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scoped mapping shared by the live runner and offline replay: the contract CEL rule
 * conversion and the evaluator laziness replay relies on.
 */
class ScopedMappingTest {

    @Test
    void blankContractExpressionsBecomeAbsentAndTheRestIsCarried() {
        CelMappingRule rule = ScopedMapping.celRule(
                ai.protomolt.proto.grpc.workflow.v1.CelMappingRule.newBuilder()
                        .setFilter(" ")
                        .setSelector("")
                        .setTarget("name")
                        .addFallback("name = input.name")
                        .build());

        assertThat(rule.filterExpression()).isNull();
        assertThat(rule.selectorExpression()).isNull();
        assertThat(rule.targetPath()).isEqualTo("name");
        assertThat(rule.textRuleFallback()).containsExactly("name = input.name");
    }

    @Test
    void presentContractExpressionsAreKeptVerbatim() {
        CelMappingRule rule = ScopedMapping.celRule(
                ai.protomolt.proto.grpc.workflow.v1.CelMappingRule.newBuilder()
                        .setFilter("input.ok")
                        .setSelector("input.name")
                        .setTarget("name")
                        .build());

        assertThat(rule.filterExpression()).isEqualTo("input.ok");
        assertThat(rule.selectorExpression()).isEqualTo("input.name");
        assertThat(rule.textRuleFallback()).isEmpty();
    }

    @Test
    void textRulesAloneNeverBuildACelEnvironment() throws Exception {
        Map<String, Message> values = Map.of("input", Struct.getDefaultInstance());
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(Value.getDescriptor());

        ScopedMapping.apply(new ScopedProtoMapper(DescriptorRegistry.create()),
                ScopedMapping.scope(values), values, builder, List.of(), List.of(),
                () -> {
                    throw new AssertionError("evaluator consulted without CEL rules");
                });

        assertThat(builder.build()).isEqualTo(
                DynamicMessage.getDefaultInstance(Value.getDescriptor()));
    }
}
