package ai.protomolt.proto.samples;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.cel.CelMappingRule;
import ai.protomolt.proto.cel.CelProtoMapper;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.List;

/** Demonstrates text mapping and CEL mapping against a protobuf Struct. */
public final class CelMappingSample {
    private CelMappingSample() {
    }

    public static void main(String[] args) throws Exception {
        Struct.Builder message = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("Ada").build())
                .putFields("enabled", Value.newBuilder().setBoolValue(true).build());
        ProtoFieldMapperImpl fieldMapper = new ProtoFieldMapperImpl(new DescriptorRegistry());
        fieldMapper.mapInPlace(message, List.of("copiedName = name"));

        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor())
                .addVar("input")
                .build());
        new CelProtoMapper(fieldMapper, evaluator).map(message, List.of(
                new CelMappingRule("input.fields['enabled'].bool_value", "input.fields['name'].string_value", "selectedName")));
        System.out.println(message.build());
    }
}
