package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapping on descriptor-native messages — the regression pins for two dynamic-only traps:
 * {@code DynamicMessage.Builder} has no {@code getFieldBuilder} (nested writes descend on
 * copies and fold back), and a runtime-compiled schema's Struct descriptor is a different
 * instance than the generated one (Struct rebuilds merge via bytes, not typed messages).
 */
class DynamicMessageStructTest {

    private static Descriptor order;

    @BeforeAll
    static void compile() throws Exception {
        // Compiled from text at runtime, exactly like registry- and inline-sourced schemas.
        var compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("probe/order.proto", """
                        syntax = "proto3";
                        package probe;
                        import "google/protobuf/struct.proto";
                        message Order {
                          string id = 1;
                          google.protobuf.Struct extras = 2;
                          Address ship_to = 3;
                        }
                        message Address {
                          string city = 1;
                        }
                        """, "probe").build());
        order = compiled.descriptorFor("probe/order.proto").orElseThrow()
                .findMessageTypeByName("Order");
        // The precondition that makes this suite meaningful: a distinct Struct descriptor.
        assertThat(order.findFieldByName("extras").getMessageType())
                .isNotSameAs(Struct.getDescriptor());
    }

    private static DynamicMessage.Builder order(String id) {
        return DynamicMessage.newBuilder(order).setField(order.findFieldByName("id"), id);
    }

    @Test
    void structKeysWriteAndReadBackOnDynamicMessages() throws Exception {
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var builder = order("o-1");
        mapper.mapInPlace(builder, List.of(
                "extras.warehouse = id",
                "extras.flags.rush = true",
                "id = extras.warehouse"));
        DynamicMessage built = builder.build();
        assertThat(mapper.getValue(built, "extras.warehouse")).isEqualTo("o-1");
        assertThat(mapper.getValue(built, "extras.flags.rush")).isEqualTo(true);
        assertThat(built.getField(order.findFieldByName("id"))).isEqualTo("o-1");
    }

    @Test
    void nestedMessageWritesFoldBackOnDynamicBuilders() throws Exception {
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var builder = order("o-2");
        mapper.setValue(builder, "ship_to.city", "Springfield");
        DynamicMessage built = builder.build();
        assertThat(mapper.getValue(built, "ship_to.city")).isEqualTo("Springfield");

        mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var cleared = built.toBuilder();
        mapper.clearField(cleared, "ship_to.city");
        assertThat(new ProtoFieldMapperImpl(DescriptorRegistry.create())
                .getValue(cleared.build(), "ship_to.city")).isNull();
    }

    @Test
    void wholeMessagesCopyAcrossSeparatelyCompiledSchemas() throws Exception {
        // The same file compiled twice yields wire-identical types under DIFFERENT
        // descriptor instances; a whole-message copy must align via bytes, not fail.
        var again = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("probe/order.proto", """
                        syntax = "proto3";
                        package probe;
                        import "google/protobuf/struct.proto";
                        message Order {
                          string id = 1;
                          google.protobuf.Struct extras = 2;
                          Address ship_to = 3;
                        }
                        message Address {
                          string city = 1;
                        }
                        """, "probe").build());
        Descriptor otherOrder = again.descriptorFor("probe/order.proto").orElseThrow()
                .findMessageTypeByName("Order");
        Descriptor otherAddress = again.descriptorFor("probe/order.proto").orElseThrow()
                .findMessageTypeByName("Address");
        assertThat(otherAddress).isNotSameAs(order.findFieldByName("ship_to").getMessageType());

        DynamicMessage foreignAddress = DynamicMessage.newBuilder(otherAddress)
                .setField(otherAddress.findFieldByName("city"), "Shelbyville").build();
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var builder = order("o-4");
        mapper.setValue(builder, "ship_to", foreignAddress);
        assertThat(mapper.getValue(builder.build(), "ship_to.city")).isEqualTo("Shelbyville");
    }

    @Test
    void scalarsWrapOntoValueTypedFields() throws Exception {
        var compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("probe/holder.proto", """
                        syntax = "proto3";
                        package probe;
                        import "google/protobuf/struct.proto";
                        message Holder {
                          google.protobuf.Value anything = 1;
                        }
                        """, "probe").build());
        Descriptor holder = compiled.descriptorFor("probe/holder.proto").orElseThrow()
                .findMessageTypeByName("Holder");
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var builder = DynamicMessage.newBuilder(holder);
        mapper.setValue(builder, "anything", "hello");
        DynamicMessage anything = (DynamicMessage) builder.build()
                .getField(holder.findFieldByName("anything"));
        assertThat(anything.getField(anything.getDescriptorForType()
                .findFieldByName("string_value"))).isEqualTo("hello");
    }

    @Test
    void structListsAppendOnDynamicMessages() throws Exception {
        ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        var builder = order("o-3");
        mapper.appendValue(builder, "extras.tags", "a");
        mapper.appendValue(builder, "extras.tags", "b");
        Object tags = mapper.getValue(builder.build(), "extras.tags");
        assertThat(tags).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .containsExactly("a", "b");
    }
}
