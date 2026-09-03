package ai.protomolt.proto.sources;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Map-field coverage for {@link ProtoSourceCompiler}.
 *
 * <p>Maps are where descriptor round trips break: a {@code map<K, V>} field compiles to a
 * synthetic nested entry message ({@code option map_entry = true}) whose {@code value}
 * field must keep the declared type's full qualification. Apicurio's text-to-descriptor
 * path lost that qualification (its 3.3.1 emitter change made every map-bearing schema
 * unreadable), so every compiler that performs this desugaring gets explicit coverage:
 * scalar values, cross-package imported values, cross-file values, nested-message values,
 * and enum values, plus a wire-format compatibility check against protobuf-java's own
 * runtime descriptor.</p>
 */
class ProtoSourceCompilerMapTest {

    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    private static FieldDescriptor mapField(FileDescriptor file, String message, String field) {
        Descriptor descriptor = file.findMessageTypeByName(message);
        assertThat(descriptor).as("message %s", message).isNotNull();
        FieldDescriptor fd = descriptor.findFieldByName(field);
        assertThat(fd).as("field %s.%s", message, field).isNotNull();
        assertThat(fd.isMapField()).as("%s.%s is a map field", message, field).isTrue();
        assertThat(fd.getMessageType().getOptions().getMapEntry()).isTrue();
        return fd;
    }

    @Test
    void compilesScalarMap() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("counts.proto", """
                        syntax = "proto3";
                        package example;
                        message Counts {
                          map<string, int64> totals = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("counts.proto").orElseThrow();
        FieldDescriptor totals = mapField(file, "Counts", "totals");
        assertThat(totals.getMessageType().findFieldByName("key").getType())
                .isEqualTo(FieldDescriptor.Type.STRING);
        assertThat(totals.getMessageType().findFieldByName("value").getType())
                .isEqualTo(FieldDescriptor.Type.INT64);
    }

    @Test
    void compilesMapWithImportedWellKnownValueType() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("meta.proto", """
                        syntax = "proto3";
                        package example;
                        import "google/protobuf/struct.proto";
                        message Meta {
                          map<string, google.protobuf.Value> metadata = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("meta.proto").orElseThrow();
        FieldDescriptor metadata = mapField(file, "Meta", "metadata");
        assertThat(metadata.getMessageType().findFieldByName("value").getMessageType().getFullName())
                .as("cross-package map value type keeps its qualification")
                .isEqualTo("google.protobuf.Value");
    }

    @Test
    void compilesMapWithCrossFileValueType() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("common/item.proto", """
                        syntax = "proto3";
                        package common;
                        message Item { string id = 1; }
                        """, "test")
                .add("bundle.proto", """
                        syntax = "proto3";
                        package app;
                        import "common/item.proto";
                        message Bundle {
                          map<string, common.Item> items = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("bundle.proto").orElseThrow();
        FieldDescriptor items = mapField(file, "Bundle", "items");
        assertThat(items.getMessageType().findFieldByName("value").getMessageType().getFullName())
                .isEqualTo("common.Item");
    }

    @Test
    void compilesMapWithNestedMessageValueType() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("nested.proto", """
                        syntax = "proto3";
                        package example;
                        message Outer {
                          message Inner { string id = 1; }
                        }
                        message Root {
                          map<string, Outer.Inner> items = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("nested.proto").orElseThrow();
        FieldDescriptor items = mapField(file, "Root", "items");
        assertThat(items.getMessageType().findFieldByName("value").getMessageType().getFullName())
                .isEqualTo("example.Outer.Inner");
    }

    @Test
    void compilesMapWithEnumValueType() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("palette.proto", """
                        syntax = "proto3";
                        package example;
                        enum Color {
                          COLOR_UNSPECIFIED = 0;
                          COLOR_RED = 1;
                        }
                        message Palette {
                          map<string, Color> colors = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("palette.proto").orElseThrow();
        FieldDescriptor colors = mapField(file, "Palette", "colors");
        assertThat(colors.getMessageType().findFieldByName("value").getEnumType().getFullName())
                .isEqualTo("example.Color");
    }

    @Test
    void mapEntryIsWireCompatibleWithProtobufJava() throws Exception {
        // The compiled descriptor must accept bytes produced by protobuf-java's own
        // generated code for an identical schema: same entry field numbers, same wire
        // types. Struct is the canonical map-bearing well-known type, so compile its
        // twin and parse real Struct bytes into it.
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("mirror.proto", """
                        syntax = "proto3";
                        package example;
                        import "google/protobuf/struct.proto";
                        message Mirror {
                          map<string, google.protobuf.Value> fields = 1;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("mirror.proto").orElseThrow();
        Descriptor mirror = file.findMessageTypeByName("Mirror");

        byte[] bytes = com.google.protobuf.Struct.newBuilder()
                .putFields("k", com.google.protobuf.Value.newBuilder().setStringValue("v").build())
                .build().toByteArray();
        DynamicMessage parsed = DynamicMessage.parseFrom(mirror, bytes);

        FieldDescriptor fields = mirror.findFieldByName("fields");
        assertThat(parsed.getRepeatedFieldCount(fields)).isEqualTo(1);
        DynamicMessage entry = (DynamicMessage) parsed.getRepeatedField(fields, 0);
        assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("key")))
                .isEqualTo("k");
        DynamicMessage value = (DynamicMessage) entry.getField(
                entry.getDescriptorForType().findFieldByName("value"));
        assertThat(value.getField(value.getDescriptorForType().findFieldByName("string_value")))
                .isEqualTo("v");
    }
}
