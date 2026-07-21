package ai.pipestream.proto.sources;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoSourceCompilerTest {

    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    @Test
    void compilesSingleFile() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("person.proto", """
                        syntax = "proto3";
                        package example;
                        message Person {
                          string name = 1;
                          int32 age = 2;
                        }
                        """, "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);

        FileDescriptor file = compiled.descriptorFor("person.proto").orElseThrow();
        Descriptor person = file.findMessageTypeByName("Person");
        assertThat(person.getFullName()).isEqualTo("example.Person");
        assertThat(person.findFieldByName("age").getType()).isEqualTo(FieldDescriptor.Type.INT32);
    }

    @Test
    void compilesCrossFileImports() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("common/base.proto", """
                        syntax = "proto3";
                        package common;
                        message Id { string value = 1; }
                        """, "test")
                .add("app.proto", """
                        syntax = "proto3";
                        package app;
                        import "common/base.proto";
                        message Doc { common.Id id = 1; }
                        """, "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);

        Descriptor doc = compiled.descriptorFor("app.proto").orElseThrow()
                .findMessageTypeByName("Doc");
        assertThat(doc.findFieldByName("id").getMessageType().getFullName())
                .isEqualTo("common.Id");
    }

    @Test
    void resolvesWellKnownTypesIncludingFieldMask() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("uses_wkt.proto", """
                        syntax = "proto3";
                        package example;
                        import "google/protobuf/timestamp.proto";
                        import "google/protobuf/field_mask.proto";
                        message Audit {
                          google.protobuf.Timestamp at = 1;
                          google.protobuf.FieldMask mask = 2;
                        }
                        """, "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);

        Descriptor audit = compiled.descriptorFor("uses_wkt.proto").orElseThrow()
                .findMessageTypeByName("Audit");
        assertThat(audit.findFieldByName("at").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");
        assertThat(audit.findFieldByName("mask").getMessageType().getFullName())
                .isEqualTo("google.protobuf.FieldMask");
    }

    /**
     * The compiler stages its own {@code field_mask.proto} only when the set does not carry one.
     * A set that supplies its own must keep it verbatim: staging the built-in stub over the
     * caller's copy would silently drop fields the caller declared.
     */
    @Test
    void callerSuppliedFieldMaskIsUsedInsteadOfTheBuiltInStub() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("google/protobuf/field_mask.proto", """
                        syntax = "proto3";
                        package google.protobuf;
                        message FieldMask {
                          repeated string paths = 1;
                          string origin = 7;
                        }
                        """, "caller")
                .add("audit.proto", """
                        syntax = "proto3";
                        package example;
                        import "google/protobuf/field_mask.proto";
                        message Audit { google.protobuf.FieldMask mask = 1; }
                        """, "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);

        Descriptor fieldMask = compiled.descriptorFor("google/protobuf/field_mask.proto")
                .orElseThrow()
                .findMessageTypeByName("FieldMask");
        assertThat(fieldMask.getFields())
                .extracting(FieldDescriptor::getName)
                .containsExactly("paths", "origin");
        assertThat(fieldMask.findFieldByName("origin").getNumber()).isEqualTo(7);

        // The importer must link against that same file, not a second copy of the stub.
        Descriptor audit = compiled.descriptorFor("audit.proto").orElseThrow()
                .findMessageTypeByName("Audit");
        assertThat(audit.findFieldByName("mask").getMessageType()).isSameAs(fieldMask);
        assertThat(compiled.descriptorSet().getFileList())
                .extracting(f -> f.getName())
                .filteredOn("google/protobuf/field_mask.proto"::equals)
                .hasSize(1);
    }

    @Test
    void unresolvableImportFails() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("broken.proto", """
                        syntax = "proto3";
                        import "missing/nowhere.proto";
                        message M {}
                        """, "test")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class);
    }

    @Test
    void unparseableSourceFails() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("garbage.proto", "this is not proto", "test")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class);
    }

    @Test
    void unsafePathIsRejected() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("../escape.proto", "syntax = \"proto3\";", "evil")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class)
                .hasMessageContaining("Unsafe source path")
                .hasMessageContaining("evil");
    }

    @Test
    void emptySetCompilesToNothing() throws Exception {
        CompiledProtos compiled = compiler.compile(ProtoSourceSet.empty());
        assertThat(compiled.fileDescriptors()).isEmpty();
        assertThat(compiled.descriptorSet().getFileCount()).isZero();
    }

    @Test
    void descriptorSetContainsAllSourceFiles() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("one.proto", "syntax = \"proto3\"; package p1; message One {}", "test")
                .add("two.proto", "syntax = \"proto3\"; package p2; message Two {}", "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);

        assertThat(compiled.descriptorSet().getFileList())
                .extracting(f -> f.getName())
                .contains("one.proto", "two.proto");
        assertThat(compiled.descriptorFor("one.proto")).isPresent();
        assertThat(compiled.descriptorFor("two.proto")).isPresent();
    }
}
