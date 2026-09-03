package ai.protomolt.proto.sources;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompiledProtosTest {

    private static CompiledProtos compileTiny() throws Exception {
        return new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("tiny.proto", """
                        syntax = "proto3";
                        package tiny;
                        message Tiny { string id = 1; }
                        """, "test")
                .build());
    }

    @Test
    void descriptorForLooksUpByImportPath() throws Exception {
        CompiledProtos compiled = compileTiny();
        assertThat(compiled.descriptorFor("tiny.proto")).isPresent();
        assertThat(compiled.descriptorFor("tiny.proto").orElseThrow().getName())
                .isEqualTo("tiny.proto");
        assertThat(compiled.descriptorFor("missing.proto")).isEmpty();
    }

    @Test
    void viewsAreDefensivelyCopied() throws Exception {
        CompiledProtos compiled = compileTiny();
        assertThatThrownBy(() -> compiled.fileDescriptors().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> compiled.byPath().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void canonicalConstructorCopiesMutableInputs() {
        FileDescriptorSet set = FileDescriptorSet.getDefaultInstance();
        java.util.ArrayList<com.google.protobuf.Descriptors.FileDescriptor> files =
                new java.util.ArrayList<>();
        java.util.HashMap<String, com.google.protobuf.Descriptors.FileDescriptor> byPath =
                new java.util.HashMap<>();
        CompiledProtos compiled = new CompiledProtos(set, files, byPath);
        files.add(null);
        byPath.put("late.proto", null);
        assertThat(compiled.fileDescriptors()).isEmpty();
        assertThat(compiled.byPath()).isEmpty();
        assertThat(compiled.descriptorSet()).isSameAs(set);
    }

    @Test
    void nullListAndMapAreRejected() {
        FileDescriptorSet set = FileDescriptorSet.getDefaultInstance();
        // List.copyOf / Map.copyOf reject nulls; the record must not accept them.
        assertThatThrownBy(() -> new CompiledProtos(set, null, Map.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CompiledProtos(set, List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
