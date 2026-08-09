package ai.pipestream.proto.registry.server;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SchemaRegistryServer#topologicallyOrdered}: the
 * descriptor-set endpoint serves dependencies before dependents so consumers
 * can link the set in one forward pass. Degenerate inputs (unknown
 * dependencies, cycles, duplicate names) must terminate and stay lossless.
 */
class TopologicalOrderTest {

    private static FileDescriptorProto file(String name, String... dependencies) {
        return FileDescriptorProto.newBuilder()
                .setName(name)
                .addAllDependency(List.of(dependencies))
                .build();
    }

    private static List<String> names(FileDescriptorSet set) {
        return set.getFileList().stream().map(FileDescriptorProto::getName).toList();
    }

    /** Every dependency present in the set appears before its dependent. */
    private static void assertTopologicallyOrdered(FileDescriptorSet set) {
        List<String> names = names(set);
        for (int i = 0; i < set.getFileCount(); i++) {
            for (String dependency : set.getFile(i).getDependencyList()) {
                if (names.contains(dependency)) {
                    assertThat(names.indexOf(dependency))
                            .as("dependency %s of %s appears earlier", dependency, names.get(i))
                            .isLessThan(i);
                }
            }
        }
    }

    @Test
    void anEmptySetStaysEmpty() {
        assertThat(SchemaRegistryServer.topologicallyOrdered(FileDescriptorSet.getDefaultInstance())
                .getFileCount()).isZero();
    }

    @Test
    void aLinearChainComesOutDependenciesFirst() {
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "b.proto"))
                .addFile(file("b.proto", "c.proto"))
                .addFile(file("c.proto"))
                .build();
        assertThat(names(SchemaRegistryServer.topologicallyOrdered(in)))
                .containsExactly("c.proto", "b.proto", "a.proto");
    }

    @Test
    void aDiamondOrdersEveryDependencyBeforeItsDependents() {
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "b.proto", "c.proto"))
                .addFile(file("b.proto", "d.proto"))
                .addFile(file("c.proto", "d.proto"))
                .addFile(file("d.proto"))
                .build();
        FileDescriptorSet out = SchemaRegistryServer.topologicallyOrdered(in);
        assertThat(names(out)).containsExactlyInAnyOrder("a.proto", "b.proto", "c.proto", "d.proto");
        assertTopologicallyOrdered(out);
    }

    @Test
    void independentFilesKeepTheirInputOrder() {
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("x.proto"))
                .addFile(file("y.proto"))
                .addFile(file("z.proto"))
                .build();
        assertThat(names(SchemaRegistryServer.topologicallyOrdered(in)))
                .containsExactly("x.proto", "y.proto", "z.proto");
    }

    @Test
    void aDependencyMissingFromTheSetIsTolerated() {
        // Well-known imports (google/protobuf/...) are not part of the served set.
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "google/protobuf/timestamp.proto"))
                .build();
        FileDescriptorSet out = SchemaRegistryServer.topologicallyOrdered(in);
        assertThat(names(out)).containsExactly("a.proto");
    }

    @Test
    void aCycleTerminatesAndLosesNoFile() {
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "b.proto"))
                .addFile(file("b.proto", "a.proto"))
                .build();
        FileDescriptorSet out = SchemaRegistryServer.topologicallyOrdered(in);
        assertThat(names(out)).containsExactlyInAnyOrder("a.proto", "b.proto");
    }

    @Test
    void duplicateFileNamesCollapseToOneEntry() {
        FileDescriptorSet in = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "b.proto"))
                .addFile(file("a.proto"))
                .build();
        FileDescriptorSet out = SchemaRegistryServer.topologicallyOrdered(in);
        assertThat(names(out)).containsExactly("a.proto");
    }
}
