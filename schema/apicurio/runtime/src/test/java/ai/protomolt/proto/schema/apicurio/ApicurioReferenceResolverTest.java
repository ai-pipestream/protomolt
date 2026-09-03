package ai.protomolt.proto.schema.apicurio;

import ai.protomolt.proto.schema.apicurio.ApicurioReferenceResolver.ArtifactSource;
import ai.protomolt.proto.schema.apicurio.ApicurioReferenceResolver.Reference;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ApicurioReferenceResolver} using an in-memory {@link ArtifactSource}
 * fake (no live registry required) and the real Apicurio {@link ProtobufSchemaParser}.
 */
class ApicurioReferenceResolverTest {

    private static final String LATEST = ApicurioReferenceResolver.LATEST_VERSION_EXPRESSION;
    private static final String GROUP = "g";

    /** In-memory registry: content and references keyed by artifactId (version-agnostic). */
    private static final class FakeSource implements ArtifactSource {
        final Map<String, String> contents = new HashMap<>();
        final Map<String, List<Reference>> references = new HashMap<>();
        final List<String> contentFetches = new ArrayList<>();

        void artifact(String artifactId, String proto, Reference... refs) {
            contents.put(artifactId, proto);
            references.put(artifactId, List.of(refs));
        }

        @Override
        public byte[] content(String groupId, String artifactId, String versionExpression) {
            contentFetches.add(artifactId);
            String proto = contents.get(artifactId);
            return proto == null ? null : proto.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public List<Reference> references(String groupId, String artifactId, String versionExpression) {
            return references.getOrDefault(artifactId, List.of());
        }
    }

    private static Reference ref(String importPath) {
        return new Reference(importPath, GROUP, importPath, "1");
    }

    private static ApicurioReferenceResolver resolver(FakeSource source) {
        return new ApicurioReferenceResolver(source, new ProtobufSchemaParser<>());
    }

    @Test
    void parsesArtifactWithoutReferences() throws Exception {
        FakeSource source = new FakeSource();
        source.artifact("plain.proto", """
                syntax = "proto3";
                package unit.v1;
                message Plain { string id = 1; }
                """);

        ProtobufSchema schema = resolver(source).resolveAndParse(GROUP, "plain.proto", LATEST);
        assertThat(schema.getFileDescriptor().findMessageTypeByName("Plain")).isNotNull();
    }

    @Test
    void resolvesDirectReference() throws Exception {
        FakeSource source = new FakeSource();
        source.artifact("common.proto", """
                syntax = "proto3";
                package unit.v1;
                message Address { string city = 1; }
                """);
        source.artifact("employee.proto", """
                syntax = "proto3";
                package unit.v1;
                import "common.proto";
                message Employee { unit.v1.Address address = 1; }
                """, ref("common.proto"));

        ProtobufSchema schema = resolver(source).resolveAndParse(GROUP, "employee.proto", LATEST);

        FileDescriptor fd = schema.getFileDescriptor();
        Descriptor employee = fd.findMessageTypeByName("Employee");
        assertThat(employee).isNotNull();
        FieldDescriptor address = employee.findFieldByName("address");
        assertThat(address.getType()).isEqualTo(FieldDescriptor.Type.MESSAGE);
        assertThat(address.getMessageType().getFullName()).isEqualTo("unit.v1.Address");
        assertThat(fd.getDependencies())
                .extracting(FileDescriptor::getName)
                .contains("common.proto");
    }

    @Test
    void resolvesTransitiveReferences() throws Exception {
        FakeSource source = new FakeSource();
        source.artifact("c.proto", """
                syntax = "proto3";
                package unit.v1;
                message C { string id = 1; }
                """);
        source.artifact("b.proto", """
                syntax = "proto3";
                package unit.v1;
                import "c.proto";
                message B { unit.v1.C c = 1; }
                """, ref("c.proto"));
        source.artifact("a.proto", """
                syntax = "proto3";
                package unit.v1;
                import "b.proto";
                message A { unit.v1.B b = 1; }
                """, ref("b.proto"));

        ProtobufSchema schema = resolver(source).resolveAndParse(GROUP, "a.proto", LATEST);

        Descriptor a = schema.getFileDescriptor().findMessageTypeByName("A");
        Descriptor b = a.findFieldByName("b").getMessageType();
        assertThat(b.getFullName()).isEqualTo("unit.v1.B");
        assertThat(b.findFieldByName("c").getMessageType().getFullName()).isEqualTo("unit.v1.C");
    }

    @Test
    void resolvesDiamondReferencesFetchingSharedDependencyOnce() throws Exception {
        FakeSource source = new FakeSource();
        source.artifact("base.proto", """
                syntax = "proto3";
                package unit.v1;
                message Base { string id = 1; }
                """);
        source.artifact("left.proto", """
                syntax = "proto3";
                package unit.v1;
                import "base.proto";
                message Left { unit.v1.Base base = 1; }
                """, ref("base.proto"));
        source.artifact("right.proto", """
                syntax = "proto3";
                package unit.v1;
                import "base.proto";
                message Right { unit.v1.Base base = 1; }
                """, ref("base.proto"));
        source.artifact("top.proto", """
                syntax = "proto3";
                package unit.v1;
                import "left.proto";
                import "right.proto";
                message Top {
                  unit.v1.Left left = 1;
                  unit.v1.Right right = 2;
                }
                """, ref("left.proto"), ref("right.proto"));

        ProtobufSchema schema = resolver(source).resolveAndParse(GROUP, "top.proto", LATEST);

        Descriptor top = schema.getFileDescriptor().findMessageTypeByName("Top");
        assertThat(top.findFieldByName("left").getMessageType().getFullName()).isEqualTo("unit.v1.Left");
        assertThat(top.findFieldByName("right").getMessageType().getFullName()).isEqualTo("unit.v1.Right");
        // Shared dependency is memoized: fetched exactly once despite two inbound edges.
        assertThat(source.contentFetches.stream().filter("base.proto"::equals)).hasSize(1);
    }

    @Test
    void missingReferenceFailsNamingTheUnresolvedImport() {
        FakeSource source = new FakeSource();
        source.artifact("dangling.proto", """
                syntax = "proto3";
                package unit.v1;
                import "missing.proto";
                message Dangling { unit.v1.Missing missing = 1; }
                """, ref("missing.proto"));

        assertThatThrownBy(() -> resolver(source).resolveAndParse(GROUP, "dangling.proto", LATEST))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unresolved reference 'missing.proto'");
    }

    @Test
    void cyclicReferencesAreDetectedNotInfinitelyRecursed() {
        FakeSource source = new FakeSource();
        source.artifact("x.proto", """
                syntax = "proto3";
                package unit.v1;
                import "y.proto";
                message X { unit.v1.Y y = 1; }
                """, ref("y.proto"));
        source.artifact("y.proto", """
                syntax = "proto3";
                package unit.v1;
                import "x.proto";
                message Y { unit.v1.X x = 1; }
                """, ref("x.proto"));

        assertThatThrownBy(() -> resolver(source).resolveAndParse(GROUP, "x.proto", LATEST))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cyclic artifact reference");
    }

    @Test
    void missingRootArtifactFails() {
        FakeSource source = new FakeSource();
        assertThatThrownBy(() -> resolver(source).resolveAndParse(GROUP, "nope.proto", LATEST))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nope.proto");
    }
}
