package ai.protomolt.proto.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.types.Taxonomy;
import ai.protomolt.proto.types.TreePath;
import ai.protomolt.proto.types.TreePathProto;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The taxonomy mount contract: the subject is the identity, the source's
 * version is the version, a valid document mounts and swaps atomically, a
 * document failing Taxonomy's own rules never mounts, and a validator built
 * over the catalog changes verdicts on the next validation after a swap —
 * updating a taxonomy is one config publish, no schema change.
 */
class TaxonomyMountsTest {

    /** A source over a map, as the consumer proof uses. */
    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();

        @Override
        public Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }
    }

    private static TreePath path(String... segments) {
        TreePath.Builder builder = TreePath.newBuilder();
        for (String segment : segments) {
            builder.addSegments(segment);
        }
        return builder.build();
    }

    private static Taxonomy taxonomy(TreePath... entries) {
        Taxonomy.Builder builder = Taxonomy.newBuilder();
        for (TreePath entry : entries) {
            builder.addEntries(entry);
        }
        return builder.build();
    }

    @Test
    void mountsFollowTheirSubjectsAndSwapAtomically() {
        FakeSource source = new FakeSource();
        source.documents.put("taxonomy:products", new ConfigSource.Fetched(
                "v1", taxonomy(path("electronics", "computers")).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TaxonomyMounts mounts = TaxonomyMounts.follow(config, List.of("products"));
            assertThat(mounts.taxonomy("products"))
                    .as("unmounted until a first document applies")
                    .isEmpty();

            config.refresh();
            TaxonomyCatalog.Mounted mounted = mounts.taxonomy("products").orElseThrow();
            assertThat(mounted.version()).isEqualTo("v1");
            assertThat(mounted.nodes())
                    .as("every ancestor of an entry is a node")
                    .containsExactlyInAnyOrder("electronics", "electronics/computers");

            source.documents.put("taxonomy:products", new ConfigSource.Fetched(
                    "v2", taxonomy(path("media")).toByteArray()));
            config.refresh();
            TaxonomyCatalog.Mounted swapped = mounts.taxonomy("products").orElseThrow();
            assertThat(swapped.version()).isEqualTo("v2");
            assertThat(swapped.nodes()).containsExactly("media");
        }
    }

    @Test
    void aDocumentFailingItsOwnRulesNeverMounts() {
        FakeSource source = new FakeSource();
        source.documents.put("taxonomy:products", new ConfigSource.Fetched(
                "v1", taxonomy(path("media")).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TaxonomyMounts mounts = TaxonomyMounts.follow(config, List.of("products"));
            config.refresh();

            // An entryless taxonomy violates Taxonomy's own min_items rule.
            source.documents.put("taxonomy:products",
                    new ConfigSource.Fetched("v2", Taxonomy.getDefaultInstance().toByteArray()));
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.refused()).singleElement()
                    .satisfies(refusal -> assertThat(refusal.reason()).contains("entries"));
            assertThat(mounts.taxonomy("products").orElseThrow().version())
                    .as("the mounted taxonomy keeps serving")
                    .isEqualTo("v1");
        }
    }

    @Test
    void theLaneFeedsTheGate() throws Exception {
        // A schema binds a TreePath field to the taxonomy by name; the gate's
        // validator follows the mount, so a config publish changes verdicts
        // with no schema change and no restart.
        Descriptors.FileDescriptor file = docFile();
        Descriptors.Descriptor doc = file.findMessageTypeByName("Doc");
        DynamicMessage document = DynamicMessage.newBuilder(doc)
                .setField(doc.findFieldByName("category"),
                        path("electronics", "computers"))
                .build();

        FakeSource source = new FakeSource();
        source.documents.put("taxonomy:products", new ConfigSource.Fetched(
                "v1", taxonomy(path("electronics", "computers", "laptops")).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TaxonomyMounts mounts = TaxonomyMounts.follow(config, List.of("products"));
            ProtoValidator validator = ProtoValidator.create(mounts);

            assertThat(validator.validate(document).valid())
                    .as("unmounted refuses fail-closed")
                    .isFalse();

            config.refresh();
            assertThat(validator.validate(document).valid()).isTrue();

            source.documents.put("taxonomy:products", new ConfigSource.Fetched(
                    "v2", taxonomy(path("media")).toByteArray()));
            config.refresh();
            ValidationResult result = validator.validate(document);
            assertThat(result.violations()).singleElement().satisfies(violation -> {
                assertThat(violation.ruleId()).isEqualTo("taxonomy.member");
                assertThat(violation.message())
                        .as("the swapped version is the evidence")
                        .contains("v2");
            });
        }
    }

    @Test
    void blankNamesRefuse() {
        try (DistributedConfig config = DistributedConfig.over(new FakeSource())) {
            assertThatThrownBy(() -> TaxonomyMounts.follow(config, List.of(" ")))
                    .hasMessageContaining("name");
        }
    }

    /** A dynamic schema with a TreePath field bound to the "products" taxonomy. */
    private static Descriptors.FileDescriptor docFile() throws Exception {
        DescriptorProtos.FieldOptions binding = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field,
                        FieldRules.newBuilder().setTaxonomy("products").build())
                .build();
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto
                .newBuilder()
                .setName("taxonomy_mounts_test.proto")
                .setSyntax("proto3")
                .setPackage("test.config")
                .addDependency("ai/pipestream/proto/types/v1/tree_path.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label
                                        .LABEL_OPTIONAL)
                                .setOptions(binding)))
                .build();
        return Descriptors.FileDescriptor.buildFrom(
                proto, new Descriptors.FileDescriptor[] {TreePathProto.getDescriptor()});
    }
}
