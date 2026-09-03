package ai.protomolt.proto.emit.okf;

import ai.protomolt.proto.emit.Bundle;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The OKF bundle must conform to the v0.1 spec (frontmatter with a type on every concept,
 * reserved index files, bundle-relative links) and must carry the schema's own metadata:
 * descriptions and sensitivity classes come from the meta.v1 annotations, and message-typed
 * fields link to their type's concept document.
 */
class OkfRendererTest {

    private static final String SHOP_PROTO = """
            syntax = "proto3";
            package okf.shop.v1;
            import "ai/protomolt/proto/meta/v1/metadata.proto";

            message Customer {
              // Wire cannot encode map-valued options (labels); tag rendering from
              // labels is covered by the frontmatter unit test below.
              option (ai.protomolt.proto.meta.v1.message) = {
                description: "A shop customer. Owned by the accounts team."
                owner: "accounts"
              };
              string id = 1 [(ai.protomolt.proto.meta.v1.field) = {
                description: "Stable customer id."
              }];
              string email = 2 [(ai.protomolt.proto.meta.v1.field) = {
                description: "Contact address."
                sensitivity: "pii"
              }];
            }

            message Order {
              string order_id = 1;
              Customer customer = 2;
              repeated string tags = 3;
              map<string, int64> counts = 4;
              Status status = 5;
            }

            enum Status {
              STATUS_UNSPECIFIED = 0;
              STATUS_OPEN = 1;
              STATUS_SHIPPED = 2;
            }

            service Orders {
              rpc Get(Order) returns (Order);
              rpc Watch(Order) returns (stream Order);
            }
            """;

    private static List<FileDescriptor> compile() throws Exception {
        String metadataProto = new String(OkfRendererTest.class.getClassLoader()
                .getResourceAsStream("ai/protomolt/proto/meta/v1/metadata.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/meta/v1/metadata.proto", metadataProto, "meta")
                .add("okf/shop/v1/shop.proto", SHOP_PROTO, "test")
                .build());
        return OkfRegistryBundles.linkWithMetadata(compiled.descriptorSet());
    }

    @Test
    void rendersAConformantCrossLinkedBundle() throws Exception {
        Bundle bundle = new OkfRenderer().render(compile(),
                new OkfRenderer.Options("Shop schemas", null));

        // Root index declares the OKF version and links every kind index.
        String root = bundle.text("index.md");
        assertThat(root).startsWith("---\nokf_version: \"0.1\"\n---");
        assertThat(root).contains("# Shop schemas")
                .contains("(/messages/index.md)")
                .contains("(/enums/index.md)")
                .contains("(/services/index.md)");

        // Every concept document has frontmatter with a non-empty type (the only
        // conformance requirement) and index files have none.
        for (String path : bundle.paths()) {
            String text = bundle.text(path);
            if (path.endsWith("index.md") && !path.equals("index.md")) {
                assertThat(text).as(path).doesNotContain("---\n");
            } else if (!path.equals("index.md")) {
                assertThat(text).as(path).startsWith("---\n").contains("type: ");
            }
        }

        // The message concept carries the schema's own metadata.
        String customer = bundle.text("messages/okf.shop.v1.Customer.md");
        assertThat(customer)
                .contains("type: Protobuf Message")
                .contains("title: Customer")
                .contains("description: A shop customer.")
                .contains("tags: [owner:accounts]")
                .contains("| `email` | `string` |  | Contact address. | `pii` |");

        // Cross-links: Order.customer links to the Customer concept, the enum to enums/.
        String order = bundle.text("messages/okf.shop.v1.Order.md");
        assertThat(order)
                .contains("[`okf.shop.v1.Customer`](/messages/okf.shop.v1.Customer.md)")
                .contains("[`okf.shop.v1.Status`](/enums/okf.shop.v1.Status.md)")
                .contains("map<`string`, `int64`>")
                .contains("| `tags` | `string` | repeated |");
        // No sensitivity column when no field in the type carries a class.
        assertThat(order).doesNotContain("Sensitivity");

        // The service concept lists methods with streaming shape and request links.
        String service = bundle.text("services/okf.shop.v1.Orders.md");
        assertThat(service)
                .contains("type: gRPC Service")
                .contains("| `Get` | [`okf.shop.v1.Order`](/messages/okf.shop.v1.Order.md) |")
                .contains("| unary |")
                .contains("| server |");

        // The option-carrier types themselves never pollute the bundle.
        assertThat(bundle.paths()).noneMatch(p -> p.contains("ai.protomolt.proto.meta"));

        // Enum values table.
        assertThat(bundle.text("enums/okf.shop.v1.Status.md"))
                .contains("| `STATUS_SHIPPED` | 2 |");
    }

    @Test
    void everyOptionCarrierDialectStaysOutOfTheBundle() throws Exception {
        // Each dialect gets a distinct shape: hints as a field type, llm as an
        // enum, projection as a nested carrier, so a filter that only handles
        // one kind of leak cannot pass by accident.
        String hints = """
                syntax = "proto3";
                package ai.protomolt.proto.index.hints.v1;
                message FieldHint { string analyzer = 1; }
                """;
        String llm = """
                syntax = "proto3";
                package ai.protomolt.proto.llm.v1;
                enum Tone { TONE_UNSPECIFIED = 0; TONE_FORMAL = 1; }
                """;
        String projection = """
                syntax = "proto3";
                package ai.protomolt.proto.projection.v1;
                message Rule { message Scope { string path = 1; } Scope scope = 1; }
                """;
        String schema = """
                syntax = "proto3";
                package okf.docs.v1;
                import "ai/protomolt/proto/index/hints/v1/hints.proto";
                import "ai/protomolt/proto/llm/v1/llm.proto";
                import "ai/protomolt/proto/projection/v1/projection.proto";

                message Doc {
                  string body = 1;
                  ai.protomolt.proto.index.hints.v1.FieldHint hint = 2;
                  ai.protomolt.proto.llm.v1.Tone tone = 3;
                  ai.protomolt.proto.projection.v1.Rule rule = 4;
                }
                """;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/index/hints/v1/hints.proto", hints, "hints")
                .add("ai/protomolt/proto/llm/v1/llm.proto", llm, "llm")
                .add("ai/protomolt/proto/projection/v1/projection.proto", projection, "projection")
                .add("okf/docs/v1/docs.proto", schema, "test")
                .build());
        Bundle bundle = new OkfRenderer().render(
                OkfRegistryBundles.linkWithMetadata(compiled.descriptorSet()),
                OkfRenderer.Options.defaults());

        assertThat(bundle.paths()).anyMatch(p -> p.contains("okf.docs.v1.Doc"));
        assertThat(bundle.paths())
                .noneMatch(p -> p.contains("ai.protomolt.proto.index.hints"))
                .noneMatch(p -> p.contains("ai.protomolt.proto.llm"))
                .noneMatch(p -> p.contains("ai.protomolt.proto.projection"));
    }

    @Test
    void frontmatterRendersLabelsAsTagsAndQuotesUnsafeYaml() {
        StringBuilder doc = new StringBuilder();
        OkfRenderer.frontmatter(doc, "Protobuf Message", "Order: v2",
                "First sentence. Second sentence.",
                java.util.Map.of("domain", "shop"), "accounts", null);
        assertThat(doc.toString())
                .contains("title: \"Order: v2\"")
                .contains("description: First sentence.")
                .contains("tags: [owner:accounts, domain:shop]");
    }

    @Test
    void registryBundlesRenderSubjectsWithVersionHistory() {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
        String v1 = """
                syntax = "proto3";
                package okf.reg.v1;
                message Reading { string sensor = 1; }
                """;
        String v2 = """
                syntax = "proto3";
                package okf.reg.v1;
                message Reading { string sensor = 1; double value = 2; }
                """;
        store.register("okf/reg/v1/reading.proto", v1, List.of());
        store.register("okf/reg/v1/reading.proto", v2, List.of());

        Bundle bundle = OkfRegistryBundles.render(store,
                new OkfRenderer.Options("Registry", "http://reg.example:8081"));

        String subject = bundle.text("subjects/okf_reg_v1_reading.proto.md");
        assertThat(subject)
                .contains("type: Registry Subject")
                .contains("title: okf/reg/v1/reading.proto")
                .contains("resource: \"http://reg.example:8081/subjects/"
                        + "okf%2Freg%2Fv1%2Freading.proto/versions/latest\"")
                .contains("Latest version 2")
                .contains("[okf.reg.v1.Reading](/messages/okf.reg.v1.Reading.md)")
                .contains("| 2 | ")
                .contains("| 1 | ");

        // The declared type is rendered as a normal message concept.
        assertThat(bundle.text("messages/okf.reg.v1.Reading.md"))
                .contains("| `value` | `double` |");
        assertThat(bundle.text("subjects/index.md"))
                .contains("* [okf/reg/v1/reading.proto](/subjects/okf_reg_v1_reading.proto.md)");
        assertThat(bundle.text("index.md")).contains("(/subjects/index.md)");
    }

    /**
     * Subject sanitisation is many-to-one, so two subjects can reduce to the same file name.
     * Without a suffix the later one silently replaced the earlier one and a whole subject
     * vanished from the bundle.
     */
    @Test
    void subjectsWhoseNamesSanitizeAlikeEachKeepTheirDocument() {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
        store.register("okf/dup/one.proto", """
                syntax = "proto3";
                package okf.dup.a;
                message A { string x = 1; }
                """, List.of());
        store.register("okf:dup:one.proto", """
                syntax = "proto3";
                package okf.dup.b;
                message B { string y = 1; }
                """, List.of());

        Bundle bundle = OkfRegistryBundles.render(store, new OkfRenderer.Options("Registry", null));

        assertThat(bundle.paths()).contains(
                "subjects/okf_dup_one.proto.md", "subjects/okf_dup_one.proto-2.md");
        assertThat(bundle.text("subjects/okf_dup_one.proto.md")
                + bundle.text("subjects/okf_dup_one.proto-2.md"))
                .contains("okf/dup/one.proto")
                .contains("okf:dup:one.proto");
    }

    /** The dependency's name is the only thing that tells an operator what the store is missing. */
    @Test
    void aMissingDependencyIsReportedByName() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("okf/dep/leaf.proto")
                        .setSyntax("proto3")
                        .addDependency("okf/dep/absent.proto"))
                .build();

        assertThatThrownBy(() -> OkfRegistryBundles.linkWithMetadata(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("okf/dep/leaf.proto")
                .hasMessageContaining("okf/dep/absent.proto");
    }
}
