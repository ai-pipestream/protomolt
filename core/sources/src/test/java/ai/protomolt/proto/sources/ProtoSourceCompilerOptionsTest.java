package ai.protomolt.proto.sources;

import ai.protomolt.proto.llm.LlmProto;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.quality.QualityProto;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the option-repair pass: Wire's {@code SchemaEncoder} mangles custom options whose
 * extension fields share a simple name across packages (the validate/meta/llm families all
 * declare a {@code field} extension; validate/meta/llm all declare {@code message}), so the
 * compiler re-encodes every element's options from Wire's linked model instead. Compiled sets
 * are read back off the wire with the generated extension classes registered — exactly how a
 * downstream consumer parses a descriptor set — and families an element did not declare must
 * be absent, not present-but-empty (the phantom shape the encoder bug produced).
 *
 * <p>The annotated form and its four annotation-family imports are verbatim copies of
 * {@code protobuf/prompt/src/test/proto/.../form.proto} and the family protos under
 * {@code protobuf/<family>/src/main/proto}; if a family proto changes, refresh the copies
 * under {@code src/test/resources}.</p>
 */
class ProtoSourceCompilerOptionsTest {

    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String METADATA = "ai/protomolt/proto/meta/v1/metadata.proto";
    private static final String LLM = "ai/protomolt/proto/llm/v1/llm.proto";
    private static final String QUALITY = "ai/protomolt/proto/quality/v1/quality.proto";
    private static final String FORM = "ai/protomolt/proto/prompt/testdata/v1/form.proto";

    private static final ExtensionRegistry REGISTRY = ExtensionRegistry.newInstance();

    static {
        ValidateProto.registerAllExtensions(REGISTRY);
        MetadataProto.registerAllExtensions(REGISTRY);
        LlmProto.registerAllExtensions(REGISTRY);
        QualityProto.registerAllExtensions(REGISTRY);
    }

    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    @Test
    void oneBracketListKeepsEveryFamily() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(METADATA, resource(METADATA), "test")
                .add(LLM, resource(LLM), "test")
                .add("doc.proto", """
                        syntax = "proto3";
                        package test.bracket;
                        import "ai/protomolt/proto/validate/v1/validate.proto";
                        import "ai/protomolt/proto/meta/v1/metadata.proto";
                        import "ai/protomolt/proto/llm/v1/llm.proto";
                        message Doc {
                          string title = 1 [
                            (ai.protomolt.proto.validate.v1.field) = {
                              required: true
                              string: {max_len: 200}
                            },
                            (ai.protomolt.proto.meta.v1.field) = {
                              description: "The title."
                            },
                            (ai.protomolt.proto.llm.v1.field) = {
                              instruction: "Fill the title."
                            }
                          ];
                        }
                        """, "test")
                .build();

        FieldOptions options = field(compile(set), "doc.proto", "Doc", "title").getOptions();

        assertThat(options.hasExtension(ValidateProto.field)).isTrue();
        assertThat(options.getExtension(ValidateProto.field).getRequired()).isTrue();
        assertThat(options.getExtension(ValidateProto.field).getString().getMaxLen()).isEqualTo(200);
        assertThat(options.hasExtension(MetadataProto.field)).isTrue();
        assertThat(options.getExtension(MetadataProto.field).getDescription()).isEqualTo("The title.");
        assertThat(options.hasExtension(LlmProto.field)).isTrue();
        assertThat(options.getExtension(LlmProto.field).getInstruction()).isEqualTo("Fill the title.");
    }

    @Test
    void separateOptionStatementsKeepEveryFamily() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(METADATA, resource(METADATA), "test")
                .add(LLM, resource(LLM), "test")
                .add("doc.proto", """
                        syntax = "proto3";
                        package test.statements;
                        import "ai/protomolt/proto/validate/v1/validate.proto";
                        import "ai/protomolt/proto/meta/v1/metadata.proto";
                        import "ai/protomolt/proto/llm/v1/llm.proto";
                        message Doc {
                          option (ai.protomolt.proto.meta.v1.message) = {
                            description: "A document."
                          };
                          option (ai.protomolt.proto.llm.v1.message) = {
                            instruction: "Fill the document."
                          };
                          string title = 1;
                        }
                        """, "test")
                .build();

        MessageOptions options = message(compile(set), "doc.proto", "Doc").getOptions();

        assertThat(options.hasExtension(MetadataProto.message)).isTrue();
        assertThat(options.getExtension(MetadataProto.message).getDescription()).isEqualTo("A document.");
        assertThat(options.hasExtension(LlmProto.message)).isTrue();
        assertThat(options.getExtension(LlmProto.message).getInstruction()).isEqualTo("Fill the document.");
        assertThat(options.hasExtension(ValidateProto.message)).isFalse();
        assertThat(options.hasExtension(QualityProto.quality)).isFalse();
    }

    @Test
    void annotatedFormKeepsEveryOption() throws Exception {
        CompiledProtos compiled = compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(METADATA, resource(METADATA), "test")
                .add(LLM, resource(LLM), "test")
                .add(QUALITY, resource(QUALITY), "test")
                .add(FORM, resource(FORM), "test")
                .build());

        // Message level: all four families with content.
        MessageOptions messageOptions = message(compiled, FORM, "DecoratedOpinion").getOptions();
        assertThat(messageOptions.getExtension(MetadataProto.message).getDescription())
                .isEqualTo("Metadata extracted from a court opinion.");
        assertThat(messageOptions.getExtension(LlmProto.message).getInstruction())
                .isEqualTo("Fill this form from the opinion text alone.");
        assertThat(messageOptions.getExtension(LlmProto.message).getSafeguardsList())
                .containsExactly("Do not use outside legal knowledge.");
        assertThat(messageOptions.getExtension(ValidateProto.message).getCelList())
                .singleElement()
                .satisfies(cel -> {
                    assertThat(cel.getId()).isEqualTo("court.when.summary");
                    assertThat(cel.getExpression())
                            .isEqualTo("size(this.summary) == 0 || size(this.court) > 0");
                    assertThat(cel.getMessage()).isEqualTo("a summary requires a court");
                });
        assertThat(messageOptions.getExtension(QualityProto.quality).getDimensionList())
                .singleElement()
                .satisfies(dimension -> {
                    assertThat(dimension.getId()).isEqualTo("completeness");
                    assertThat(dimension.getCel())
                            .isEqualTo("size(this.court) > 0 && size(this.summary) > 0 ? 1.0 : 0.5");
                    assertThat(dimension.getWeight()).isEqualTo(2.0);
                });

        // court: validate + meta + llm.
        FieldOptions court = field(compiled, FORM, "DecoratedOpinion", "court").getOptions();
        assertThat(court.getExtension(ValidateProto.field).getRequired()).isTrue();
        assertThat(court.getExtension(ValidateProto.field).getString().getMaxLen()).isEqualTo(200);
        assertThat(court.getExtension(MetadataProto.field).getDescription())
                .isEqualTo("The issuing court, as it appears in the caption.");
        assertThat(court.getExtension(LlmProto.field).getInstruction())
                .isEqualTo("Name the court exactly as it appears in the caption.");
        assertThat(court.getExtension(LlmProto.field).getSafeguardsList())
                .containsExactly("Do not abbreviate.");

        // topics: validate repeated rules only — the other families must be absent, not empty.
        FieldOptions topics = field(compiled, FORM, "DecoratedOpinion", "topics").getOptions();
        assertThat(topics.getExtension(ValidateProto.field).getRepeated().getMinItems()).isEqualTo(1);
        assertThat(topics.getExtension(ValidateProto.field).getRepeated().getMaxItems()).isEqualTo(10);
        assertThat(topics.getExtension(ValidateProto.field).getRepeated().getUnique()).isTrue();
        assertThat(topics.hasExtension(MetadataProto.field)).isFalse();
        assertThat(topics.hasExtension(LlmProto.field)).isFalse();

        // posture: enum defined_only only.
        FieldOptions posture = field(compiled, FORM, "DecoratedOpinion", "posture").getOptions();
        assertThat(posture.getExtension(ValidateProto.field).getEnum().getDefinedOnly()).isTrue();
        assertThat(posture.hasExtension(MetadataProto.field)).isFalse();
        assertThat(posture.hasExtension(LlmProto.field)).isFalse();

        // leading_authority: llm instruction + volatile.
        FieldOptions authority = field(compiled, FORM, "DecoratedOpinion", "leading_authority")
                .getOptions();
        assertThat(authority.getExtension(LlmProto.field).getInstruction())
                .isEqualTo("Cite the authority the opinion treats as controlling.");
        assertThat(authority.getExtension(LlmProto.field).getVolatile()).isTrue();
        assertThat(authority.hasExtension(ValidateProto.field)).isFalse();

        // summary: string max_len 2000.
        FieldOptions summary = field(compiled, FORM, "DecoratedOpinion", "summary").getOptions();
        assertThat(summary.getExtension(ValidateProto.field).getString().getMaxLen()).isEqualTo(2000);

        // year: int32 gte/lte.
        FieldOptions year = field(compiled, FORM, "DecoratedOpinion", "year").getOptions();
        assertThat(year.getExtension(ValidateProto.field).getInt32().getGte()).isEqualTo(1600);
        assertThat(year.getExtension(ValidateProto.field).getInt32().getLte()).isEqualTo(2100);

        // The plain enum carries nothing from any family.
        assertThat(compiled.descriptorSet().getFileList()).anySatisfy(file -> {
            if (file.getName().equals(FORM)) {
                assertThat(file.getEnumType(0).getName()).isEqualTo("Posture");
            }
        });
    }

    @Test
    void regularOptionsSurviveRepair() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("regular.proto", """
                        syntax = "proto2";
                        package test.regular;
                        option java_package = "com.example.regular";
                        message Doc {
                          optional string name = 1 [json_name = "n", deprecated = true];
                        }
                        """, "test")
                .build();

        FileDescriptorProto file = fileProto(compile(set), "regular.proto");

        FileOptions fileOptions = FileOptions.parseFrom(
                file.getOptions().toByteArray(), REGISTRY);
        assertThat(fileOptions.getJavaPackage()).isEqualTo("com.example.regular");
        FieldDescriptorProto name = file.getMessageType(0).getField(0);
        assertThat(name.getJsonName()).isEqualTo("n");
        assertThat(name.getOptions().getDeprecated()).isTrue();
    }

    @Test
    void repeatedOptionValuesRoundTripInOrder() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("doc.proto", """
                        syntax = "proto3";
                        package test.repeated;
                        import "ai/protomolt/proto/validate/v1/validate.proto";
                        message Doc {
                          option (ai.protomolt.proto.validate.v1.message) = {
                            cel: {id: "first", expression: "true"}
                            cel: {id: "second", expression: "size(this.status) > 0"}
                          };
                          string status = 1 [
                            (ai.protomolt.proto.validate.v1.field) = {
                              string: {in: ["a", "b", "c"]}
                            }
                          ];
                        }
                        """, "test")
                .build();

        CompiledProtos compiled = compile(set);

        assertThat(field(compiled, "doc.proto", "Doc", "status").getOptions()
                .getExtension(ValidateProto.field).getString().getInList())
                .containsExactly("a", "b", "c");
        assertThat(message(compiled, "doc.proto", "Doc").getOptions()
                .getExtension(ValidateProto.message).getCelList())
                .extracting(ai.protomolt.proto.validate.CelRule::getId)
                .containsExactly("first", "second");
    }

    @Test
    void enumValuedOptionCoercesByName() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("enumerated.proto", """
                        syntax = "proto3";
                        package test.enumerated;
                        import "google/protobuf/descriptor.proto";
                        message Fill { Mode mode = 1; }
                        enum Mode {
                          MODE_UNSPECIFIED = 0;
                          MODE_FAST = 1;
                          MODE_SLOW = 2;
                        }
                        extend google.protobuf.FieldOptions { Fill fill = 51234; }
                        message Doc {
                          string name = 1 [(fill) = {mode: MODE_FAST}];
                        }
                        """, "test")
                .build();

        CompiledProtos compiled = compile(set);

        // No generated class exists for this test extension, so read it dynamically: the
        // extension is field 51234 of FieldOptions, holding a Fill message.
        FieldOptions options = FieldOptions.parseFrom(
                field(compiled, "enumerated.proto", "Doc", "name").getOptions().toByteArray(),
                REGISTRY);
        assertThat(options.getUnknownFields().hasField(51234)).isTrue();
        Descriptor fill = compiled.descriptorFor("enumerated.proto").orElseThrow()
                .findMessageTypeByName("Fill");
        DynamicMessage value = DynamicMessage.parseFrom(fill,
                options.getUnknownFields().getField(51234).getLengthDelimitedList().get(0));
        FieldDescriptor mode = fill.findFieldByName("mode");
        assertThat(((com.google.protobuf.Descriptors.EnumValueDescriptor) value.getField(mode))
                .getName()).isEqualTo("MODE_FAST");
    }

    @Test
    void mapValuedOptionsRoundTrip() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(METADATA, resource(METADATA), "test")
                .add("doc.proto", """
                        syntax = "proto3";
                        package test.labels;
                        import "ai/protomolt/proto/meta/v1/metadata.proto";
                        message Doc {
                          option (ai.protomolt.proto.meta.v1.message) = {
                            description: "A labeled document."
                            labels: {key: "component", value: "jobs"}
                            labels: {key: "tier", value: "gold"}
                          };
                          string title = 1 [
                            (ai.protomolt.proto.meta.v1.field) = {
                              description: "The title."
                              labels: {key: "pii", value: "no"}
                            }
                          ];
                        }
                        """, "test")
                .build();

        CompiledProtos compiled = compile(set);

        MessageOptions messageOptions = message(compiled, "doc.proto", "Doc").getOptions();
        assertThat(messageOptions.getExtension(MetadataProto.message).getLabelsMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("component", "jobs", "tier", "gold"));
        assertThat(messageOptions.getExtension(MetadataProto.message).getDescription())
                .isEqualTo("A labeled document.");

        FieldOptions fieldOptions = field(compiled, "doc.proto", "Doc", "title").getOptions();
        assertThat(fieldOptions.getExtension(MetadataProto.field).getLabelsMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("pii", "no"));
        assertThat(fieldOptions.getExtension(MetadataProto.field).getDescription())
                .isEqualTo("The title.");
    }

    @Test
    void allowAliasEnumCompiles() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("alias.proto", """
                        syntax = "proto3";
                        package test.alias;
                        enum Status {
                          option allow_alias = true;
                          STATUS_UNSPECIFIED = 0;
                          STATUS_ACTIVE = 1;
                          STATUS_ON = 1;
                        }
                        """, "test")
                .build();

        // allow_alias is structural: stripping it would make the duplicate tags fail to link.
        assertThat(compile(set).descriptorFor("alias.proto").orElseThrow()
                .findEnumTypeByName("Status").getOptions().getAllowAlias()).isTrue();
    }

    @Test
    void unknownEnumIdentifierFailsLoud() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("enumerated.proto", """
                        syntax = "proto3";
                        package test.enumerated;
                        import "google/protobuf/descriptor.proto";
                        message Fill { Mode mode = 1; }
                        enum Mode {
                          MODE_UNSPECIFIED = 0;
                          MODE_FAST = 1;
                        }
                        extend google.protobuf.FieldOptions { Fill fill = 51234; }
                        message Doc {
                          string name = 1 [(fill) = {mode: MODE_BOGUS}];
                        }
                        """, "test")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class);
    }

    /** Compiles and re-parses the set off the wire with the annotation extensions registered. */
    private FileDescriptorProto fileProto(CompiledProtos compiled, String path) throws IOException {
        FileDescriptorSet wire = FileDescriptorSet.parseFrom(
                compiled.descriptorSet().toByteArray(), REGISTRY);
        return wire.getFileList().stream()
                .filter(file -> file.getName().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("compiled set has no " + path));
    }

    private DescriptorProto message(CompiledProtos compiled, String path, String name)
            throws IOException {
        return fileProto(compiled, path).getMessageTypeList().stream()
                .filter(message -> message.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(path + " has no message " + name));
    }

    private FieldDescriptorProto field(CompiledProtos compiled, String path, String message,
                                       String field) throws IOException {
        return message(compiled, path, message).getFieldList().stream()
                .filter(candidate -> candidate.getName().equals(field))
                .findFirst()
                .orElseThrow(() -> new AssertionError(path + " has no field " + message + "." + field));
    }

    private CompiledProtos compile(ProtoSourceSet set) throws ProtoCompilationException {
        return compiler.compile(set);
    }

    private static String resource(String path) {
        try (InputStream in = ProtoSourceCompilerOptionsTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
