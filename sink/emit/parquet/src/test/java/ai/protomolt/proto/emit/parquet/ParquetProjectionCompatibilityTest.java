package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.projection.FieldProjection;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionPaths;
import ai.protomolt.proto.projection.ProjectionProto;
import ai.protomolt.proto.projection.ProjectionSources;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetProjectionCompatibilityTest {
    private static final ParquetRendererOptions OPTIONS = ParquetRendererOptions.getDefaultInstance();

    static Stream<Arguments> incompatibleSources() {
        return Stream.of("different_type", "renamed_value", "renumbered_value", "alias_order")
                .flatMap(change -> Stream.of(false, true).flatMap(repeated ->
                        Stream.of("state", "nested.state").map(path ->
                                Arguments.of(change, repeated, path))));
    }

    @ParameterizedTest
    @MethodSource("incompatibleSources")
    void refusesEnumSubstitutionBeforeWriting(String change, boolean repeated, String path)
            throws Exception {
        FileDescriptor declared = schema("expected", "state", path);
        FileDescriptor supplied = schema(change, "state", path);
        ParquetRendererProvider provider = provider(declared, repeated);
        Message input = input(supplied, repeated);

        assertThatThrownBy(() -> provider.check(input, OPTIONS))
                .isInstanceOfSatisfying(RenderFailure.class, failure -> {
                    assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.UNSUPPORTED);
                    assertThat(failure.getMessage()).contains("enum", "state");
                });
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertThatThrownBy(() -> provider.render(input, OPTIONS, output, () -> false))
                .isInstanceOfSatisfying(RenderFailure.class, failure ->
                        assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.UNSUPPORTED));
        assertThat(output.size()).isZero();
    }

    @Test
    void refusesRenamedNestedFieldEvenWhenItsMessageNameIsUnchanged() throws Exception {
        FileDescriptor declared = schema("expected", "state", "nested.state");
        FileDescriptor supplied = schema("expected", "other_state", "nested.state");
        assertThatThrownBy(() -> provider(declared, false).check(input(supplied, false), OPTIONS))
                .isInstanceOfSatisfying(RenderFailure.class, failure -> {
                    assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.UNSUPPORTED);
                    assertThat(failure.getMessage()).contains("state");
                });
    }

    @Test
    void independentlyLinkedCompatibleSchemasPreserveEnumNames(@TempDir Path dir) throws Exception {
        for (boolean repeated : new boolean[] {false, true}) {
            for (String path : new String[] {"state", "nested.state"}) {
                FileDescriptor declared = schema("expected", "state", path);
                FileDescriptor supplied = schema("expected", "state", path);
                assertThat(supplied).isNotSameAs(declared);
                ParquetRendererProvider provider = provider(declared, repeated);
                Message input = input(supplied, repeated);
                provider.check(input, OPTIONS);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                provider.render(input, OPTIONS, output, () -> false);
                Path file = dir.resolve(repeated + "-" + path + ".parquet");
                Files.write(file, output.toByteArray());
                try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(file)).build()) {
                    assertThat(reader.read().getString("state", 0)).isEqualTo("READY");
                    assertThat(reader.read()).isNull();
                }
            }
        }
    }

    static Stream<Arguments> mixedValues() {
        return Stream.of("enum", "nested", "element").flatMap(injection ->
                Stream.of(false, true).filter(repeated -> !injection.equals("element") || repeated)
                        .flatMap(repeated -> Stream.of("state", "nested.state")
                                .filter(path -> !injection.equals("nested") || path.equals("nested.state"))
                                .map(path -> Arguments.of(injection, repeated, path))));
    }

    static Stream<Arguments> incompatibleMixedValues() {
        return Stream.of("different_type", "renamed_value", "renumbered_value", "alias_order")
                .flatMap(change -> mixedValues().map(args -> Arguments.of(
                        change, args.get()[0], args.get()[1], args.get()[2])));
    }

    @ParameterizedTest
    @MethodSource("incompatibleMixedValues")
    void refusesIncompatibleRuntimeValuesBeforeWriting(
            String change, String injection, boolean repeated, String path) throws Exception {
        FileDescriptor declared = schema("expected", "state", path);
        FileDescriptor supplied = schema(change, "state", path);
        Message mixed = mixedInput(declared, supplied, injection, repeated, path);
        assertThat(mixed.getDescriptorForType())
                .isSameAs(declared.findMessageTypeByName(repeated ? "Batch" : "Source"));
        assertRefusedBeforeWriting(provider(declared, repeated), mixed);
    }

    @ParameterizedTest
    @MethodSource("mixedValues")
    void compatibleRuntimeValuesPreserveEnumNames(
            String injection, boolean repeated, String path, @TempDir Path dir) throws Exception {
        FileDescriptor declared = schema("expected", "state", path);
        FileDescriptor supplied = schema("expected", "state", path);
        Message mixed = mixedInput(declared, supplied, injection, repeated, path);
        ParquetRendererProvider provider = provider(declared, repeated);
        provider.check(mixed, OPTIONS);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        provider.render(mixed, OPTIONS, output, () -> false);
        Path file = dir.resolve("mixed.parquet");
        Files.write(file, output.toByteArray());
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(file)).build()) {
            assertThat(reader.read().getString("state", 0)).isEqualTo("READY");
            assertThat(reader.read()).isNull();
        }
    }

    @Test
    void refusesRuntimeChildWithRenamedField() throws Exception {
        FileDescriptor declared = schema("expected", "state", "nested.state");
        FileDescriptor supplied = schema("expected", "other_state", "nested.state");
        assertRefusedBeforeWriting(provider(declared, false),
                mixedInput(declared, supplied, "nested", false, "nested.state"));
    }

    @Test
    void refusesRuntimeChildWithDifferentMessageName() throws Exception {
        FileDescriptor declared = schema("expected", "state", "nested.state");
        Descriptor row = declared.findMessageTypeByName("Row");
        Message otherType = DynamicMessage.newBuilder(row)
                .setField(row.findFieldByName("state"),
                        declared.getEnumTypes().getFirst().findValueByNumber(1)).build();
        Descriptor source = declared.findMessageTypeByName("Source");
        Message mixed = input(declared, false).toBuilder()
                .setField(source.findFieldByName("nested"), otherType).build();
        assertRefusedBeforeWriting(provider(declared, false), mixed);
    }

    private static void assertRefusedBeforeWriting(ParquetRendererProvider provider, Message input) {
        assertThatThrownBy(() -> provider.check(input, OPTIONS))
                .isInstanceOfSatisfying(RenderFailure.class, failure ->
                        assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.UNSUPPORTED));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertThatThrownBy(() -> provider.render(input, OPTIONS, output, () -> false))
                .isInstanceOfSatisfying(RenderFailure.class, failure ->
                        assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.UNSUPPORTED));
        assertThat(output.size()).isZero();
    }

    private static Message mixedInput(FileDescriptor declared, FileDescriptor supplied,
            String injection, boolean repeated, String path) {
        Message foreign = input(supplied, false);
        Descriptor source = declared.findMessageTypeByName("Source");
        Message row;
        if (injection.equals("element")) {
            row = foreign;
        } else {
            var builder = input(declared, false).toBuilder();
            if (injection.equals("nested")) {
                builder.setField(source.findFieldByName("nested"),
                        foreign.getField(foreign.getDescriptorForType().findFieldByName("nested")));
            } else {
                var value = supplied.getEnumTypes().getFirst().findValueByNumber(1);
                if (path.equals("state")) {
                    builder.setField(source.findFieldByName("state"), value);
                } else {
                    Descriptor nested = declared.findMessageTypeByName("Nested");
                    builder.setField(source.findFieldByName("nested"), DynamicMessage.newBuilder(nested)
                            .setField(nested.findFieldByName("state"), value).build());
                }
            }
            row = builder.build();
        }
        if (!repeated) return row;
        Descriptor batch = declared.findMessageTypeByName("Batch");
        return DynamicMessage.newBuilder(batch)
                .addRepeatedField(batch.findFieldByName("items"), row).build();
    }

    private static ParquetRendererProvider provider(FileDescriptor file, boolean repeated) {
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.registerFile(file);
        MessageProjection projection = MessageProjection.forTarget(
                file.findMessageTypeByName("Row"), registry).orElseThrow();
        Descriptor source = file.findMessageTypeByName(repeated ? "Batch" : "Source");
        return repeated
                ? ParquetRendererProvider.projectedRepeated("test.enum.batch", "batch", source,
                        source.findFieldByName("items"), projection)
                : ParquetRendererProvider.projected("test.enum.single", "single", source, projection);
    }

    private static Message input(FileDescriptor file, boolean repeated) {
        Descriptor nested = file.findMessageTypeByName("Nested");
        var enumValue = file.getEnumTypes().getFirst().findValueByNumber(1);
        Message nestedValue = DynamicMessage.newBuilder(nested)
                .setField(nested.getFields().getFirst(), enumValue).build();
        Descriptor source = file.findMessageTypeByName("Source");
        DynamicMessage.Builder rowBuilder = DynamicMessage.newBuilder(source)
                .setField(source.findFieldByName("nested"), nestedValue);
        if (source.findFieldByName("state") != null) {
            rowBuilder.setField(source.findFieldByName("state"), enumValue);
        }
        Message row = rowBuilder.build();
        if (!repeated) return row;
        Descriptor batch = file.findMessageTypeByName("Batch");
        return DynamicMessage.newBuilder(batch)
                .addRepeatedField(batch.findFieldByName("items"), row).build();
    }

    private static FileDescriptor schema(String change, String nestedField, String path)
            throws Exception {
        String enumName = change.equals("different_type") ? "OtherState" : "State";
        EnumDescriptorProto.Builder values = EnumDescriptorProto.newBuilder().setName(enumName)
                .setOptions(EnumOptions.newBuilder().setAllowAlias(true))
                .addValue(value("UNSPECIFIED", 0));
        if (change.equals("alias_order")) values.addValue(value("ALIAS", 1));
        values.addValue(value(change.equals("renamed_value") ? "REJECTED" : "READY",
                        change.equals("renumbered_value") ? 2 : 1))
                .addValue(value("FAULT", change.equals("renumbered_value") ? 1 : 2));
        if (!change.equals("alias_order")) {
            values.addValue(value("ALIAS", change.equals("renumbered_value") ? 2 : 1));
        }
        String enumType = ".enumcheck." + enumName;
        DescriptorProto.Builder source = DescriptorProto.newBuilder().setName("Source")
                .addField(field("nested", 2, Type.TYPE_MESSAGE, ".enumcheck.Nested"));
        if (path.equals("state")) source.addField(field("state", 1, Type.TYPE_ENUM, enumType));
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("enumcheck.proto").setPackage("enumcheck").setSyntax("proto3")
                .addDependency(ProjectionProto.getDescriptor().getName())
                .addEnumType(values)
                .addMessageType(DescriptorProto.newBuilder().setName("Nested")
                        .addField(field(nestedField, 1, Type.TYPE_ENUM, enumType)))
                .addMessageType(source)
                .addMessageType(DescriptorProto.newBuilder().setName("Batch")
                        .addField(field("items", 1, Type.TYPE_MESSAGE, ".enumcheck.Source")
                                .setLabel(Label.LABEL_REPEATED)))
                .addMessageType(DescriptorProto.newBuilder().setName("Row")
                        .setOptions(MessageOptions.newBuilder().setExtension(ProjectionProto.sources,
                                ProjectionSources.newBuilder().addSource("enumcheck.Source").build()))
                        .addField(field("state", 1, Type.TYPE_ENUM, enumType)
                                .setOptions(FieldOptions.newBuilder().setExtension(ProjectionProto.from,
                                        FieldProjection.newBuilder().setPaths(ProjectionPaths.newBuilder()
                                                .addPath(path)).build()))))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[] {ProjectionProto.getDescriptor()});
    }

    private static EnumValueDescriptorProto value(String name, int number) {
        return EnumValueDescriptorProto.newBuilder().setName(name).setNumber(number).build();
    }

    private static FieldDescriptorProto.Builder field(String name, int number, Type type, String typeName) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                .setLabel(Label.LABEL_OPTIONAL).setType(type).setTypeName(typeName);
    }

    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) { super(file); }
        @Override protected GroupReadSupport getReadSupport() { return new GroupReadSupport(); }
    }
}
