package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.meta.SensitivityMasker;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parquet export controls: projection drops columns from the file, masking obscures values in the
 * columns it keeps, and the two compose. The distinction that bites: {@code REMOVE} on a proto3
 * plain scalar (a Parquet {@code required} column) only clears it to its default; projecting the
 * column out is what actually keeps it out of the file.
 */
class ParquetExportTest {

    private static final String SENS_PROTO = """
            syntax = "proto3";
            package pq.export;
            import "ai/pipestream/proto/meta/v1/metadata.proto";
            message Person {
              string id = 1;
              string email = 2 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
              string ssn = 3 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "secret"}];
              int64 age = 4;
            }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        String metadataProto = new String(ParquetExportTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/meta/v1/metadata.proto").readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/meta/v1/metadata.proto", metadataProto, "meta")
                .add("pq/export/person.proto", SENS_PROTO, "test").build());
        // Re-parse with the metadata extensions registered so the sensitivity option is readable,
        // not an unknown field - what ProtoMolt's compile/reflect verbs hand a real caller.
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        DescriptorMetadata.registerExtensions(registry);
        FileDescriptorSet set = DescriptorMetadata.materializeJsonNames(
                FileDescriptorSet.parseFrom(compiled.descriptorSet().toByteArray(), registry));
        file = link(set).get("pq/export/person.proto");
    }

    private static Map<String, FileDescriptor> link(FileDescriptorSet set) throws Exception {
        Map<String, FileDescriptorProto> byName = new java.util.LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new java.util.LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            build(proto, byName, built);
        }
        return built;
    }

    private static FileDescriptor build(FileDescriptorProto proto,
                                        Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built) throws Exception {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] deps = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            deps[i] = build(byName.get(proto.getDependency(i)), byName, built);
        }
        FileDescriptor linked = FileDescriptor.buildFrom(proto, deps);
        built.put(proto.getName(), linked);
        return linked;
    }

    private static DynamicMessage person(String id, String email, String ssn, long age) {
        Descriptor type = file.findMessageTypeByName("Person");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), id)
                .setField(type.findFieldByName("email"), email)
                .setField(type.findFieldByName("ssn"), ssn)
                .setField(type.findFieldByName("age"), age)
                .build();
    }

    private static Group readOne(Path dir, byte[] parquet) throws Exception {
        Path onDisk = dir.resolve("export-" + System.nanoTime() + ".parquet");
        Files.write(onDisk, parquet);
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(onDisk)).build()) {
            return reader.read();
        }
    }

    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    @Test
    void projectionKeepsOnlyTheSelectedColumns(@TempDir Path dir) throws Exception {
        Descriptor type = file.findMessageTypeByName("Person");
        byte[] parquet = ParquetEmitter.toBytes(type,
                List.of(person("p-1", "a@x.com", "111-22-3333", 30)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id", "age")));

        Group row = readOne(dir, parquet);
        assertThat(row.getType().containsField("id")).isTrue();
        assertThat(row.getType().containsField("age")).isTrue();
        assertThat(row.getType().containsField("email")).isFalse();
        assertThat(row.getType().containsField("ssn")).isFalse();
        assertThat(row.getString("id", 0)).isEqualTo("p-1");
        assertThat(row.getLong("age", 0)).isEqualTo(30L);
    }

    @Test
    void projectingOutAMiddleColumnKeepsTheRestAligned(@TempDir Path dir) throws Exception {
        // email is field 2: dropping it must not shift ssn/age into the wrong column slots.
        Descriptor type = file.findMessageTypeByName("Person");
        byte[] parquet = ParquetEmitter.toBytes(type,
                List.of(person("p-2", "b@x.com", "999-88-7777", 41)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id", "ssn", "age")));

        Group row = readOne(dir, parquet);
        assertThat(row.getType().containsField("email")).isFalse();
        assertThat(row.getString("id", 0)).isEqualTo("p-2");
        assertThat(row.getString("ssn", 0)).isEqualTo("999-88-7777");
        assertThat(row.getLong("age", 0)).isEqualTo(41L);
    }

    @Test
    void maskingRedactsSensitiveColumnsButKeepsThem(@TempDir Path dir) throws Exception {
        Descriptor type = file.findMessageTypeByName("Person");
        byte[] parquet = ParquetEmitter.toBytes(type,
                List.of(person("p-3", "c@x.com", "222-33-4444", 52)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.masking(Set.of("pii", "secret"),
                        SensitivityMasker.Strategy.REDACT, null));

        Group row = readOne(dir, parquet);
        assertThat(row.getString("email", 0)).isEqualTo("***");
        assertThat(row.getString("ssn", 0)).isEqualTo("***");
        assertThat(row.getString("id", 0)).isEqualTo("p-3");
        assertThat(row.getLong("age", 0)).isEqualTo(52L);
    }

    @Test
    void encryptMaskingReplacesTheValueWithCiphertext(@TempDir Path dir) throws Exception {
        Descriptor type = file.findMessageTypeByName("Person");
        byte[] key = "0123456789abcdef".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] parquet = ParquetEmitter.toBytes(type,
                List.of(person("p-4", "d@x.com", "555-66-7777", 60)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.masking(Set.of("pii"),
                        SensitivityMasker.Strategy.ENCRYPT, key));

        Group row = readOne(dir, parquet);
        assertThat(row.getString("email", 0)).isNotEqualTo("d@x.com").isNotEmpty();
    }

    @Test
    void removeOnlyZeroesARequiredColumnProjectOutToTrulyDropIt(@TempDir Path dir) throws Exception {
        Descriptor type = file.findMessageTypeByName("Person");
        // REMOVE clears email to its proto3 default; the required column still writes an empty
        // string, so the value is gone but the column is not.
        byte[] removed = ParquetEmitter.toBytes(type,
                List.of(person("p-5", "e@x.com", "000-11-2222", 25)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.masking(Set.of("pii"),
                        SensitivityMasker.Strategy.REMOVE, null));
        Group zeroed = readOne(dir, removed);
        assertThat(zeroed.getType().containsField("email")).isTrue();
        assertThat(zeroed.getString("email", 0)).isEmpty();

        // Projecting it out is what keeps the column out of the file entirely.
        byte[] projected = ParquetEmitter.toBytes(type,
                List.of(person("p-5", "e@x.com", "000-11-2222", 25)),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id", "ssn", "age")));
        assertThat(readOne(dir, projected).getType().containsField("email")).isFalse();
    }
}
