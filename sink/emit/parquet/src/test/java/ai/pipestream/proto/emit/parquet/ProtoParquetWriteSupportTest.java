package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The record-writing half of the module, round-tripped through the emitter: repeated
 * <em>message</em> fields, message-valued maps (including an entry whose value was never
 * set), the JSON well-known types beyond {@code Struct}, and timestamps before the epoch —
 * the shapes {@code ParquetEmitterTest}'s flat row does not exercise — plus the write
 * support's own name, schema, and footer metadata.
 */
class ProtoParquetWriteSupportTest {

    private static final String PROTO = """
            syntax = "proto3";
            package pq.write;
            import "google/protobuf/timestamp.proto";
            import "google/protobuf/struct.proto";
            message Envelope {
              string id = 1;
              repeated Item items = 2;
              map<string, Item> indexed = 3;
              google.protobuf.Value anything = 4;
              google.protobuf.ListValue bag = 5;
              google.protobuf.Timestamp at = 6;
            }
            message Item { string name = 1; int64 qty = 2; }
            """;

    private static FileDescriptor file() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pq/write/envelope.proto", PROTO, "test").build());
        return compiled.descriptorFor("pq/write/envelope.proto").orElseThrow();
    }

    private static Message jsonValue(Descriptor holder, String field, String json)
            throws Exception {
        Descriptor type = holder.findFieldByName(field).getMessageType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        com.google.protobuf.util.JsonFormat.parser().merge(json, builder);
        return builder.build();
    }

    private static Message timestamp(Descriptor envelope, long seconds, int nanos) {
        Descriptor type = envelope.findFieldByName("at").getMessageType();
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("seconds"), seconds)
                .setField(type.findFieldByName("nanos"), nanos)
                .build();
    }

    /** ParquetReader over a plain InputFile — no Hadoop filesystem anywhere in the test. */
    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    private static Group readOne(Path dir, byte[] parquet) throws Exception {
        Path onDisk = dir.resolve("write-" + System.nanoTime() + ".parquet");
        Files.write(onDisk, parquet);
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(onDisk)).build()) {
            return reader.read();
        }
    }

    @Test
    void repeatedMessagesRoundTripAsListsOfGroups(@TempDir Path dir) throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        Descriptor item = file().findMessageTypeByName("Item");
        DynamicMessage envelope = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), "e-1")
                .addRepeatedField(type.findFieldByName("items"), DynamicMessage.newBuilder(item)
                        .setField(item.findFieldByName("name"), "bolt")
                        .setField(item.findFieldByName("qty"), 4L).build())
                .addRepeatedField(type.findFieldByName("items"), DynamicMessage.newBuilder(item)
                        .setField(item.findFieldByName("name"), "nut")
                        .setField(item.findFieldByName("qty"), 9L).build())
                .build();

        Group row = readOne(dir, ParquetEmitter.toBytes(type, List.of(envelope)));
        Group items = row.getGroup("items", 0);
        assertThat(items.getFieldRepetitionCount("list")).isEqualTo(2);
        Group second = items.getGroup("list", 1).getGroup("element", 0);
        assertThat(second.getString("name", 0)).isEqualTo("nut");
        assertThat(second.getLong("qty", 0)).isEqualTo(9L);
    }

    @Test
    void messageValuedMapsRoundTripAndUnsetValuesWriteTheProto3Default(@TempDir Path dir)
            throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        Descriptor item = file().findMessageTypeByName("Item");
        Descriptor entry = type.findFieldByName("indexed").getMessageType();
        DynamicMessage envelope = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), "e-2")
                .addRepeatedField(type.findFieldByName("indexed"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "with")
                        .setField(entry.findFieldByName("value"), DynamicMessage.newBuilder(item)
                                .setField(item.findFieldByName("name"), "bolt")
                                .setField(item.findFieldByName("qty"), 1L).build())
                        .build())
                // An entry built without its message value: once the envelope is built, the
                // entry is a MapEntry, and proto3 map values are always present — so the
                // writer emits the default Item, exactly as a parsed message would.
                .addRepeatedField(type.findFieldByName("indexed"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "without")
                        .build())
                .build();

        Group row = readOne(dir, ParquetEmitter.toBytes(type, List.of(envelope)));
        Group indexed = row.getGroup("indexed", 0);
        assertThat(indexed.getFieldRepetitionCount("key_value")).isEqualTo(2);
        Group with = indexed.getGroup("key_value", 0);
        assertThat(with.getString("key", 0)).isEqualTo("with");
        assertThat(with.getGroup("value", 0).getString("name", 0)).isEqualTo("bolt");
        Group without = indexed.getGroup("key_value", 1);
        assertThat(without.getString("key", 0)).isEqualTo("without");
        Group defaultValue = without.getGroup("value", 0);
        assertThat(defaultValue.getString("name", 0)).isEmpty();
        assertThat(defaultValue.getLong("qty", 0)).isZero();
    }

    @Test
    void valueAndListValueWriteAsJsonStrings(@TempDir Path dir) throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        DynamicMessage envelope = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), "e-3")
                .setField(type.findFieldByName("anything"),
                        jsonValue(type, "anything", "\"loose\""))
                .setField(type.findFieldByName("bag"),
                        jsonValue(type, "bag", "[1, \"two\", null]"))
                .build();

        Group row = readOne(dir, ParquetEmitter.toBytes(type, List.of(envelope)));
        assertThat(row.getBinary("anything", 0).toStringUsingUTF8()).isEqualTo("\"loose\"");
        assertThat(row.getBinary("bag", 0).toStringUsingUTF8())
                .contains("\"two\"").contains("null");
    }

    @Test
    void preEpochTimestampsKeepTheirSign(@TempDir Path dir) throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        DynamicMessage envelope = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), "e-4")
                .setField(type.findFieldByName("at"), timestamp(type, -1L, 500_000_000))
                .build();

        Group row = readOne(dir, ParquetEmitter.toBytes(type, List.of(envelope)));
        // -1s + 500ms is half a second before the epoch: -500_000 microseconds.
        assertThat(row.getLong("at", 0)).isEqualTo(-500_000L);
    }

    @Test
    void zeroMessagesStillProduceAReadableFile(@TempDir Path dir) throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        byte[] parquet = ParquetEmitter.toBytes(type, List.of());
        assertThat(new String(parquet, 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
                .isEqualTo("PAR1");

        Path onDisk = dir.resolve("empty.parquet");
        Files.write(onDisk, parquet);
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(onDisk)).build()) {
            assertThat(reader.read()).isNull();
        }
    }

    @Test
    void theWriteSupportNamesItselfAndStampsTheFooter() throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        ProtoParquetWriteSupport support = new ProtoParquetWriteSupport(type);
        assertThat(support.getName()).isEqualTo("protomolt");

        // Both init paths — the Hadoop-free one the emitter actually uses and the classic
        // one — hand the writer the same schema and the same footer metadata.
        WriteContext context = support.init(new org.apache.parquet.conf.PlainParquetConfiguration());
        assertThat(context.getSchema()).isEqualTo(ProtoParquetSchemas.schema(type));
        assertThat(context.getExtraMetaData())
                .containsEntry("protomolt.proto.message", "pq.write.Envelope");

        WriteContext hadoopContext =
                support.init(new org.apache.hadoop.conf.Configuration(false));
        assertThat(hadoopContext.getSchema()).isEqualTo(context.getSchema());
        assertThat(hadoopContext.getExtraMetaData()).isEqualTo(context.getExtraMetaData());
    }

    @Test
    void nullAndDefaultConstructorArgumentsAreHandled() throws Exception {
        Descriptor type = file().findMessageTypeByName("Envelope");
        org.assertj.core.api.Assertions.assertThatNullPointerException()
                .isThrownBy(() -> new ProtoParquetWriteSupport(null));
        // A null projection is the same as an empty one: the full schema comes out.
        ProtoParquetWriteSupport support = new ProtoParquetWriteSupport(
                type, ProtoParquetSchemas.FieldIdResolver.NONE, null);
        WriteContext context = support.init(new org.apache.parquet.conf.PlainParquetConfiguration());
        assertThat(context.getSchema().getFields()).hasSize(type.getFields().size());
    }
}
