package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.meta.FieldMeta;
import ai.pipestream.proto.meta.MetadataProto;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Building a {@link PartitionSpec} from declared partition columns: transforms resolve by column
 * name (so the spec survives a catalog's fresh field ids), widths parse, and an ill-typed or
 * unknown transform fails at bind time rather than at write time.
 */
class IcebergPartitionsTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "symbol", Types.StringType.get()),
            Types.NestedField.optional(2, "at", Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "price", Types.DoubleType.get()));

    @Test
    void bindResolvesTransformsByColumnName() {
        PartitionSpec spec = IcebergPartitions.bind(SCHEMA, List.of(
                new IcebergPartitions.PartitionField("at", "day"),
                new IcebergPartitions.PartitionField("symbol", "identity"),
                new IcebergPartitions.PartitionField("symbol", "bucket[16]")));
        assertThat(spec.isPartitioned()).isTrue();
        assertThat(spec.fields()).extracting(f -> f.transform().toString())
                .containsExactly("day", "identity", "bucket[16]");
        // The source id resolves to the schema field of that name.
        assertThat(spec.fields().get(0).sourceId()).isEqualTo(SCHEMA.findField("at").fieldId());
    }

    @Test
    void truncateWidthIsParsed() {
        PartitionSpec spec = IcebergPartitions.bind(SCHEMA,
                List.of(new IcebergPartitions.PartitionField("symbol", "truncate[4]")));
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("truncate[4]");
    }

    @Test
    void anUnknownTransformIsRejected() {
        assertThatThrownBy(() -> IcebergPartitions.bind(SCHEMA,
                List.of(new IcebergPartitions.PartitionField("symbol", "sha256"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported partition transform");
    }

    @Test
    void aBucketWithoutAWidthIsRejected() {
        assertThatThrownBy(() -> IcebergPartitions.bind(SCHEMA,
                List.of(new IcebergPartitions.PartitionField("symbol", "bucket"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("width");
    }

    @Test
    void fromHintsReadsThePartitionLabelDeclaredOnFields() throws Exception {
        // The descriptor is built directly with the metadata extension set: ProtoMolt's own
        // compiler cannot yet parse a map-typed custom option in .proto text, so a partition
        // label declared inline would need protoc. fromHints itself reads it either way.
        Descriptor row = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("p/row.proto").setPackage("p").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Row")
                        .addField(labelled("region", 1, "identity"))
                        .addField(labelled("at", 2, "day"))
                        .addField(FieldDescriptorProto.newBuilder().setName("price").setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build(), new FileDescriptor[0]).findMessageTypeByName("Row");

        // In field order; the unlabelled column is skipped.
        assertThat(IcebergPartitions.fromHints(row)).containsExactly(
                new IcebergPartitions.PartitionField("region", "identity"),
                new IcebergPartitions.PartitionField("at", "day"));
    }

    private static FieldDescriptorProto labelled(String name, int number, String transform) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(FieldOptions.newBuilder().setExtension(MetadataProto.field,
                        FieldMeta.newBuilder()
                                .putLabels(IcebergPartitions.PARTITION_HINT, transform).build()))
                .build();
    }

    /**
     * uint32 widens to a long column and the Parquet emitter writes it unsigned. Sign-extending
     * the partition value instead would file a row whose own column reads 4294967295 under the
     * partition for -1, so the partition would not match its data.
     */
    @Test
    void unsignedInt32PartitionValuesWidenTheSameWayTheirColumnDoes() throws Exception {
        Descriptor shard = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("p/shard.proto", """
                                syntax = "proto3";
                                package p;
                                message Shard { uint32 bucket = 1; }
                                """, "test").build())
                .descriptorFor("p/shard.proto").orElseThrow().findMessageTypeByName("Shard");
        Schema schema = IcebergSchemas.fromDescriptor(shard);
        assertThat(schema.columns().get(0).type()).isInstanceOf(Types.LongType.class);

        // uint32 0xFFFFFFFF arrives from protobuf as the int bit pattern -1.
        DynamicMessage message = DynamicMessage.newBuilder(shard)
                .setField(shard.findFieldByName("bucket"), -1).build();

        assertThat(IcebergPartitions.row(message, schema).get(0, Long.class))
                .isEqualTo(4294967295L);
    }

    @Test
    void aTransformThatDoesNotFitItsSourceFailsAtBind() {
        // day() needs a timestamp source; applying it to a string must fail here, not at write.
        assertThatThrownBy(() -> IcebergPartitions.bind(SCHEMA,
                List.of(new IcebergPartitions.PartitionField("symbol", "day"))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("source type");
    }
}
