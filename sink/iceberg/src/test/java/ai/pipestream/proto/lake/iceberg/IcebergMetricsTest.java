package ai.pipestream.proto.lake.iceberg;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The footer-level metrics read: emitter-produced Parquet bytes yield real record and value
 * counts (and per-column bounds) without Hadoop, and bytes whose footer trailer no longer
 * describes a readable footer fail as an {@link UncheckedIOException} instead of returning
 * plausible-looking garbage metrics. The end-to-end suites prove the metrics land on the
 * committed data file; this pins the read itself, corrupt input included.
 */
class IcebergMetricsTest {

    private static Descriptor rowDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("m/row.proto").setPackage("m").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Row")
                        .addField(FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder().setName("num").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Row");
    }

    private static byte[] parquetOf(Descriptor row, int messages) throws Exception {
        List<DynamicMessage> batch = new java.util.ArrayList<>();
        for (int i = 0; i < messages; i++) {
            batch.add(DynamicMessage.newBuilder(row)
                    .setField(row.findFieldByName("name"), "n" + i)
                    .setField(row.findFieldByName("num"), (long) i)
                    .build());
        }
        return ai.pipestream.proto.emit.parquet.ParquetEmitter.toBytes(row, batch);
    }

    @Test
    void theFooterYieldsRecordAndValueCountsForEveryColumn() throws Exception {
        Descriptor row = rowDescriptor();
        Metrics metrics = IcebergMetrics.forParquet(
                parquetOf(row, 3), MetricsConfig.getDefault());

        assertThat(metrics.recordCount()).isEqualTo(3L);
        // Field ids here are the emitter's own (no table to stamp them), so assert by shape:
        // both columns counted every row, and bounds exist for pruning.
        assertThat(metrics.valueCounts()).hasSize(2).allSatisfy((id, count) ->
                assertThat(count).isEqualTo(3L));
        assertThat(metrics.lowerBounds()).isNotEmpty();
        assertThat(metrics.upperBounds()).isNotEmpty();
    }

    @Test
    void aSingleRowStillProducesMetrics() throws Exception {
        Metrics metrics = IcebergMetrics.forParquet(
                parquetOf(rowDescriptor(), 1), MetricsConfig.getDefault());
        assertThat(metrics.recordCount()).isEqualTo(1L);
        assertThat(metrics.valueCounts()).allSatisfy((id, count) ->
                assertThat(count).isEqualTo(1L));
    }

    @Test
    void aFooterTrailerThatPointsAtNothingFailsLoudly() throws Exception {
        byte[] parquet = parquetOf(rowDescriptor(), 2);
        // Zero the 4-byte footer length in the trailer: the metadata slice is then empty and
        // the Thrift read must fail, not produce a zeroed-out Metrics.
        parquet[parquet.length - 8] = 0;
        parquet[parquet.length - 7] = 0;
        parquet[parquet.length - 6] = 0;
        parquet[parquet.length - 5] = 0;

        assertThatThrownBy(() -> IcebergMetrics.forParquet(parquet, MetricsConfig.getDefault()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("footer");
    }
}
