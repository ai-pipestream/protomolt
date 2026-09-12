package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.AvroDataset;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.DatasetField;
import ai.protomolt.proto.asset.v1.DatasetSchema;
import ai.protomolt.proto.asset.v1.DelimitedTable;
import ai.protomolt.proto.asset.v1.FieldKind;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.HeaderPresence;
import ai.protomolt.proto.asset.v1.NdjsonDataset;
import ai.protomolt.proto.asset.v1.ParquetDataset;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The schema bridge over delimited tables, NDJSON, and Avro containers. */
class DatasetSchemaBridgeTest {

    private final DatasetSchemaBridge bridge = new DatasetSchemaBridge();

    // ------------------------------------------------------------------
    // Delimited
    // ------------------------------------------------------------------

    @Test
    void aCsvWithAHeaderNamesItsColumnsAndInfersTheirKinds() throws IOException {
        DatasetSchema schema = schema(csv(","), """
                id,name,score,active,started
                1,Ada,9.5,true,2024-01-02
                2,Grace,8,false,2024-03-04
                """);

        assertThat(schema.getDeclaredByFormat()).isFalse();
        assertThat(schema.getRowCount()).isEqualTo(2);
        assertThat(schema.getRowsInspected()).isEqualTo(2);
        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("id", "name", "score", "active", "started");
        assertThat(schema.getFieldsList()).extracting(DatasetField::getKind)
                .containsExactly(FieldKind.FIELD_KIND_INTEGER,
                        FieldKind.FIELD_KIND_STRING,
                        FieldKind.FIELD_KIND_DOUBLE,
                        FieldKind.FIELD_KIND_BOOLEAN,
                        FieldKind.FIELD_KIND_TIMESTAMP);
        assertThat(schema.getFieldsList()).allMatch(field -> !field.getNullable());
    }

    @Test
    void integersAmongDecimalsWidenAndMixedValuesFallBackToText() throws IOException {
        DatasetSchema schema = schema(csv(","), """
                widens,mixes
                1,1
                2.5,later text
                """);

        assertThat(schema.getFields(0).getKind()).isEqualTo(FieldKind.FIELD_KIND_DOUBLE);
        assertThat(schema.getFields(1).getKind()).isEqualTo(FieldKind.FIELD_KIND_STRING);
    }

    @Test
    void aBlankValueMakesItsColumnNullable() throws IOException {
        DatasetSchema schema = schema(csv(","), """
                always,sometimes
                1,7
                2,
                """);

        assertThat(schema.getFields(0).getNullable()).isFalse();
        assertThat(schema.getFields(1).getNullable()).isTrue();
        // A blank is the absence of evidence, so the column keeps the kind
        // the other rows support.
        assertThat(schema.getFields(1).getKind()).isEqualTo(FieldKind.FIELD_KIND_INTEGER);
    }

    @Test
    void aHeaderlessTableNumbersItsColumns() throws IOException {
        DatasetSchema schema = schema(FormatFact.newBuilder()
                .setDelimited(DelimitedTable.newBuilder()
                        .setFilename("rows.csv").setDelimiter(",")
                        .setHeader(HeaderPresence.HEADER_PRESENCE_ABSENT)).build(),
                "1,Ada\n2,Grace\n");

        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("column_1", "column_2");
        assertThat(schema.getRowCount()).isEqualTo(2);
    }

    @Test
    void theDelimiterIsTheProducersNotAGuess() throws IOException {
        DatasetSchema schema = schema(csv("|"), "a|b\n1|2\n");

        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("a", "b");
    }

    @Test
    void quotedFieldsHoldDelimitersNewlinesAndDoubledQuotes() throws IOException {
        DatasetSchema schema = schema(csv(","),
                "id,note\n1,\"holds, a comma\nand a newline and \"\"quotes\"\"\"\n2,plain\n");

        assertThat(schema.getRowCount()).isEqualTo(2);
        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("id", "note");
        assertThat(schema.getFields(1).getKind()).isEqualTo(FieldKind.FIELD_KIND_STRING);
    }

    @Test
    void aShortRowLeavesTheRemainingColumnsNullable() throws IOException {
        DatasetSchema schema = schema(csv(","), "a,b,c\n1,2,3\n4,5\n");

        assertThat(schema.getFields(2).getNullable()).isTrue();
        assertThat(schema.getFields(0).getNullable()).isFalse();
    }

    @Test
    void aBoundedInspectionReportsWhatItSawAndAdmitsTheRowCountIsUnknown()
            throws IOException {
        StringBuilder rows = new StringBuilder("n\n");
        for (int i = 0; i < 50; i++) {
            rows.append(i).append('\n');
        }
        Bridge.Derivation derivation = new DatasetSchemaBridge(10).derive(
                new ByteArrayInputStream(rows.toString().getBytes(StandardCharsets.UTF_8)),
                new Bridge.Context(csv(","), "rows.csv"));
        DatasetSchema schema = DatasetSchema.parseFrom(derivation.content());

        assertThat(schema.getRowsInspected()).isEqualTo(10);
        assertThat(schema.getRowCount()).isEqualTo(-1);
        assertThat(derivation.degraded()).isTrue();
        assertThat(derivation.warnings()).containsExactly(
                "schema inferred from the first 10 rows; the row count is unknown");
    }

    @Test
    void anUnterminatedQuotedFieldFailsRatherThanGuessing() {
        assertThatThrownBy(() -> schema(csv(","), "a,b\n1,\"never closed\n"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("never closes");
    }

    // ------------------------------------------------------------------
    // NDJSON
    // ------------------------------------------------------------------

    @Test
    void ndjsonTakesItsKindsFromTheValuesThemselves() throws IOException {
        DatasetSchema schema = schema(ndjson(), """
                {"id":1,"name":"Ada","ratio":0.5,"ok":true,"tags":["x"],"meta":{"a":1}}
                {"id":2,"name":"Grace","ratio":1.5,"ok":false,"tags":[],"meta":{}}
                """);

        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("id", "name", "ratio", "ok", "tags", "meta");
        assertThat(schema.getFieldsList()).extracting(DatasetField::getKind)
                .containsExactly(FieldKind.FIELD_KIND_INTEGER,
                        FieldKind.FIELD_KIND_STRING,
                        FieldKind.FIELD_KIND_DOUBLE,
                        FieldKind.FIELD_KIND_BOOLEAN,
                        FieldKind.FIELD_KIND_LIST,
                        FieldKind.FIELD_KIND_RECORD);
        assertThat(schema.getRowCount()).isEqualTo(2);
    }

    @Test
    void aFieldMissingFromSomeLineOrNullInOneIsNullable() throws IOException {
        DatasetSchema schema = schema(ndjson(), """
                {"always":1,"sometimes":2,"nulled":3}
                {"always":4,"nulled":null}
                """);

        assertThat(field(schema, "always").getNullable()).isFalse();
        assertThat(field(schema, "sometimes").getNullable()).isTrue();
        assertThat(field(schema, "nulled").getNullable()).isTrue();
        assertThat(field(schema, "nulled").getKind()).isEqualTo(FieldKind.FIELD_KIND_INTEGER);
    }

    @Test
    void aLineThatIsNotAnObjectFailsByLineNumber() {
        assertThatThrownBy(() -> schema(ndjson(), "{\"a\":1}\n[1,2,3]\n"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("line 2")
                .hasMessageContaining("not a JSON object");
    }

    @Test
    void malformedJsonFailsNamingTheOffset() {
        assertThatThrownBy(() -> schema(ndjson(), "{\"a\":}\n"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("malformed JSON at offset");
    }

    // ------------------------------------------------------------------
    // Avro
    // ------------------------------------------------------------------

    @Test
    void anAvroContainerDeclaresItsSchemaAndNothingIsInferred() throws IOException {
        byte[] container = avroContainer("""
                {"type":"record","name":"Row","fields":[
                  {"name":"id","type":"long"},
                  {"name":"name","type":["null","string"]},
                  {"name":"when","type":{"type":"long","logicalType":"timestamp-millis"}},
                  {"name":"blob","type":"bytes"},
                  {"name":"tags","type":{"type":"array","items":"string"}}
                ]}""");

        DatasetSchema schema = schema(FormatFact.newBuilder()
                .setAvro(AvroDataset.newBuilder().setFilename("rows.avro")).build(),
                container);

        assertThat(schema.getDeclaredByFormat()).isTrue();
        assertThat(schema.getRowsInspected()).isZero();
        // The header states no row count, and the blocks were not read.
        assertThat(schema.getRowCount()).isEqualTo(-1);
        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("id", "name", "when", "blob", "tags");
        assertThat(schema.getFieldsList()).extracting(DatasetField::getKind)
                .containsExactly(FieldKind.FIELD_KIND_INTEGER,
                        FieldKind.FIELD_KIND_STRING,
                        FieldKind.FIELD_KIND_INTEGER,
                        FieldKind.FIELD_KIND_BYTES,
                        FieldKind.FIELD_KIND_LIST);
        assertThat(field(schema, "name").getNullable()).isTrue();
        assertThat(field(schema, "id").getNullable()).isFalse();
        // The writer's own type name survives verbatim for a reader that
        // speaks Avro.
        assertThat(field(schema, "name").getSourceType()).isEqualTo("null|string");
        assertThat(field(schema, "when").getSourceType()).isEqualTo("timestamp-millis");
    }

    @Test
    void bytesThatAreNoAvroContainerFailByName() {
        assertThatThrownBy(() -> schema(FormatFact.newBuilder()
                .setAvro(AvroDataset.newBuilder().setFilename("rows.avro")).build(),
                "not avro at all".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not an Avro object container");
    }

    // ------------------------------------------------------------------
    // Reach
    // ------------------------------------------------------------------

    @Test
    void parquetIsNotHandledHereSoItDefersRatherThanFailing() {
        FormatFact parquet = FormatFact.newBuilder()
                .setParquet(ParquetDataset.newBuilder().setFilename("f.parquet")).build();

        assertThat(bridge.handles(parquet)).isFalse();
        assertThat(BridgeEngine.standard().forKind(
                BridgeKind.BRIDGE_KIND_DATASET_SCHEMA, parquet))
                .isEmpty();
        assertThat(Bridges.deferralReason(
                BridgeKind.BRIDGE_KIND_DATASET_SCHEMA, parquet))
                .contains("Parquet reader");
    }

    // ------------------------------------------------------------------

    private DatasetSchema schema(FormatFact format, String content) throws IOException {
        return schema(format, content.getBytes(StandardCharsets.UTF_8));
    }

    private DatasetSchema schema(FormatFact format, byte[] content) throws IOException {
        return DatasetSchema.parseFrom(bridge.derive(new ByteArrayInputStream(content),
                new Bridge.Context(format, "dataset")).content());
    }

    private static DatasetField field(DatasetSchema schema, String name) {
        return schema.getFieldsList().stream()
                .filter(item -> item.getName().equals(name))
                .findFirst().orElseThrow(() -> new AssertionError("no field '" + name + "'"));
    }

    private static FormatFact csv(String delimiter) {
        return FormatFact.newBuilder().setDelimited(DelimitedTable.newBuilder()
                .setFilename("rows.csv")
                .setDelimiter(delimiter)
                .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT)).build();
    }

    private static FormatFact ndjson() {
        return FormatFact.newBuilder()
                .setNdjson(NdjsonDataset.newBuilder().setFilename("rows.ndjson")).build();
    }

    /** An Avro object-container header carrying one metadata block. */
    private static byte[] avroContainer(String writerSchema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] {'O', 'b', 'j', 0x01});
        writeLong(out, 2);                                   // two metadata entries
        writeString(out, "avro.codec");
        writeString(out, "null");
        writeString(out, "avro.schema");
        writeString(out, writerSchema);
        writeLong(out, 0);                                   // end of the map
        out.writeBytes(new byte[16]);                        // sync marker
        return out.toByteArray();
    }

    private static void writeString(ByteArrayOutputStream out, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeLong(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        long zigzag = (value << 1) ^ (value >> 63);
        while ((zigzag & ~0x7FL) != 0) {
            out.write((int) ((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        out.write((int) zigzag);
    }
}
