package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.DatasetField;
import ai.protomolt.proto.asset.v1.DatasetSchema;
import ai.protomolt.proto.asset.v1.DelimitedTable;
import ai.protomolt.proto.asset.v1.FieldKind;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.HeaderPresence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The schema bridge: a dataset in, its columns out.
 *
 * <p>Two sources of truth, told apart in the output. Avro DECLARES its
 * schema in the container header, so the bridge reads it and marks the
 * result {@code declared_by_format}. Delimited tables and NDJSON declare
 * nothing, so the bridge INFERS from a bounded prefix of the rows and
 * reports how many it inspected. A consumer can then tell a schema the
 * format stated from one a bridge concluded, and treat them differently.
 *
 * <p>Inference widens only: a column of integers that later shows a decimal
 * reads as double, and one that shows text reads as string. Order of rows
 * cannot change the answer.
 */
public final class DatasetSchemaBridge implements Bridge {

    /** Rows inspected before inference stops. */
    public static final int DEFAULT_MAX_ROWS = 1_000;

    private final int maxRows;

    /** A bridge with the default inspection bound. */
    public DatasetSchemaBridge() {
        this(DEFAULT_MAX_ROWS);
    }

    /**
     * A bridge with an explicit inspection bound.
     *
     * @param maxRows rows inspected before inference stops
     */
    public DatasetSchemaBridge(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public BridgeKind kind() {
        return BridgeKind.BRIDGE_KIND_DATASET_SCHEMA;
    }

    @Override
    public boolean handles(FormatFact format) {
        return switch (format.getFormatCase()) {
            case DELIMITED, NDJSON, AVRO -> true;
            default -> false;
        };
    }

    @Override
    public Derivation derive(InputStream original, Context context) throws IOException {
        return switch (context.format().getFormatCase()) {
            case DELIMITED -> delimited(original, context.format().getDelimited());
            case NDJSON -> ndjson(original);
            case AVRO -> avro(original);
            default -> throw new IOException("the schema bridge reads no "
                    + context.format().getFormatCase().name().toLowerCase() + " dataset");
        };
    }

    // ------------------------------------------------------------------
    // Delimited: no declared schema, so every column is inferred from text.
    // ------------------------------------------------------------------

    private Derivation delimited(InputStream original, DelimitedTable table) throws IOException {
        char delimiter = table.getDelimiter().charAt(0);
        DelimitedReader reader = new DelimitedReader(
                new BufferedReader(new InputStreamReader(original, StandardCharsets.UTF_8)),
                delimiter);
        List<String> names = new ArrayList<>();
        if (table.getHeader() == HeaderPresence.HEADER_PRESENCE_PRESENT) {
            List<String> header = reader.next();
            if (header == null) {
                return empty("the table holds no header row to name its columns");
            }
            names.addAll(header);
        }

        List<FieldKind> kinds = new ArrayList<>();
        List<Boolean> nullable = new ArrayList<>();
        long inspected = 0;
        boolean exhausted = true;
        for (List<String> row; (row = reader.next()) != null; ) {
            if (inspected >= maxRows) {
                exhausted = false;
                break;
            }
            inspected++;
            for (int column = 0; column < row.size(); column++) {
                while (kinds.size() <= column) {
                    kinds.add(FieldKind.FIELD_KIND_UNSPECIFIED);
                    nullable.add(false);
                }
                FieldKind kind = FieldKinds.ofText(row.get(column));
                kinds.set(column, FieldKinds.merge(kinds.get(column), kind));
                if (kind == FieldKind.FIELD_KIND_UNSPECIFIED) {
                    nullable.set(column, true);
                }
            }
            // A row with fewer fields than the widest one leaves the rest
            // missing, which is the same fact as a blank value.
            for (int column = row.size(); column < kinds.size(); column++) {
                nullable.set(column, true);
            }
        }

        DatasetSchema.Builder schema = DatasetSchema.newBuilder()
                .setRowCount(exhausted ? inspected : -1)
                .setRowsInspected(inspected);
        int columns = Math.max(names.size(), kinds.size());
        for (int column = 0; column < columns; column++) {
            FieldKind kind = column < kinds.size()
                    ? kinds.get(column) : FieldKind.FIELD_KIND_UNSPECIFIED;
            schema.addFields(DatasetField.newBuilder()
                    .setName(column < names.size() && !names.get(column).isBlank()
                            ? names.get(column) : "column_" + (column + 1))
                    // A column whose every inspected value was blank has no
                    // evidence for a kind; text is what it is on the wire.
                    .setKind(kind == FieldKind.FIELD_KIND_UNSPECIFIED
                            ? FieldKind.FIELD_KIND_STRING : kind)
                    .setNullable(column >= nullable.size() || nullable.get(column)));
        }
        return derivation(schema.build(), exhausted, inspected);
    }

    // ------------------------------------------------------------------
    // NDJSON: the values state their own types, the document states no
    // column set, so the union of the inspected lines is the schema.
    // ------------------------------------------------------------------

    private Derivation ndjson(InputStream original) throws IOException {
        Map<String, FieldKind> kinds = new LinkedHashMap<>();
        Map<String, Long> present = new LinkedHashMap<>();
        long inspected = 0;
        boolean exhausted = true;
        try (BufferedReader lines = new BufferedReader(
                new InputStreamReader(original, StandardCharsets.UTF_8))) {
            for (String line; (line = lines.readLine()) != null; ) {
                if (line.isBlank()) {
                    continue;
                }
                if (inspected >= maxRows) {
                    exhausted = false;
                    break;
                }
                inspected++;
                Object value = Json.read(line);
                if (!(value instanceof Map<?, ?> record)) {
                    throw new IOException("line " + inspected
                            + " of the NDJSON dataset is not a JSON object");
                }
                for (Map.Entry<?, ?> member : record.entrySet()) {
                    String name = String.valueOf(member.getKey());
                    FieldKind kind = FieldKinds.ofJson(member.getValue());
                    kinds.merge(name, kind, FieldKinds::merge);
                    if (member.getValue() != null) {
                        present.merge(name, 1L, Long::sum);
                    }
                }
            }
        }

        DatasetSchema.Builder schema = DatasetSchema.newBuilder()
                .setRowCount(exhausted ? inspected : -1)
                .setRowsInspected(inspected);
        for (Map.Entry<String, FieldKind> field : kinds.entrySet()) {
            FieldKind kind = field.getValue();
            schema.addFields(DatasetField.newBuilder()
                    .setName(field.getKey())
                    .setKind(kind == FieldKind.FIELD_KIND_UNSPECIFIED
                            ? FieldKind.FIELD_KIND_STRING : kind)
                    // Absent from a line, or null in one: both mean the
                    // column admits a missing value.
                    .setNullable(present.getOrDefault(field.getKey(), 0L) < inspected));
        }
        return derivation(schema.build(), exhausted, inspected);
    }

    // ------------------------------------------------------------------
    // Avro: the container declares the writer's schema. Nothing is inferred.
    // ------------------------------------------------------------------

    private Derivation avro(InputStream original) throws IOException {
        String json = AvroHeader.metadata(original).get(AvroHeader.SCHEMA_KEY);
        if (json == null) {
            throw new IOException("the Avro container header declares no "
                    + AvroHeader.SCHEMA_KEY);
        }
        if (!(Json.read(json) instanceof Map<?, ?> declared)) {
            throw new IOException("the Avro writer schema is not a JSON object");
        }
        if (!(declared.get("fields") instanceof List<?> fields)) {
            // A container whose top-level schema is not a record has no
            // columns to report, and inventing one would be a lie.
            throw new IOException("the Avro writer schema declares no record fields");
        }
        DatasetSchema.Builder schema = DatasetSchema.newBuilder()
                .setDeclaredByFormat(true)
                // The header states no row count and the blocks were not
                // read; unknown is honest, zero would not be.
                .setRowCount(-1);
        for (Object item : fields) {
            if (!(item instanceof Map<?, ?> field)) {
                throw new IOException("an Avro record field is not a JSON object");
            }
            Object type = field.get("type");
            schema.addFields(DatasetField.newBuilder()
                    .setName(String.valueOf(field.get("name")))
                    .setKind(avroKind(type))
                    .setNullable(avroNullable(type))
                    .setSourceType(avroSourceType(type)));
        }
        return Derivation.of(schema.build());
    }

    /** An Avro type is a name, a union, or a nested definition. */
    private static FieldKind avroKind(Object type) {
        if (type instanceof List<?> union) {
            for (Object member : union) {
                if (!"null".equals(member)) {
                    return avroKind(member);
                }
            }
            return FieldKind.FIELD_KIND_UNSPECIFIED;
        }
        if (type instanceof Map<?, ?> nested) {
            return avroKind(nested.get("type"));
        }
        return switch (String.valueOf(type)) {
            case "string", "enum", "fixed" -> FieldKind.FIELD_KIND_STRING;
            case "int", "long" -> FieldKind.FIELD_KIND_INTEGER;
            case "float", "double" -> FieldKind.FIELD_KIND_DOUBLE;
            case "boolean" -> FieldKind.FIELD_KIND_BOOLEAN;
            case "bytes" -> FieldKind.FIELD_KIND_BYTES;
            case "record", "map" -> FieldKind.FIELD_KIND_RECORD;
            case "array" -> FieldKind.FIELD_KIND_LIST;
            default -> FieldKind.FIELD_KIND_UNSPECIFIED;
        };
    }

    /** Avro spells optionality as a union with "null". */
    private static boolean avroNullable(Object type) {
        return type instanceof List<?> union && union.contains("null");
    }

    /** The writer's own type name, verbatim, for a reader that speaks Avro. */
    private static String avroSourceType(Object type) {
        if (type instanceof List<?> union) {
            return union.stream().map(DatasetSchemaBridge::avroSourceType)
                    .reduce((left, right) -> left + "|" + right).orElse("");
        }
        if (type instanceof Map<?, ?> nested) {
            Object logical = nested.get("logicalType");
            return logical != null ? String.valueOf(logical)
                    : String.valueOf(nested.get("type"));
        }
        return String.valueOf(type);
    }

    // ------------------------------------------------------------------

    private static Derivation derivation(DatasetSchema schema, boolean exhausted,
                                         long inspected) {
        if (exhausted) {
            return Derivation.of(schema);
        }
        return new Derivation(schema.toByteArray(), null, List.of(
                "schema inferred from the first " + inspected
                        + " rows; the row count is unknown"));
    }

    private static Derivation empty(String why) {
        return new Derivation(
                DatasetSchema.newBuilder().setRowCount(0).build().toByteArray(), null,
                List.of(why));
    }
}
