package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.List;

/** Bounded statement inputs, prepared before holding database locks. */
record DocumentAttemptPlanEncoding(String keys, List<String> objects, List<String> sources) {
    static final int BATCH_SIZE = 256;

    DocumentAttemptPlanEncoding {
        objects = List.copyOf(objects);
        sources = List.copyOf(sources);
    }

    static DocumentAttemptPlanEncoding prepare(DocumentPartAttemptLedger.Plan plan) {
        var objects = new ArrayList<String>();
        var batch = ListValue.newBuilder();
        for (int ordinal = 0; ordinal < plan.objects().size(); ordinal++) {
            var object = plan.objects().get(ordinal);
            // Integers are decimal strings, not Struct's double-valued numbers.
            // PostgreSQL recordset conversion parses them directly as INT/BIGINT.
            batch.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                    .putFields("ordinal", text(Integer.toString(ordinal)))
                    .putFields("part", text(Integer.toString(object.part().getNumber())))
                    .putFields("sub_key", text(object.subKey()))
                    .putFields("object_key", text(object.objectKey()))
                    .putFields("expected_size", text(Long.toString(object.size())))
                    .putFields("expected_sha256", text(object.sha256()))
                    .putFields("content_type", text(object.contentType()))));
            flushFull(batch, objects);
        }
        flush(batch, objects);
        var sources = new ArrayList<String>();
        plan.sources().entrySet().stream().sorted(java.util.Map.Entry.comparingByKey()).forEach(source -> {
            batch.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                    .putFields("source_node_id", text(source.getKey().toString()))
                    .putFields("revision", text(Long.toString(source.getValue())))));
            flushFull(batch, sources);
        });
        flush(batch, sources);
        return new DocumentAttemptPlanEncoding(DocumentKeyReservations.encode(
                plan.objects().stream().map(DocumentPartAttemptLedger.PlannedObject::objectKey).toList()), objects, sources);
    }

    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }

    private static void flushFull(ListValue.Builder batch, List<String> result) {
        if (batch.getValuesCount() == BATCH_SIZE) flush(batch, result);
    }

    private static void flush(ListValue.Builder batch, List<String> result) {
        if (batch.getValuesCount() == 0) return;
        try { result.add(JsonFormat.printer().omittingInsignificantWhitespace().print(batch)); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode document attempt plan", invalid);
        }
        batch.clear();
    }
}
