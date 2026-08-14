package ai.pipestream.proto.kafka.connect.opensearch;

import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.validate.FieldRules;
import ai.pipestream.proto.validate.StringRules;
import ai.pipestream.proto.validate.ValidateProto;
import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexFieldType;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenSearchSinkTaskTest {

    @Test
    void hintsInTheDescriptorSetShapeTheDocuments() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        task.put(List.of(record(doc("d1", "Hello", 3).toByteArray(), 7)));

        assertThat(client.writes).hasSize(1);
        BulkWrite write = client.writes.get(0);
        assertThat(write.index).isEqualTo("docs");
        Map<String, Object> document = write.documents.get("orders-0-7");
        assertThat(document)
                .containsEntry("doc_id", "d1")
                .containsEntry("headline", "Hello")
                .containsEntry("pages", 3);
    }

    @Test
    void documentIdPathReadsTheIdFromTheMessage() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        task.put(List.of(record(doc("d1", "Hello", 3).toByteArray(), 7)));

        assertThat(client.writes.get(0).documents).containsOnlyKeys("d1");
    }

    @Test
    void unsetDocumentIdPathIsADataError() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        assertThatThrownBy(() -> task.put(List.of(record(doc("", "Hello", 3).toByteArray(), 7))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("doc_id");
    }

    @Test
    void ensureIndexRunsAtStartWithThePlanAndCanBeTurnedOff() throws Exception {
        RecordingClient client = new RecordingClient();
        startedTask(client, Map.of());
        assertThat(client.ensured).hasSize(1);
        assertThat(client.ensured.get(0).plan.find("title")).isPresent();

        RecordingClient quiet = new RecordingClient();
        startedTask(quiet, Map.of(OpenSearchSinkConfig.ENSURE_INDEX, "false"));
        assertThat(quiet.ensured).isEmpty();
    }

    @Test
    void registeredAnyPayloadsExpandIntoTheDocument() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());
        Fixture fixture = Fixture.create();
        DynamicMessage inner = DynamicMessage.newBuilder(fixture.inner)
                .setField(fixture.inner.findFieldByName("label"), "packed")
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixture.doc)
                .setField(fixture.doc.findFieldByName("doc_id"), "d1")
                .setField(fixture.doc.findFieldByName("title"), "Hello")
                .setField(fixture.doc.findFieldByName("payload"),
                        Any.pack(inner, "type.googleapis.com/"))
                .build();

        task.put(List.of(record(message.toByteArray(), 1)));

        assertThat(client.writes.get(0).documents.values().iterator().next())
                .containsEntry("payload_label", "packed");
    }

    @Test
    void unknownAnyTypeUrlsAreDataErrorsNotConnectorFailures() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());
        Fixture fixture = Fixture.create();
        DynamicMessage message = DynamicMessage.newBuilder(fixture.doc)
                .setField(fixture.doc.findFieldByName("doc_id"), "d1")
                .setField(fixture.doc.findFieldByName("title"), "Hello")
                .setField(fixture.doc.findFieldByName("payload"), Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/connect.test.Missing")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> task.put(List.of(record(message.toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("connect.test.Missing");
    }

    @Test
    void declaredRuleViolationsAreDataErrorsAndValidateFalseSuspendsThem() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        // title carries required + min_len 2
        assertThatThrownBy(() -> task.put(List.of(record(doc("d1", "x", 3).toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("title");

        RecordingClient tolerant = new RecordingClient();
        OpenSearchSinkTask relaxed = startedTask(tolerant,
                Map.of(OpenSearchSinkConfig.VALIDATE, "false"));
        relaxed.put(List.of(record(doc("d1", "x", 3).toByteArray(), 1)));
        assertThat(tolerant.writes).hasSize(1);
    }

    @Test
    void nullAndUndecodableValuesAreDataErrors() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        assertThatThrownBy(() -> task.put(List.of(record(null, 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("null");
        assertThatThrownBy(() -> task.put(List.of(record("not-bytes", 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("byte[]");
    }

    @Test
    void jsonValueFormatDecodesText() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.VALUE_FORMAT, "json"));

        task.put(List.of(record("{\"doc_id\":\"d1\",\"title\":\"Hello\"}"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8), 3)));

        assertThat(client.writes.get(0).documents.values().iterator().next())
                .containsEntry("doc_id", "d1");
    }

    @Test
    void aFailedBulkWriteIsRetriable() throws Exception {
        RecordingClient client = new RecordingClient();
        client.failWrites = true;
        OpenSearchSinkTask task = startedTask(client, Map.of());

        assertThatThrownBy(() -> task.put(List.of(record(doc("d1", "Hello", 3).toByteArray(), 1))))
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("docs");
    }

    @Test
    void anEmptyBatchWritesNothing() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        task.put(List.of());

        assertThat(client.writes).isEmpty();
    }

    @Test
    void stopClosesTheClient() throws Exception {
        RecordingClient client = new RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        task.stop();

        assertThat(client.closed).isTrue();
    }

    // ---------------------------------------------------------------- fixture

    private static OpenSearchSinkTask startedTask(RecordingClient client, Map<String, String> extra)
            throws Exception {
        OpenSearchSinkTask task = new OpenSearchSinkTask();
        task.clientFactory = config -> client;
        Map<String, String> props = new HashMap<>(Map.of(
                OpenSearchSinkConfig.DESCRIPTOR_SET, Fixture.create().descriptorSetBase64(),
                OpenSearchSinkConfig.MESSAGE_TYPE, "connect.test.Doc",
                OpenSearchSinkConfig.URL, "http://localhost:39999",
                OpenSearchSinkConfig.INDEX, "docs"));
        props.putAll(extra);
        task.start(props);
        return task;
    }

    private static SinkRecord record(Object value, long offset) {
        return new SinkRecord("orders", 0, null, null, null, value, offset);
    }

    private static DynamicMessage doc(String docId, String title, int pages) throws Exception {
        Fixture fixture = Fixture.create();
        return DynamicMessage.newBuilder(fixture.doc)
                .setField(fixture.doc.findFieldByName("doc_id"), docId)
                .setField(fixture.doc.findFieldByName("title"), title)
                .setField(fixture.doc.findFieldByName("pages"), pages)
                .build();
    }

    static final class RecordingClient implements OpenSearchSinkTask.IndexClient {
        final List<Ensure> ensured = new ArrayList<>();
        final List<BulkWrite> writes = new ArrayList<>();
        boolean failWrites;
        boolean closed;

        @Override
        public boolean ensureIndex(String index, IndexingPlan plan) {
            ensured.add(new Ensure(index, plan));
            return true;
        }

        @Override
        public void bulkWrite(String index, Map<String, Map<String, Object>> documentsById,
                              boolean refresh) throws IOException {
            if (failWrites) {
                throw new IOException("bulk rejected");
            }
            writes.add(new BulkWrite(index, new LinkedHashMap<>(documentsById), refresh));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    record Ensure(String index, IndexingPlan plan) {
    }

    record BulkWrite(String index, Map<String, Map<String, Object>> documents, boolean refresh) {
    }

    /**
     * A descriptor set built in-memory with the hint and validation options attached the
     * typed way, exactly as protoc would compile them: doc_id KEYWORD, title TEXT named
     * "headline" with required+min_len(2) rules, pages plain, payload a
     * google.protobuf.Any.
     */
    record Fixture(Descriptor doc, Descriptor inner, FileDescriptorSet set) {

        static Fixture create() throws Exception {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("connect/test/doc.proto")
                    .setPackage("connect.test")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("label")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("doc_id")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setType(IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("title")
                                    .setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setType(IndexFieldType.INDEX_FIELD_TYPE_TEXT)
                                                            .setName("headline")
                                                            .build())
                                            .setExtension(ValidateProto.field,
                                                    FieldRules.newBuilder()
                                                            .setRequired(true)
                                                            .setString(StringRules.newBuilder()
                                                                    .setMinLen(2))
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("pages")
                                    .setNumber(3)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("payload")
                                    .setNumber(4)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".google.protobuf.Any")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .build();
            FileDescriptorSet set = FileDescriptorSet.newBuilder()
                    .addFile(AnyProto.getDescriptor().toProto())
                    .addFile(file)
                    .build();
            FileDescriptor linked = FileDescriptor.buildFrom(
                    file, new FileDescriptor[]{AnyProto.getDescriptor()});
            return new Fixture(
                    linked.findMessageTypeByName("Doc"),
                    linked.findMessageTypeByName("Inner"),
                    set);
        }

        String descriptorSetBase64() {
            return Base64.getEncoder().encodeToString(set.toByteArray());
        }
    }
}
