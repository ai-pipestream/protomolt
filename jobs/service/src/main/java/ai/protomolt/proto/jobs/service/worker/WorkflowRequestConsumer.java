package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.jobs.v1.WorkflowRunRequest;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Package-private Kafka request-topic consumer for broker-native {@link WorkflowRunRequest}
 * job submissions.
 */
final class WorkflowRequestConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowRequestConsumer.class);

    private final WorkflowRunStore store;
    private final ActionContext context;
    private final WorkflowRepository repository;
    private final WorkflowRunsConfig config;
    private final WorkflowRunSubmitter submitter;

    WorkflowRequestConsumer(WorkflowRunStore store, ActionContext context,
                            WorkflowRepository repository, WorkflowRunsConfig config,
                            WorkflowRunSubmitter submitter) {
        this.store = store;
        this.context = context;
        this.repository = repository;
        this.config = config;
        this.submitter = submitter;
    }

    void consumeLoop(BooleanSupplier closed) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, WorkflowRunsConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (ProtoMoltProtobufDeserializer deserializer = configuredDeserializer();
                KafkaConsumer<String, Message> consumer =
                        new KafkaConsumer<>(props, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(config.requestTopic()));
            while (!closed.getAsBoolean()) {
                ConsumerRecords<String, Message> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Message> record : records) {
                    submitRequest(record.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (RuntimeException e) {
            LOG.error("workflow-run request consumer died: {}", e.getMessage(), e);
        }
    }

    void submitRequest(Message value) {
        WorkflowRunRequest request;
        try {
            request = value instanceof WorkflowRunRequest typed
                    ? typed
                    : WorkflowRunRequest.parseFrom(value.toByteArray());
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            LOG.error("dropping an undecodable WorkflowRunRequest record: {}", e.getMessage(), e);
            return;
        }
        UUID jobId;
        try {
            jobId = UUID.fromString(request.getJobId());
        } catch (IllegalArgumentException e) {
            LOG.error("dropping a WorkflowRunRequest whose job_id is not a uuid: '{}'",
                    request.getJobId());
            return;
        }
        String inputJson;
        try {
            inputJson = JsonFormat.printer().print(request.getInput());
        } catch (InvalidProtocolBufferException e) {
            failAtBirth(jobId, request.getWorkflowName(), null, "{}",
                    "the request's input Struct is not printable: " + e.getMessage());
            return;
        }
        WorkflowRunSubmitter.Outcome outcome;
        try {
            outcome = submitter.submit(null, request.getWorkflowName(),
                    context.objectMapper().readTree(inputJson), request.getJobId(), context);
        } catch (JsonProcessingException | RuntimeException e) {
            throw WorkflowRunStoreException.wrap("broker-native submit of job " + jobId
                    + " failed", e);
        }
        if (!outcome.ok()) {
            if (outcome.conflict()) {
                throw new IllegalStateException("conflicting WorkflowRunRequest for job "
                        + jobId + ": " + outcome.error());
            }
            ObjectNode resolved = repository == null
                    ? null
                    : repository.workflow(request.getWorkflowName()).orElse(null);
            failAtBirth(jobId, request.getWorkflowName(), resolved,
                    inputJson, outcome.error());
        }
    }

    private static ProtoMoltProtobufDeserializer configuredDeserializer() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                WorkflowRunEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                WorkflowRunRequest.getDescriptor().getFullName()), false);
        return deserializer;
    }

    private void failAtBirth(UUID jobId, String workflowName, ObjectNode workflowDefinition,
                             String inputJson, String error) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = jobId;
        record.workflowName = workflowName == null || workflowName.isBlank()
                ? "(unknown)" : workflowName;
        record.workflowDefinition = workflowDefinition == null
                ? "{}" : workflowDefinition.toString();
        record.input = inputJson == null ? "{}" : inputJson;
        record.status = WorkflowRunRecord.STATUS_FAILED;
        record.error = error;
        record.maxAttempts = config.maxAttemptsDefault();
        record.runAfter = Instant.now();
        record.completedAt = Instant.now();
        WorkflowRunStore.InsertOutcome inserted =
                store.insert(record, WorkflowRunEventFactory.failed(record, "", error));
        if (inserted.conflict()) {
            throw new IllegalStateException("conflicting failed WorkflowRunRequest for job "
                    + jobId + ": " + error);
        } else if (inserted.created()) {
            LOG.warn("workflow run {} FAILED at birth: {}", jobId, error);
        }
    }
}
