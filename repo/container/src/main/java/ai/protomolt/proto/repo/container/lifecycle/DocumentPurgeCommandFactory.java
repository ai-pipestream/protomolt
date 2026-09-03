package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.v1.DocumentPurgeCommand;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;

import java.util.ArrayDeque;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the {@link DocumentPurgeCommand} the Kafka-backed purge queue
 * publishes for one {@link DocumentPurgeRecord}: the purge and node ids as
 * strings (the record key is the node id, so one document's commands are
 * partition-ordered), the requested-at instant, and the object-key snapshot -
 * everything a purger needs to drain without re-reading the row.
 * <p>
 * Also packages the producer's contract: {@link #descriptorSetBase64()} is
 * the serialized {@link FileDescriptorSet} of document_purge.proto and its
 * transitive imports (the validate.v1 options, the well-known
 * timestamp.proto), built from the generated classes' own descriptors, so the
 * protomolt serde validates and frames every record against exactly the
 * schema this service was compiled with - no registry, no drift.
 */
public final class DocumentPurgeCommandFactory {

    private DocumentPurgeCommandFactory() {
    }

    /**
     * The topic payload for one purge record.
     *
     * @param record the purge record being relayed (snapshot included)
     * @return the command to publish, keyed by node id
     */
    public static DocumentPurgeCommand command(DocumentPurgeRecord record) {
        return DocumentPurgeCommand.newBuilder()
                .setPurgeId(record.purgeId.toString())
                .setNodeId(record.nodeId.toString())
                .setAccountId(record.accountId)
                .setDriveName(record.driveName)
                .setRequestedAt(Timestamp.newBuilder()
                        .setSeconds(record.requestedAt.getEpochSecond())
                        .setNanos(record.requestedAt.getNano()))
                .addAllObjectKeys(record.readObjectKeys())
                .build();
    }

    /**
     * The descriptor set the protomolt serde publishes and reads against,
     * base64: document_purge.proto plus its transitive imports, taken from
     * the generated classes' runtime descriptors.
     *
     * @return the serialized FileDescriptorSet, base64-encoded
     */
    public static String descriptorSetBase64() {
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> files =
                new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(java.util.List.of(DocumentPurgeCommand.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                .addAllFile(files.values())
                .build().toByteArray());
    }
}
