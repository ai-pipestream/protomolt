package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.v1.DocumentPurgeCommand;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DocumentPurgeCommandFactory}: the relayed command carries everything
 * a purger needs to drain without re-reading the row (ids as strings, the
 * requested-at instant, the object-key snapshot), and the packaged descriptor
 * set contains document_purge.proto and its transitive imports.
 */
class DocumentPurgeCommandFactoryTest {

    private static final Instant WHEN = Instant.parse("2026-08-09T01:02:03.987654321Z");

    @Test
    void commandMirrorsThePurgeRecord() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = UUID.randomUUID();
        record.nodeId = UUID.randomUUID();
        record.docId = "doc-1";
        record.graphAddressId = "ds-1";
        record.accountId = "acct-1";
        record.graphId = "intake:acct-1";
        record.driveName = "docs";
        record.writeObjectKeys(List.of("a/core.pb", "a/chunks/set-1.pb"));
        record.requestedAt = WHEN;

        DocumentPurgeCommand command = DocumentPurgeCommandFactory.command(record);

        assertThat(command.getPurgeId()).isEqualTo(record.purgeId.toString());
        assertThat(command.getNodeId()).isEqualTo(record.nodeId.toString());
        assertThat(command.getAccountId()).isEqualTo("acct-1");
        assertThat(command.getDriveName()).isEqualTo("docs");
        assertThat(command.getRequestedAt().getSeconds()).isEqualTo(WHEN.getEpochSecond());
        assertThat(command.getRequestedAt().getNanos()).isEqualTo(WHEN.getNano());
        assertThat(command.getObjectKeysList()).containsExactly("a/core.pb", "a/chunks/set-1.pb");
    }

    @Test
    void theDescriptorSetContainsTheContractAndItsTransitiveImports() throws Exception {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(
                Base64.getDecoder().decode(DocumentPurgeCommandFactory.descriptorSetBase64()));
        List<String> files = set.getFileList().stream().map(f -> f.getName()).toList();

        assertThat(files).contains(
                "ai/pipestream/proto/repo/v1/document_purge.proto",
                "ai/pipestream/proto/validate/v1/validate.proto",
                "google/protobuf/timestamp.proto");
    }
}
