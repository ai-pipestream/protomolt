package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Stored command, member and schema associations, without a payload validation verdict.
 * The caller supplies an authorized, sealed snapshot from DocumentHistoricalSchemaRows.
 * This value does not establish fragment identity, evidence membership or delivery permission.
 */
record DocumentHistoricalSchemaBinding(ByteString commandSha256, DocumentPublicationMember member,
        DocumentAdmissionPolicy policy, DocumentSchemaAdmission.Reference container,
        List<DocumentSchemaAdmission.Reference> references) {
    DocumentHistoricalSchemaBinding { references = List.copyOf(references); }

    static DocumentHistoricalSchemaBinding read(NodeAddress address, DocumentHistoricalSchemaRows.Snapshot snapshot,
            Runnable control) throws InvalidProtocolBufferException {
        control.run();
        var h = snapshot.header();
        if (!DocumentPublicationCommand.CODEC.equals(h.commandCodec())
                || h.commandVersion() != DocumentPublicationCommand.ENCODING_VERSION)
            throw DocumentHistoricalSchemaRows.invalid("Unsupported historical command encoding");
        var storedIntent = DocumentPublicationIntent.parseFrom(h.command());
        if (!storedIntent.getOperationId().isEmpty())
            throw DocumentHistoricalSchemaRows.invalid("Historical canonical command contains an operation identifier");
        var command = new DocumentPublicationCommand(storedIntent.toBuilder().setOperationId(h.operation().toString()).build());
        if (!command.canonical().equals(h.command()) || !command.sha256().equals(h.commandSha())
                || !command.intent().getAccountId().equals(address.getAccountId()))
            throw DocumentHistoricalSchemaRows.invalid("Historical command identity differs");
        var member = command.intent().getMembersList().stream().filter(value -> value.getMemberId().equals(h.member()))
                .findFirst().orElseThrow(() -> DocumentHistoricalSchemaRows.invalid("Historical member is absent from its command"));
        if (!member.getDestination().getAddress().equals(address))
            throw DocumentHistoricalSchemaRows.invalid("Historical member address differs");
        var policy = DocumentAdmissionPolicy.decode(h.policyCodec(), h.policyVersion(), h.policy(), h.policySha(), control);
        if (!policy.definition().getAccountId().equals(address.getAccountId()))
            throw DocumentHistoricalSchemaRows.invalid("Historical policy account differs");
        var references = new ArrayList<>(snapshot.references());
        var containers = references.stream().filter(reference -> reference.descriptorSha256().equals(h.containerDescriptorSha())
                && DocumentPartCodec.sha256Hex(reference.typeUrl().getBytes(StandardCharsets.UTF_8)).equals(h.containerUrlSha())).toList();
        if (containers.size() != 1) throw DocumentHistoricalSchemaRows.invalid("Historical container association is missing or ambiguous");
        var container = containers.getFirst();
        references.remove(container);
        return new DocumentHistoricalSchemaBinding(ByteString.copyFrom(HexFormat.of().parseHex(h.commandSha())),
                member, policy, container, references);
    }

    /** Complete-candidate input for strict replay, not a selected-content materialization request. */
    DocumentSchemaAdmission.Request validationRequest(Map<Integer, ByteString> fragments,
            List<DocumentHistoricalSchemaRows.Root> roots) {
        var evidence = new HashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
        for (var root : roots) evidence.computeIfAbsent(root.ordinal(), ignored -> new ArrayList<>()).add(root.evidence());
        return new DocumentSchemaAdmission.Request(commandSha256, policy.sha256(),
                policy.definition().getRequireStructuredRoot(), member, fragments, evidence, container, references);
    }
}
