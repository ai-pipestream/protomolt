package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;

/** Internal historical schema replay; provider reads and their pin lifetimes belong to the host. */
final class DocumentHistoricalSchemas {
    private final Tx tx;
    DocumentHistoricalSchemas(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Rechecks supplied exact fragment bytes using retained definitions and the current runtime.
     * No registry or historical executable code is consulted. This does not establish provider
     * availability. The host must protect provider reads with historical pins and bound concurrent
     * invocations, including JDBC/protobuf copies of up to 64 MiB artifacts and 16 MiB evidence.
     * The full command and policy are internal: permission for one member never exposes siblings.
     */
    DocumentSchemaAdmission.Proof check(RepositoryCaller caller, NodeAddress address, UUID revision,
            Map<Integer, ByteString> fragments, Runnable control) {
        Objects.requireNonNull(address); Objects.requireNonNull(revision); Objects.requireNonNull(fragments);
        Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("Historical schema read interrupted");
            control.run();
        };
        final DocumentSchemaAdmission.Proof proof;
        try {
            active.run();
            var snapshot = tx.inTransaction(em -> {
                DocumentAdmissionAuthorization.authorizeHistory(em, caller, address);
                return DocumentHistoricalSchemaRows.capture(em, address, revision, active);
            });
            proof = replay(address, snapshot, fragments, active);
            active.run();
        } catch (CancellationException failure) {
            throw failure;
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            authorize(caller, address);
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Historical content or retained schema failed validation", failure);
        } catch (RuntimeException failure) {
            // Revocation during replay suppresses detailed content/storage failures as well as results.
            authorize(caller, address);
            throw failure;
        }
        authorize(caller, address);
        return proof;
    }

    private void authorize(RepositoryCaller caller, NodeAddress address) {
        tx.inTransaction(em -> { DocumentAdmissionAuthorization.authorizeHistory(em, caller, address); });
    }

    private static DocumentSchemaAdmission.Proof replay(NodeAddress address, DocumentHistoricalSchemaRows.Snapshot snapshot,
            Map<Integer, ByteString> fragments, Runnable control) throws InvalidProtocolBufferException {
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
        var evidence = new HashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
        for (var root : snapshot.roots()) evidence.computeIfAbsent(root.ordinal(), ignored -> new ArrayList<>()).add(root.evidence());
        var request = new DocumentSchemaAdmission.Request(ByteString.copyFrom(HexFormat.of().parseHex(h.commandSha())),
                h.policySha(), policy.definition().getRequireStructuredRoot(), member, fragments, evidence, container, references);
        var proof = DocumentSchemaAdmission.check(request, sha -> Optional.ofNullable(snapshot.artifacts().get(sha)), policy.limits(), control);
        policy.verifyProof(proof, control);
        if (!proof.artifacts().equals(snapshot.artifacts()))
            throw DocumentHistoricalSchemaRows.invalid("Historical artifact set differs from replayed schema closure");
        if (proof.roots().size() != snapshot.roots().size())
            throw DocumentHistoricalSchemaRows.invalid("Historical root set differs from replayed evidence");
        record RootKey(int ordinal, String locator) {}
        var roots = new HashMap<RootKey, DocumentSchemaAdmission.RootEvidence>();
        for (var root : proof.roots()) roots.put(new RootKey(root.ordinal(), root.locatorSha256()), root);
        for (var retained : snapshot.roots()) {
            control.run();
            var actual = roots.remove(new RootKey(retained.ordinal(), retained.locatorSha()));
            var fragment = proof.fragments().get(retained.ordinal());
            // check() already verified every fragment against the canonical part hash.
            // Compare the retained row to that identity without hashing a large fragment once per root.
            var part = retained.ordinal() >= 0 && retained.ordinal() < member.getPartsCount()
                    ? member.getParts(retained.ordinal()) : null;
            String fragmentSha = part == null ? null : part.hasUpload() ? part.getUpload().getSha256()
                    : part.hasReuse() ? part.getReuse().getObject().getSha256() : null;
            if (actual == null || !actual.encoded().equals(retained.evidence()) || fragment == null
                    || fragment.size() != retained.fragmentSize()
                    || !retained.fragmentSha().equals(fragmentSha))
                throw DocumentHistoricalSchemaRows.invalid("Historical root or fragment identity differs");
        }
        return proof;
    }
}
