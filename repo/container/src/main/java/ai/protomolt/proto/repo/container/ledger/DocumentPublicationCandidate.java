package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/** Owns checked candidate bytes through staging/commit; neither authorizes nor publishes. */
final class DocumentPublicationCandidate implements AutoCloseable {
    enum Mode { TYPED, OPAQUE }
    @FunctionalInterface interface Resolver {
        /** The host scopes schema access to this member and its authenticated caller. */
        DocumentSchemaAdmission.Definition select(DocumentPublicationMember member, DocumentSchemaAdmission.Selection occurrence);
    }

    private final DocumentPublicationFragments fragments;
    private final List<DocumentSchemaAdmission.PreparedProof> owners;
    private DocumentSchemaBatch schemas;
    private Map<String, DocumentCommandContent> opaque;

    private DocumentPublicationCandidate(DocumentPublicationFragments fragments,
            List<DocumentSchemaAdmission.PreparedProof> owners, DocumentSchemaBatch schemas,
            Map<String, DocumentCommandContent> opaque) {
        this.fragments = fragments; this.owners = new ArrayList<>(owners);
        this.schemas = schemas; this.opaque = Map.copyOf(opaque);
    }

    /**
     * Every command member has an explicit mode. Policy may refuse opaque admission,
     * but failed typed admission never changes mode. Inputs stay borrowed/stable during
     * capture; the result owns private fragment, schema and evidence copies. Opaque
     * assembly limits are independently supplied, not derived from a schema-asset cap.
     * Policy selection, source pins, resolver inputs and parsed heap remain host-owned.
     */
    static DocumentPublicationCandidate prepare(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, Mode> modes, Map<String, Map<Integer, ByteString>> supplied,
            Optional<DocumentSchemaAdmission.Definition> container, Resolver resolver, PayloadBudget budget,
            DocumentRevisionAssembly.Limits opaqueLimits, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(policy); Objects.requireNonNull(modes);
        Objects.requireNonNull(container); Objects.requireNonNull(resolver); Objects.requireNonNull(budget);
        Objects.requireNonNull(opaqueLimits); active(control);
        if (!policy.account().equals(command.intent().getAccountId()))
            throw new IllegalArgumentException("Policy account differs from command");
        if (modes.size() != command.intent().getMembersCount())
            throw new IllegalArgumentException("Admission modes differ from command members");
        var selectedModes = Map.copyOf(modes);
        var expected = new HashSet<String>();
        for (var member : command.intent().getMembersList()) {
            active(control);
            expected.add(member.getMemberId());
            var mode = selectedModes.get(member.getMemberId());
            if (mode == null) throw new IllegalArgumentException("Admission mode is missing for member");
            if (mode == Mode.OPAQUE && policy.policy().requiresTyped(member))
                throw new IllegalArgumentException("Policy requires typed admission for member");
            if (mode == Mode.TYPED && container.isEmpty())
                throw new IllegalArgumentException("Typed admission requires a container definition");
        }
        if (!expected.equals(selectedModes.keySet())) throw new IllegalArgumentException("Unknown admission mode member");
        DocumentAdmissionReservations reservations = bytes -> {
            try {
                var lease = budget.reserve(bytes);
                return lease::close;
            } catch (PayloadBudget.CapacityExceededException exhausted) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Publication admission capacity exhausted");
            }
        };
        var snapshot = DocumentPublicationFragments.capture(command, supplied, budget, control);
        var owners = new ArrayList<DocumentSchemaAdmission.PreparedProof>();
        boolean transferred = false;
        try {
            var proofs = new HashMap<String, DocumentSchemaAdmission.Proof>();
            var opaque = new HashMap<String, DocumentCommandContent>();
            var digest = ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()));
            for (var member : command.intent().getMembersList()) {
                active(control);
                String id = member.getMemberId();
                var bytes = snapshot.fragments().get(id);
                if (selectedModes.get(id) == Mode.TYPED) {
                    var owner = policy.policy().prepareAndCheck(digest, member, bytes, container.orElseThrow(),
                            occurrence -> resolver.select(member, occurrence), reservations, control);
                    try { owners.add(owner); }
                    catch (RuntimeException | Error failure) { owner.close(); throw failure; }
                    proofs.put(id, owner.proof());
                } else {
                    opaque.put(id, DocumentCommandContent.check(command, id, bytes, false, opaqueLimits, control));
                }
            }
            var schemas = DocumentSchemaBatch.prepare(command, policy, proofs, reservations, control);
            active(control);
            var result = new DocumentPublicationCandidate(snapshot, owners, schemas, opaque);
            transferred = true;
            return result;
        } finally {
            if (!transferred) {
                for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
                snapshot.close();
            }
        }
    }

    /** Borrow until close; callers must drain all staging/commit consumers first. */
    synchronized DocumentSchemaBatch schemas() { requireOpen(); return schemas; }
    synchronized Map<String, DocumentCommandContent> opaque() { requireOpen(); return opaque; }
    private void requireOpen() { if (schemas == null) throw new IllegalStateException("Publication candidate is closed"); }
    @Override public synchronized void close() {
        if (schemas == null) return;
        schemas = null; opaque = Map.of();
        for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
        owners.clear(); fragments.close();
    }
    private static void active(Runnable control) {
        Objects.requireNonNull(control);
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Publication candidate preparation interrupted");
        control.run();
    }
}
