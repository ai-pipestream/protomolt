package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationResultCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Internal native commit. The embedding host owns authentication, byte reservations and provider qualification. */
final class DocumentPublicationCommit {
    private final Tx tx;
    private final DriveLedger drives;
    private final boolean requireTypedSchema;
    private final boolean deliverEvents;

    DocumentPublicationCommit(Tx tx, DriveLedger drives, boolean requireTypedSchema, boolean deliverEvents) {
        this.tx=Objects.requireNonNull(tx); this.drives=Objects.requireNonNull(drives);
        this.requireTypedSchema=requireTypedSchema; this.deliverEvents=deliverEvents;
    }

    /** Prepared must describe the currently selected attempt; rebuild it after replacing an attempt. */
    DocumentPublicationResult commit(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentCommandContent> content,
            Map<String,DocumentSelectedAttemptLedger.Selected> selections, Runnable callerControl) {
        Objects.requireNonNull(owner); Objects.requireNonNull(prepared); Objects.requireNonNull(callerControl);
        Runnable control=() -> {
            if (Thread.currentThread().isInterrupted())
                throw new java.util.concurrent.CancellationException("Document publication interrupted");
            callerControl.run();
        };
        Objects.requireNonNull(content); Objects.requireNonNull(selections);
        control.run();
        var plan=prepared.plan(); var command=plan.command();
        DocumentAdmissionAuthorization.requireCaller(caller,owner,command.intent().getAccountId());
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Publication command differs from operation scope");
        if (requireTypedSchema || plan.members().stream().anyMatch(m -> m.intent().hasStructuredSchema()))
            throw new UnsupportedOperationException("Typed publication requires retained schema integration");
        if (content.size()>64 || selections.size()>64) throw new IllegalArgumentException("Publication preparation exceeds member bounds");
        var checked=Map.copyOf(content); var selected=Map.copyOf(selections);
        Set<String> members=plan.members().stream().map(m -> m.intent().getMemberId()).collect(Collectors.toSet());
        Set<String> uploads=plan.members().stream().filter(m -> m.attempt().isPresent())
                .map(m -> m.intent().getMemberId()).collect(Collectors.toSet());
        if (!checked.keySet().equals(members) || !selected.keySet().equals(uploads))
            throw new IllegalArgumentException("Publication preparation differs from the complete command member set");
        for (var member:plan.members()) {
            var actual=checked.get(member.intent().getMemberId());
            if (!actual.command().canonical().equals(command.canonical()) || !actual.member().equals(member.intent()))
                throw new IllegalArgumentException("Checked document content belongs to another command or member");
        }
        var authorization=DocumentAdmissionAuthorization.prepare(plan);
        var reuse=DocumentReuseAdmission.prepare(plan);
        var retainedEntries=DocumentRetainedManifestEntries.prepare(plan,control);
        var placements=plan.members().stream().map(DocumentUploadPlan.Member::placement).distinct()
                .sorted(Comparator.comparing(p -> p.drive().id())).toList();
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,owner);
            RepositoryOperationLedger.requireCommand(em,owner.key(),command);
            // Before document locks; the SQL projection trigger also protects legacy paths.
            DocumentSchemaPolicies.lockUnboundWriter(em, command.intent().getAccountId());
            var locked=DocumentAdmissionAuthorization.lockAndAuthorize(em,caller,plan,authorization);
            for (var placement:placements) {
                placement.drive().lock(em,drives);
                if (!ManagedBackendLedger.find(em,placement.generation()).orElseThrow(
                        () -> new IllegalArgumentException("Selected backend is not registered")).equals(placement.profile()))
                    throw new IllegalArgumentException("Selected backend differs from its immutable profile");
            }
            var parts=DocumentCommitParts.bind(em,owner,plan,selected,reuse,control);
            control.run();
            // Snapshot all sources and candidates before any destination changes; a
            // source may also be another member's destination in this same commit.
            var sources=DocumentRetainedManifestEntries.read(em,retainedEntries,control);
            var candidates=new ArrayList<DocumentCommitWriter.Candidate>();
            Instant now=Instant.now();
            for (var member:plan.members()) {
                control.run();
                candidates.add(DocumentCommitWriter.prepare(member,checked.get(member.intent().getMemberId()),parts,locked,sources,now,control));
            }
            var result=DocumentPublicationResult.newBuilder().setOperationId(command.operationId().toString())
                    .setAccountId(owner.key().account()).setPrincipal(owner.key().principal()).setOwnerGeneration(owner.generation())
                    .setCommandEncodingVersion(ai.protomolt.proto.repo.spi.DocumentPublicationCommand.ENCODING_VERSION).setCommandSha256(command.sha256());
            for (int i=0;i<candidates.size();i++) {
                control.run();
                result.addMembers(DocumentCommitWriter.write(em,owner,candidates.get(i),i,deliverEvents));
            }
            em.flush();
            var success=result.build();
            var encoded=DocumentPublicationResultCodec.encode(command,success,owner.key().principal(),owner.generation());
            control.run();
            em.createNativeQuery("""
                    INSERT INTO repository_operation_success(account_id,principal,operation_id,owner_generation,
                        command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,member_count)
                    SELECT account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                        :codec,:version,:result,:digest,:count FROM repository_operations
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    """).setParameter("generation",owner.generation()).setParameter("codec",DocumentPublicationResultCodec.CODEC)
                    .setParameter("version",DocumentPublicationResultCodec.VERSION).setParameter("result",encoded.bytes().toByteArray())
                    .setParameter("digest",java.util.HexFormat.of().parseHex(encoded.sha256())).setParameter("count",success.getMembersCount())
                    .setParameter("account",owner.key().account()).setParameter("principal",owner.key().principal())
                    .setParameter("operation",owner.key().operationId()).executeUpdate();
            control.run();
            em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
            // No cancellation check after commit: a returned success is the durable outcome.
            return success;
        });
    }
}
