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
        return commit(caller, owner, prepared, content, selections, null, callerControl);
    }

    /** Schema artifacts must already be staged; content contains exactly the proofless members. */
    DocumentPublicationResult commit(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentCommandContent> content,
            Map<String,DocumentSelectedAttemptLedger.Selected> selections, DocumentSchemaBatch schemas, Runnable callerControl) {
        prepared.plan().command().requireExecutionSupported();
        return commitInternal(caller, owner, prepared, content, selections, schemas, callerControl, null);
    }

    /** Internal only: caller owns the exact live source preparations through transaction completion. */
    DocumentPublicationResult commitHistorical(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentCommandContent> content,
            Map<String,DocumentSelectedAttemptLedger.Selected> selections, DocumentSchemaBatch schemas, Runnable control,
            java.util.List<DocumentHistoricalReferenceAdmission.Prepared> sources) {
        if (owner.executionClaim().isPresent())
            throw new UnsupportedOperationException("Claimed historical publication is not implemented");
        Objects.requireNonNull(schemas, "Historical publication requires a checked schema batch");
        var references = DocumentHistoricalReferenceAdmission.requireComplete(prepared.plan().command(), sources, control);
        if (references.isEmpty() || !references.equals(prepared.plan().historical()))
            throw new IllegalArgumentException("Historical publication differs from physical plan sources");
        var historical = DocumentHistoricalManifestEntries.prepare(prepared.plan().command(), references, control);
        return commitInternal(caller, owner, prepared, content, selections, schemas, control, historical);
    }

    private DocumentPublicationResult commitInternal(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentCommandContent> content,
            Map<String,DocumentSelectedAttemptLedger.Selected> selections, DocumentSchemaBatch schemas, Runnable callerControl,
            DocumentHistoricalManifestEntries historical) {
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
        if (schemas == null && (requireTypedSchema || plan.members().stream().anyMatch(m -> m.intent().hasStructuredSchema())))
            throw new UnsupportedOperationException("Typed publication requires a checked schema batch");
        if (content.size()>64 || selections.size()>64) throw new IllegalArgumentException("Publication preparation exceeds member bounds");
        var checked=new java.util.HashMap<>(Map.copyOf(content)); var selected=Map.copyOf(selections);
        if (schemas != null && !schemas.command().canonical().equals(command.canonical()))
            throw new IllegalArgumentException("Schema batch differs from publication command");
        var retention = new java.util.HashMap<String,DocumentSchemaRetention>();
        Set<String> members=plan.members().stream().map(m -> m.intent().getMemberId()).collect(Collectors.toSet());
        Set<String> uploads=plan.members().stream().filter(m -> m.attempt().isPresent())
                .map(m -> m.intent().getMemberId()).collect(Collectors.toSet());
        var requiredContent = new java.util.HashSet<>(members);
        if (schemas != null) requiredContent.removeAll(schemas.proofs().keySet());
        if (!checked.keySet().equals(requiredContent) || !selected.keySet().equals(uploads))
            throw new IllegalArgumentException("Publication preparation differs from the complete command member set");
        for (var member:plan.members()) {
            String id = member.intent().getMemberId();
            if (schemas != null) {
                var proof = schemas.proofs().get(id);
                if (proof == null && (requireTypedSchema || member.intent().hasStructuredSchema()))
                    throw new IllegalArgumentException("Required typed publication proof is absent");
                if (proof != null) {
                    checked.put(id, DocumentCommandContent.fromSchema(schemas, id, control));
                    retention.put(id, DocumentSchemaRetention.prepare(schemas, id));
                    continue;
                }
            }
            var actual=checked.get(id);
            if (!actual.command().canonical().equals(command.canonical()) || !actual.member().equals(member.intent()))
                throw new IllegalArgumentException("Checked document content belongs to another command or member");
        }
        var authorization=historical == null ? DocumentAdmissionAuthorization.prepare(plan)
                : DocumentAdmissionAuthorization.prepare(plan, plan.historical());
        var reuse=DocumentReuseAdmission.prepare(plan);
        var retainedEntries=DocumentRetainedManifestEntries.prepare(plan,control);
        var placements=plan.members().stream().map(DocumentUploadPlan.Member::placement).distinct()
                .sorted(Comparator.comparing(p -> p.drive().id())).toList();
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,owner);
            RepositoryOperationLedger.requireCommand(em,owner.key(),command);
            // Before document locks; the SQL projection trigger also protects legacy paths.
            if (schemas == null) DocumentSchemaPolicies.lockUnboundWriter(em, command.intent().getAccountId());
            else schemas.lockPolicy(em, owner, control);
            var locked=DocumentAdmissionAuthorization.lockAndAuthorize(em,caller,plan,authorization);
            DocumentPublicationModeBinding.require(em, owner, command, schemas == null ? Set.of() : schemas.proofs().keySet(), control);
            for (var placement:placements) {
                placement.drive().lock(em,drives);
                if (!ManagedBackendLedger.find(em,placement.generation()).orElseThrow(
                        () -> new IllegalArgumentException("Selected backend is not registered")).equals(placement.profile()))
                    throw new IllegalArgumentException("Selected backend differs from its immutable profile");
            }
            var parts=historical == null ? DocumentCommitParts.bind(em,owner,plan,selected,reuse,control)
                    : DocumentCommitParts.bindHistoricalPublication(em,owner,plan,selected,reuse,control);
            if (schemas != null) schemas.lockArtifacts(em, owner, control);
            control.run();
            // Snapshot all sources and candidates before any destination changes; a
            // source may also be another member's destination in this same commit.
            var sources=DocumentRetainedManifestEntries.read(em,retainedEntries,control);
            var candidates=new ArrayList<DocumentCommitWriter.Candidate>();
            Instant now=Instant.now();
            for (var member:plan.members()) {
                control.run();
                var checkedMember = checked.get(member.intent().getMemberId());
                candidates.add(historical == null
                        ? DocumentCommitWriter.prepare(member,checkedMember,parts,locked,sources,now,control)
                        : DocumentCommitWriter.prepareHistorical(member,checkedMember,parts,locked,sources,historical,now,control));
            }
            var decisions = new java.util.HashMap<String,String>();
            if (schemas != null) {
                for (var candidate : candidates) {
                    control.run();
                    decisions.put(candidate.member(), DocumentSchemaAdmissionBinding.insert(em, owner, schemas, candidate, parts, control));
                }
            }
            var result=DocumentPublicationResult.newBuilder().setOperationId(command.operationId().toString())
                    .setAccountId(owner.key().account()).setPrincipal(owner.key().principal()).setOwnerGeneration(owner.generation())
                    .setCommandEncodingVersion(ai.protomolt.proto.repo.spi.DocumentPublicationCommand.ENCODING_VERSION).setCommandSha256(command.sha256());
            for (int i=0;i<candidates.size();i++) {
                control.run();
                var candidate = candidates.get(i);
                result.addMembers(schemas == null
                        ? DocumentCommitWriter.write(em,owner,candidate,i,deliverEvents)
                        : DocumentCommitWriter.write(em,owner,candidate,i,deliverEvents,decisions.get(candidate.member()),retention.get(candidate.member()),control));
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
            if (historical != null) DocumentHistoricalReferenceAdmission.requireComplete(command, plan.historical(), control);
            em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
            // No cancellation check after commit: a returned success is the durable outcome.
            return success;
        });
    }
}
