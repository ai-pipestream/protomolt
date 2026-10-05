package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationRejectionCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationAssessmentBinding;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Internal decision gate; not mounted until terminal evidence reads are qualified. */
final class DocumentAssessmentRejections {
    private final Tx tx;
    private final long minimumRemainingMicros;

    /** Explicit host policy, not an implicit default or an extension of staging retention. */
    DocumentAssessmentRejections(Tx tx, Duration minimumRemaining) {
        this.tx = Objects.requireNonNull(tx);
        Objects.requireNonNull(minimumRemaining);
        if (minimumRemaining.isNegative() || minimumRemaining.isZero()
                || minimumRemaining.compareTo(Duration.ofDays(1)) > 0 || minimumRemaining.getNano() % 1000 != 0)
            throw new IllegalArgumentException("Minimum assessment window must be positive exact microseconds within one day");
        minimumRemainingMicros = Math.addExact(Math.multiplyExact(minimumRemaining.getSeconds(), 1_000_000), minimumRemaining.getNano() / 1000);
    }

    DocumentPublicationReplay.Observation reject(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentReadLedger.PinnedAssessment capture, DocumentAssessmentReader reader,
            PayloadBudget budget, DocumentRevisionAssembly.Limits limits,
            DocumentAssessmentRuntimeObserver.Observation observation, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        var existing = new DocumentPublicationReplay(tx).observe(caller, command);
        control.check();
        if (existing.state() == DocumentPublicationReplay.State.COMMITTED || existing.state() == DocumentPublicationReplay.State.TERMINATED)
            return existing;
        // No decision runs inside replay's catch/reauthorization boundary. A lost
        // commit acknowledgement remains uncertain and must use receipt replay.
        try (var verified = DocumentAssessmentReplay.verify(capture, reader, budget, limits, observation, control)) {
            return decide(caller, owner, command, verified, control);
        }
    }

    /** The private-constructor proof owns the exact replay inputs and read Use through commit. */
    DocumentPublicationReplay.Observation decide(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentReplay.Verified verified, RepositoryReadControl control) {
        Objects.requireNonNull(owner); Objects.requireNonNull(command); Objects.requireNonNull(verified).check(control);
        var key = owner.key(); var snapshot = verified.snapshot(); var manifest = snapshot.manifest(); var stage = verified.stage();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId())
                || !snapshot.command().canonical().equals(command.canonical()) || !key.principal().equals(manifest.getPrincipal())
                || owner.generation() != manifest.getOwnerGeneration())
            throw new IllegalArgumentException("Verified assessment differs from decision owner or command");
        if (verified.result().firstFailure().isEmpty())
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Accepted assessment cannot justify rejection");
        if (!verified.result().recordedRuntime().equals(verified.result().replayRuntime()))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Admission decision requires the recorded validation runtime");
        var policy = new DocumentSchemaPolicies.Selection(key.account(), manifest.getPolicyRevision(), snapshot.policy());
        long retainUntil = Math.addExact(Math.multiplyExact(stage.retainUntil().getEpochSecond(), 1_000_000),
                stage.retainUntil().getNano() / 1000);
        if (stage.retainUntil().getNano() % 1000 != 0) throw new IllegalStateException("Assessment deadline lost microsecond precision");
        return tx.inTransaction(em -> {
            var owners = em.createNativeQuery("""
                    SELECT owner_generation FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:op FOR UPDATE
                    """).setParameter("account", key.account()).setParameter("principal", key.principal())
                    .setParameter("op", key.operationId()).getResultList();
            if (owners.isEmpty()) throw new RepositoryOperationLedger.OwnerFencedException();
            var existing = DocumentPublicationReplay.observe(em, caller, command, key);
            verified.check(control);
            if (existing.state() == DocumentPublicationReplay.State.COMMITTED || existing.state() == DocumentPublicationReplay.State.TERMINATED)
                return existing;
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            DocumentSchemaPolicies.lockCurrent(em, policy, control::check);
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            var rows = em.createNativeQuery("""
                    SELECT assessment_id FROM document_assessment_owners
                    WHERE assessment_id=:assessment AND account_id=:account AND principal=:principal
                        AND operation_id=:op AND owner_generation=:generation AND command_codec=:codec AND command_version=:version
                        AND encode(command_sha256,'hex')=:command AND encode(manifest_sha256,'hex')=:manifest
                        AND sealed AND release_xid IS NULL AND retain_until=:deadline FOR SHARE
                    """).setParameter("assessment", stage.assessment()).setParameter("account", key.account())
                    .setParameter("principal", key.principal()).setParameter("op", key.operationId()).setParameter("generation", owner.generation())
                    .setParameter("codec", DocumentPublicationCommand.CODEC).setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                    .setParameter("command", command.sha256()).setParameter("manifest", stage.manifestSha256())
                    .setParameter("deadline", OffsetDateTime.ofInstant(stage.retainUntil(), ZoneOffset.UTC)).getResultList();
            if (rows.size() != 1) throw new IllegalStateException("Verified assessment is no longer retained under its exact identity");
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            verified.check(control);
            long now = ((Number) em.createNativeQuery("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)")
                    .getSingleResult()).longValue();
            if (retainUntil - now < minimumRemainingMicros)
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Assessment has insufficient remaining evidence retention");
            var binding = DocumentPublicationAssessmentBinding.newBuilder().setAssessmentId(stage.assessment().toString())
                    .setManifestCodec(DocumentAssessmentManifestCodec.CODEC).setManifestEncodingVersion(DocumentAssessmentManifestCodec.VERSION)
                    .setManifestSha256(stage.manifestSha256()).setRetainUntilEpochMicros(retainUntil).build();
            var receipt = DocumentPublicationRejection.newBuilder().setOperationId(key.operationId().toString())
                    .setAccountId(key.account()).setPrincipal(key.principal()).setOwnerGeneration(owner.generation())
                    .setCommandCodec(DocumentPublicationCommand.CODEC).setCommandEncodingVersion(DocumentPublicationCommand.ENCODING_VERSION)
                    .setCommandSha256(command.sha256()).setRecordedAtEpochMicros(now).setDispositionValue(1).setReasonValue(2)
                    .setAssessment(binding).build();
            var encoded = DocumentPublicationRejectionCodec.encode(command, receipt, key.principal(), owner.generation());
            verified.check(control);
            int inserted = em.createNativeQuery("""
                    INSERT INTO repository_operation_rejection(account_id,principal,operation_id,owner_generation,
                        command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,
                        recorded_at_epoch_micros,disposition,reason,assessment_id,manifest_codec,manifest_version,manifest_sha256,retain_until_epoch_micros)
                    SELECT :account,:principal,:op,:generation,:commandCodec,:commandVersion,:commandSha,
                        :resultCodec,:resultVersion,:bytes,:sha,:now,1,2,:assessment,:manifestCodec,:manifestVersion,:manifest,:deadline
                    WHERE :deadline-floor(extract(epoch FROM clock_timestamp())*1000000)>=:minimum
                    """).setParameter("account", key.account()).setParameter("principal", key.principal()).setParameter("op", key.operationId())
                    .setParameter("generation", owner.generation()).setParameter("commandCodec", DocumentPublicationCommand.CODEC)
                    .setParameter("commandVersion", DocumentPublicationCommand.ENCODING_VERSION).setParameter("commandSha", hex(command.sha256()))
                    .setParameter("resultCodec", DocumentPublicationRejectionCodec.CODEC).setParameter("resultVersion", DocumentPublicationRejectionCodec.VERSION)
                    .setParameter("bytes", encoded.bytes().toByteArray()).setParameter("sha", hex(encoded.sha256())).setParameter("now", now)
                    .setParameter("assessment", stage.assessment()).setParameter("manifestCodec", binding.getManifestCodec())
                    .setParameter("manifestVersion", binding.getManifestEncodingVersion()).setParameter("manifest", hex(stage.manifestSha256()))
                    .setParameter("deadline", retainUntil).setParameter("minimum", minimumRemainingMicros).executeUpdate();
            if (inserted != 1)
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Assessment has insufficient remaining evidence retention");
            // Still before commit: cancellation observed here rolls back the row.
            verified.check(control);
            // The outcome is committed on return. No post-commit control check or
            // live-owner delivery check may turn a durable outcome into failure.
            return new DocumentPublicationReplay.Observation(DocumentPublicationReplay.State.TERMINATED,
                    Optional.empty(), Optional.of(receipt));
        });
    }

    private static byte[] hex(String value) { return HexFormat.of().parseHex(value); }
}
