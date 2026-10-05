package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;

/** Real observed evidence and SQL transaction; physical observations are explicitly synthetic. */
public final class AssessmentCreationProbe {
    public static void run(Tx tx, DocumentAssessmentRuntimeObserver.Observation observation, javax.sql.DataSource database,
            AssessmentProviderProbe provider) throws Exception {
        var mixedSource = AssessmentMixedReuseProbe.publishSource(tx, provider);
        List<DocumentPublicationMember> restartMembers = new ArrayList<>();
        for (String memberId : List.of("a", "b")) {
            var member = ObservedAssessmentProbe.member(memberId).member();
            restartMembers.add(member.toBuilder().setDestination(DocumentRevisionCondition.newBuilder()
                    .setAddress(member.getDestination().getAddress().toBuilder().setGraphId("restart"))
                    .setExpectedMutationRevision(1)).build());
        }
        // Pre-policy legacy rows; the restart candidate updates these destinations.
        restartMembers = AssessmentRestartProbe.seedDestinations(tx, restartMembers);
        var rejectionMembers = new ArrayList<DocumentPublicationMember>();
        for (String id : List.of("a", "b")) {
            var member = ObservedAssessmentProbe.member(id).member();
            rejectionMembers.add(member.toBuilder().setDestination(member.getDestination().toBuilder()
                    .setAddress(member.getDestination().getAddress().toBuilder().setGraphId("rejection-authorization"))).build());
        }
        var rejectionTargets = AssessmentRestartProbe.seedDestinations(tx, rejectionMembers);
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setValidationProfile(DocumentSchemaAdmission.PROFILE).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(20).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        var initial = new DocumentSchemaPolicies(tx).activate(policy, 0, () -> {});
        AssessmentMixedReuseProbe.run(tx, provider, mixedSource, initial, observation);
        var active = AssessmentOperationReplayProbe.run(tx, provider, initial, observation, database, rejectionTargets);
        for (int scenario : new int[]{0, 2, 1}) {
            boolean invalid = scenario == 1;
            boolean afterScope = scenario == 2;
            var a = ObservedAssessmentProbe.member("a"); var b = ObservedAssessmentProbe.member("b");
            var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                    .setOperationId(UUID.randomUUID().toString()).setAccountId("account")
                    .addAllMembers(afterScope ? restartMembers : List.of(a.member(), b.member())).build());
            var caller = new RepositoryCaller("principal", true);
            var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), afterScope ? Duration.ofSeconds(60) : Duration.ofMinutes(5)).owner().orElseThrow();
            String generation = afterScope ? "assessment-s3" : "creation-probe";
            var profile = afterScope ? provider.profile() : new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1",
                    Map.of("endpoint", "synthetic-provider-observations")), "creation-probe");
            new ManagedBackendLedger(tx).bind(generation, profile);
            var drives = new DriveLedger(tx);
            var placements = new HashMap<UUID,DocumentUploadPlan.Placement>();
            var attempts = new HashMap<String,UUID>();
            for (var member : command.intent().getMembersList()) {
                var drive = new DriveRecord(); drive.driveId = UUID.fromString(member.getDriveId()); drive.accountId = "account";
                drive.name = "creation-" + drive.driveId; drive.bucket = "namespace"; drive.prefix = "root";
                drive.provider = profile.identity().provider(); drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
                drives.insert(drive);
                placements.put(drive.driveId, DocumentUploadPlan.Placement.sample(drive, generation, profile));
                attempts.put(member.getMemberId(), UUID.randomUUID());
            }
            var prepared = DocumentOperationUploadAdmission.prepare(command, placements, attempts, Duration.ofMinutes(5));
            var admitted = new DocumentOperationUploadAdmission(tx, drives).admit(caller, owner, prepared);
            var selected = new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
            for (var member : prepared.plan().members()) {
                var attempt = admitted.stream().filter(value -> value.id().equals(member.attempt().orElseThrow().id())).findFirst().orElseThrow();
                var selection = new DocumentSelectedAttemptLedger.Selected(member.intent().getMemberId(), 1, attempt.id(), attempt.token());
                selected.put(selection.member(), selection);
                // Earlier negative fixtures use labelled synthetic measurements.
                // The afterScope fixture performs real S3 PUT and bounded read-back.
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selection, member.attempt().orElseThrow().uploads().stream().map(upload -> {
                    var object = upload.object();
                    if (afterScope) {
                        var fragments = member.intent().getMemberId().equals("a") ? a.fragments() : b.fragments();
                        var measured = DocumentPartTransfer.upload(provider.store(), "namespace", object,
                                fragments.get(upload.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                        return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                                object.contentType(), measured.version(), measured.etag());
                    }
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), "fixture-version", "fixture-etag");
                }).toList());
            }
            var budget = new PayloadBudget(64_000_000);
            var payload = invalid ? ObservedAssessmentProbe.invalidSchema() : ObservedAssessmentProbe.asset(StringValue.getDescriptor());
            DocumentAssessmentCreation.Created retained;
            try (var assessment = DocumentPublicationAssessment.prepare(command, active,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED, "b", DocumentPublicationCandidate.Mode.OPAQUE),
                    Map.of("a", a.fragments(), "b", b.fragments()), Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                    (member, occurrence) -> payload, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), () -> {})) {
                require(assessment.failure().isPresent() == invalid, "real semantic result");
                UUID id = UUID.randomUUID();
                Instant deadline = Instant.now().plusSeconds(invalid || afterScope ? 120 : 8).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                long before = budget.reservedBytes();
                retained = assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                    var writer = new DocumentAssessmentCreation(tx, drives);
                    var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation() + 1, owner.token(), owner.leaseUntil());
                    try { writer.create(caller, wrong, prepared, selected, evidence, id, deadline, budget, () -> {}); throw new AssertionError("wrong owner accepted"); }
                    catch (IllegalArgumentException expected) { require(expected.getMessage().contains("differs from operation owner"), "owner identity refusal"); }
                    try { writer.create(caller, owner, prepared, selected, evidence, id, deadline.plusNanos(1), budget, () -> {}); throw new AssertionError("rounded deadline accepted"); }
                    catch (IllegalArgumentException expected) { require(expected.getMessage().contains("exact microsecond precision"), "deadline precision refusal"); }
                    // Missing claims fail after physical acquisition/sealing and must roll back all rows.
                    try { writer.create(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {}); throw new AssertionError("unstaged schemas accepted"); }
                    catch (RuntimeException expected) {
                        require(hasMessage(expected, "schema artifact is missing") || hasMessage(expected, "exact current-generation artifact claim"), "artifact failure");
                    }
                    require(count(tx, "document_assessment_owners", id) == 0, "owner rollback");
                    require(count(tx, "document_assessment_objects", id) == 0, "physical rollback");
                    var reconciliation = new DocumentAssessmentReconciliation(tx);
                    require(reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, id, deadline, budget, () -> {}).isEmpty(),
                            "absent stage is not observed");
                    new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                    cancelledRootInsert(tx, writer, caller, owner, prepared, selected, evidence, deadline, budget);
                    var result = writer.create(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {});
                    require(reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, id, deadline, budget, () -> {}).orElseThrow().equals(result),
                            "original committed stage acknowledged");
                    try { reconciliation.observe(new RepositoryCaller("other-principal", true), owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected),
                            evidence, id, deadline, budget, () -> {}); throw new AssertionError("other principal acknowledged stage"); }
                    catch (RepositoryException expected) { require(expected.getMessage().contains("principal differs"), "current caller refusal"); }
                    try { reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, UUID.randomUUID(), deadline, budget, () -> {});
                        throw new AssertionError("different assessment identity adopted"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("requested original stage"), "assessment identity refusal"); }
                    require(result.assessment().equals(id) && result.retainUntil().equals(deadline), "exact create result");
                    require(count(tx, "document_assessment_roots", id) == evidence.roots(() -> {}).size(), "complete roots");
                    require(count(tx, "document_assessment_artifacts", id) == evidence.artifacts(() -> {}).size(), "complete schema assets");
                    byte[] stored = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(ByteString.copyFrom(stored).equals(evidence.manifestBytes(() -> {})), "exact observed manifest");
                    var retainedSlots = tx.readOnly(em -> em.createNativeQuery("""
                            SELECT member_id,revision_ordinal,selection_revision,object_id,declaration,source_revision,source_ordinal
                            FROM document_assessment_slots WHERE assessment_id=:id
                            """).setParameter("id", id).getResultList()).stream().map(value -> {
                        Object[] row = (Object[]) value;
                        return new DocumentAssessmentSlots.Slot((String) row[0], ((Number) row[1]).intValue(), ((Number) row[2]).longValue(),
                                (UUID) row[3], (String) row[4], (UUID) row[5], row[6] == null ? null : ((Number) row[6]).intValue());
                    }).toList();
                    try (var snapshot = DocumentAssessmentSlotSnapshot.encode(new DocumentAssessmentSlotSnapshot.Identity(
                            id, owner.key(), owner.generation(), command.sha256(), evidence.manifestSha256(() -> {}), deadline),
                            retainedSlots, budget, () -> {})) {
                        Object[] snapshotRow = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                                SELECT snapshot_codec,snapshot_version,snapshot_bytes,encode(snapshot_sha256,'hex')
                                FROM document_assessment_slot_snapshots WHERE assessment_id=:id
                                """).setParameter("id", id).getSingleResult());
                        require(snapshotRow[0].equals(DocumentAssessmentSlotSnapshot.CODEC)
                                && ((Number) snapshotRow[1]).intValue() == DocumentAssessmentSlotSnapshot.VERSION
                                && ByteString.copyFrom((byte[]) snapshotRow[2]).equals(snapshot.bytes())
                                && snapshotRow[3].equals(snapshot.sha256()), "exact retained slot snapshot");
                    }
                    var slotIdentity = new DocumentAssessmentSlotSnapshot.Identity(id, owner.key(), owner.generation(),
                            command.sha256(), evidence.manifestSha256(() -> {}), deadline);
                    verifyRetainedSlots(tx, caller, owner, command, prepared, selected, slotIdentity, budget);
                    var changedSelection = new HashMap<>(selected);
                    var originalSelection = selected.get("a");
                    changedSelection.put("a", new DocumentSelectedAttemptLedger.Selected("a", originalSelection.revision() + 1,
                            originalSelection.attempt(), originalSelection.token()));
                    try { verifyRetainedSlots(tx, caller, owner, command, prepared, changedSelection, slotIdentity, budget);
                        throw new AssertionError("changed selection reconciled"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("original staging identity"), "changed selection refusal"); }
                    try { verifyRetainedSlots(tx, caller, owner, command, prepared, selected,
                            new DocumentAssessmentSlotSnapshot.Identity(id, owner.key(), owner.generation(), command.sha256(),
                                    evidence.manifestSha256(() -> {}), deadline.plusSeconds(1)), budget);
                        throw new AssertionError("extended deadline reconciled"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("original staging identity"), "changed deadline refusal"); }
                    // An uncertain acknowledgement must use reconciliation, not a second create.
                    UUID duplicate = UUID.randomUUID();
                    try { writer.create(caller, owner, prepared, selected, evidence, duplicate, deadline, budget, () -> {}); throw new AssertionError("duplicate generation adopted"); }
                    catch (RuntimeException expected) { require(hasSqlState(expected, "23505"), "duplicate create conflicts"); }
                    require(count(tx, "document_assessment_owners", duplicate) == 0, "duplicate owner rollback");
                    require(count(tx, "document_assessment_owners", id) == 1, "original owner preserved");
                    byte[] afterRetry = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(Arrays.equals(stored, afterRetry), "duplicate create preserves original evidence");
                    if (invalid) {
                        var revised = DocumentAdmissionPolicy.of(policy.definition().toBuilder()
                                .setLimits(policy.definition().getLimits().toBuilder().setMaxRoots(101)).build(), () -> {});
                        var changedPolicy = new DocumentSchemaPolicies(tx).activate(revised, active.revision(), () -> {});
                        require(changedPolicy.revision() > active.revision(), "policy actually advanced");
                        require(reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, id, deadline, budget, () -> {})
                                .orElseThrow().equals(result), "policy advance does not erase original staging acknowledgement");
                        var cancelled = new DocumentPublicationRejections(tx).cancel(caller, owner, command, RepositoryReadControl.NONE);
                        require(cancelled.state() == DocumentPublicationReplay.State.TERMINATED, "explicit cancellation is terminal");
                        try { reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, id, deadline, budget, () -> {});
                            throw new AssertionError("terminal operation acknowledged as active"); }
                        catch (RuntimeException expected) { require(hasMessage(expected, "Repository operation is terminal"), "terminal fence refusal"); }
                    } else if (!afterScope) {
                        // Wait for real database time; never rewrite immutable retention metadata.
                        tx.readOnly(em -> em.createNativeQuery("""
                                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM retain_until-clock_timestamp()))+0.02)
                                FROM document_assessment_owners WHERE assessment_id=:id
                                """).setParameter("id", id).getSingleResult());
                        try { reconciliation.observe(caller, owner, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), evidence, id, deadline, budget, () -> {});
                            throw new AssertionError("expired stage acknowledged as usable"); }
                        catch (IllegalStateException expected) { require(expected.getMessage().contains("unavailable"), "expired stage refusal"); }
                    }
                    require(count(tx, "document_assessment_owners", id) == 1 && count(tx, "document_assessment_slot_snapshots", id) == 1,
                            "expiry or terminal decision does not implicitly delete retained evidence");
                    return result;
                });
                require(budget.reservedBytes() == before, "writer and manifest reservations released");
                long published = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                long decisions = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                require(published == 0 && decisions == (invalid ? 1 : 0),
                        "no publication; only explicitly requested cancellation creates a terminal decision");
            }
            require(budget.reservedBytes() == 0, "assessment reservations released");
            if (afterScope) {
                // No DocumentPublicationAssessment, borrowed evidence, upload plan or
                // registry resolver is passed into this reconstructed request.
                var recoveredCommand = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(command.intent().toByteArray()));
                var recoveredOwner = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(owner.key().account(),
                        owner.key().principal(), UUID.fromString(owner.key().operationId().toString())), owner.generation(),
                        UUID.fromString(owner.token().toString()), Instant.parse(owner.leaseUntil().toString()));
                var recoveryBudget = new PayloadBudget(64_000_000);
                var recovery = new DocumentAssessmentReconciliation(tx);
                var selections = DocumentAssessmentRetainedSlots.uploadSelections(selected);
                AssessmentCaptureFaultProbe.run(database, tx, caller, recoveredOwner, recoveredCommand, selections, retained);
                var assessmentReader = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
                try {
                    assessmentReader.captureAssessment(caller, recoveredOwner, recoveredCommand, selections,
                            retained.assessment(), "00".repeat(32), retained.retainUntil(), recoveryBudget, () -> {});
                    throw new AssertionError("assessment capture accepted a different manifest");
                } catch (IllegalStateException expected) {
                    require(expected.getMessage().contains("requested original stage"), "capture validates retained manifest");
                }
                require(assessmentReader.outstandingReads() == 0, "invalid capture returns capacity");
                var protectedAssessment = assessmentReader.captureAssessment(caller, recoveredOwner, recoveredCommand, selections,
                        retained.assessment(), retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {});
                var retainedUse = protectedAssessment.use();
                require(retainedUse.plan().stage().equals(retained), "canonical evidence capture matches original stage");
                var readEntries = retainedUse.plan().entries();
                require(readEntries.size() == recoveredCommand.intent().getMembersList().stream()
                        .mapToInt(member -> (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum(),
                        "read plan covers every nonempty candidate slot");
                for (var entry : readEntries) {
                    var declared = recoveredCommand.intent().getMembersList().stream()
                            .filter(member -> member.getMemberId().equals(entry.member())).findFirst().orElseThrow()
                            .getParts(entry.revisionOrdinal());
                    require(declared.hasUpload() && declared.getUpload().getSizeBytes() == entry.part().part().size()
                            && declared.getUpload().getSha256().equals(entry.part().part().sha256())
                            && declared.getSlot().getPart() == entry.part().part().part()
                            && declared.getSlot().getSubKey().equals(entry.part().part().subKey()),
                            "read plan preserves candidate ordinals and payload identity");
                    require(entry.part().binding().generation().equals(generation)
                            && entry.part().binding().namespace().equals("namespace")
                            && entry.part().binding().profile().equals(profile), "read plan retains original backend identity");
                    require(entry.part().part().providerVersion() != null && !entry.part().part().providerVersion().isBlank(),
                            "read plan retains provider version");
                }
                protectedAssessment.close();
                require(assessmentReader.releaseDrained(1) == 0, "assessment use blocks release until drain");
                retainedUse.close();
                require(assessmentReader.releaseDrained(1) == 1, "exact assessment session released after drain");
                require(assessmentReader.outstandingReads() == 0, "assessment release returns capacity");
                provider.verifyReads(tx, recoveredOwner, recoveredCommand, selections, retained, recoveryBudget,
                        Map.of("a", a.fragments(), "b", b.fragments()));
                tx.inTransaction(em -> {
                    em.createNativeQuery("SELECT lock_repository_retention_set(array_agg(object_id)) FROM document_assessment_objects WHERE assessment_id=:id")
                            .setParameter("id", retained.assessment()).getSingleResult();
                    em.createNativeQuery("UPDATE repository_object_retention SET retiring=true WHERE object_id IN (SELECT object_id FROM document_assessment_objects WHERE assessment_id=:id)")
                            .setParameter("id", retained.assessment()).executeUpdate();
                });
                var recoveredCapture = assessmentReader.captureAssessment(caller, recoveredOwner, recoveredCommand, selections,
                        retained.assessment(), retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {});
                try (var use = recoveredCapture.use()) {
                    require(use.plan().entries().equals(readEntries), "existing assessment may read its exact retiring objects");
                }
                recoveredCapture.close();
                assessmentReader.fence(); assessmentReader.attestLocalQuiescence();
                require(assessmentReader.recoverQuiescedPins(1) == 1, "assessment recovery finds durable session");
                require(assessmentReader.reconcileDrained(1) == 1, "exact released identity retires local handle");
                require(assessmentReader.recoverQuiescedPins(1) == 0, "assessment reader is fully recovered");
                require(recovery.observeRetained(caller, recoveredOwner, recoveredCommand, selections, retained.assessment(),
                        retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {}).orElseThrow().equals(retained),
                        "closed-scope evidence acknowledged from durable identities");
                try { recovery.observeRetained(caller, recoveredOwner, recoveredCommand, selections, retained.assessment(),
                        "00".repeat(32), retained.retainUntil(), recoveryBudget, () -> {}); throw new AssertionError("different manifest adopted"); }
                catch (IllegalStateException expected) { require(expected.getMessage().contains("requested original stage"), "wrong digest refusal"); }
                var wrongNonce = new RepositoryOperationLedger.Owner(recoveredOwner.key(), recoveredOwner.generation(), UUID.randomUUID(), recoveredOwner.leaseUntil());
                try { recovery.observeRetained(caller, wrongNonce, recoveredCommand, selections, retained.assessment(),
                        retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {}); throw new AssertionError("different owner nonce adopted"); }
                catch (RepositoryOperationLedger.OwnerFencedException expected) { /* exact live-fence refusal */ }
                Thread.currentThread().interrupt();
                try {
                    try { recovery.observeRetained(caller, recoveredOwner, recoveredCommand, selections, retained.assessment(),
                            retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {});
                        throw new AssertionError("interrupted acknowledgement succeeded"); }
                    catch (java.util.concurrent.CancellationException expected) {
                        require(expected.getMessage().contains("acknowledgement interrupted"), "interrupt refusal before SQL");
                    }
                } finally { Thread.interrupted(); }
                var checkpoints = new java.util.concurrent.atomic.AtomicInteger();
                recovery.observeRetained(caller, recoveredOwner, recoveredCommand, selections, retained.assessment(),
                        retained.manifestSha256(), retained.retainUntil(), recoveryBudget, checkpoints::incrementAndGet).orElseThrow();
                int finalCheckpoint = checkpoints.get();
                require(finalCheckpoint > 1, "acknowledgement includes a final control checkpoint");
                checkpoints.set(0);
                try {
                    try { recovery.observeRetained(caller, recoveredOwner, recoveredCommand, selections, retained.assessment(),
                            retained.manifestSha256(), retained.retainUntil(), recoveryBudget, () -> {
                                if (checkpoints.incrementAndGet() == finalCheckpoint) Thread.currentThread().interrupt();
                            });
                        throw new AssertionError("final callback interruption acknowledged success"); }
                    catch (java.util.concurrent.CancellationException expected) {
                        require(expected.getMessage().contains("acknowledgement interrupted"), "final callback interrupt refusal");
                    }
                } finally { Thread.interrupted(); }
                require(recoveryBudget.reservedBytes() == 0, "durable acknowledgement reservations released");
                System.out.println("CLOSED_SCOPE_ASSESSMENT_ACK_OK");
                AssessmentRestartProbe.persist(java.nio.file.Path.of(System.getenv("PROTOMOLT_TEST_RESTART_REQUEST")),
                        recoveredOwner, recoveredCommand, selections, retained);
            }
        }
        System.out.println("OBSERVED_ASSESSMENT_CREATION_OK");
    }
    private static void verifyRetainedSlots(Tx tx, RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentOperationUploadAdmission.Prepared prepared,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentSlotSnapshot.Identity identity, PayloadBudget budget) {
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            em.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE")
                    .setParameter("id", identity.assessment()).getSingleResult();
            DocumentAssessmentRetainedSlots.verify(em, identity, command, DocumentAssessmentRetainedSlots.uploadSelections(selected), budget, () -> {});
        });
    }
    /** Cancel real SQL after owner/physical/schema insertion; no successful backend is simulated. */
    private static void cancelledRootInsert(Tx tx, DocumentAssessmentCreation writer, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared prepared,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentEvidence evidence,
            Instant deadline, PayloadBudget budget) {
        UUID cancelled = UUID.randomUUID();
        long before = budget.reservedBytes();
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_cancel_assessment_root() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN RAISE EXCEPTION 'injected assessment root cancellation' USING ERRCODE='57014'; END $$
                    """).executeUpdate();
            em.createNativeQuery("CREATE TRIGGER test_cancel_assessment_root BEFORE INSERT ON document_assessment_roots "
                    + "FOR EACH ROW EXECUTE FUNCTION test_cancel_assessment_root()").executeUpdate();
        });
        try {
            try { writer.create(caller, owner, prepared, selected, evidence, cancelled, deadline, budget, () -> {});
                throw new AssertionError("cancelled SQL acknowledged creation"); }
            catch (RuntimeException expected) {
                require(hasSqlState(expected, "57014") && hasMessage(expected, "injected assessment root cancellation"), "exact SQL cancellation propagated");
            }
            for (String table : List.of("document_assessment_owners", "document_assessment_slots",
                    "document_assessment_objects", "document_assessment_artifacts", "document_assessment_roots", "document_assessment_slot_snapshots"))
                require(count(tx, table, cancelled) == 0, "cancelled transaction rolled back " + table);
            long references = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                    .setParameter("id", cancelled).getSingleResult()).longValue());
            require(references == 0, "cancelled physical references rolled back");
            require(budget.reservedBytes() == before, "cancelled writer scratch released");
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER test_cancel_assessment_root ON document_assessment_roots").executeUpdate();
                em.createNativeQuery("DROP FUNCTION test_cancel_assessment_root()").executeUpdate();
            });
        }
    }
    private static long count(Tx tx, String table, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static void require(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
    private static boolean hasMessage(Throwable failure, String text) {
        for (var cause = failure; cause != null; cause = cause.getCause())
            if (cause.getMessage() != null && cause.getMessage().contains(text)) return true;
        return false;
    }
    private static boolean hasSqlState(Throwable failure, String state) {
        for (var cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof java.sql.SQLException sql && state.equals(sql.getSQLState())) return true;
        return false;
    }
}
