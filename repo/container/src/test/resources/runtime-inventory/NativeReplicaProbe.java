package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.s3.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Small native workload, separate from fault/expiry fixtures and their mutable seeds. */
public final class NativeReplicaProbe {
    private static final String ACCOUNT = "native-replica", GENERATION = "native-replica-s3";
    private static final RepositoryCaller CALLER = new RepositoryCaller("native-worker", true);
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[1]);
        var options = Map.of("endpoint", env("PROTOMOLT_TEST_S3_ENDPOINT"), "region", "us-east-1",
                "path-style", "true", "conditional-writes", "true", "access-key", env("PROTOMOLT_TEST_S3_ACCESS"),
                "secret-key", env("PROTOMOLT_TEST_S3_SECRET"));
        var profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(options.get("endpoint"), "us-east-1", true), "native-replica");
        try (var database = new LedgerDatabase(new LedgerConfig(env("PROTOMOLT_TEST_JDBC"), env("PROTOMOLT_TEST_USER"), env("PROTOMOLT_TEST_PASSWORD"),
                Integer.parseInt(System.getenv().getOrDefault("PROTOMOLT_NATIVE_POOL", "10")), LedgerConfig.DEFAULT_MIGRATION_LOCATION));
                var opened = new S3BlobStoreProvider().open(options)) {
            var tx = new Tx(database.entityManagerFactory());
            switch (args[0]) {
                case "seed" -> seed(tx, root, options, profile);
                case "write" -> write(tx, root, args[2], opened, profile);
                case "read" -> read(tx, root, opened, profile);
                case "traffic" -> NativeMixedTrafficProbe.run(tx, database.dataSource(), root, args[2], opened, profile);
                default -> throw new IllegalArgumentException("Unknown native workload mode");
            }
        }
        System.out.println("NATIVE_REPLICA_" + args[0].toUpperCase(Locale.ROOT) + "_OK");
    }

    private static void seed(Tx tx, Path root, Map<String,String> options, ManagedBackendLedger.Profile profile) throws Exception {
        try (var client = software.amazon.awssdk.services.s3.S3Client.builder()
                .endpointOverride(java.net.URI.create(options.get("endpoint"))).forcePathStyle(true)
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(options.get("access-key"), options.get("secret-key")))).build()) {
            client.createBucket(b -> b.bucket("native-replica"));
            client.putBucketVersioning(b -> b.bucket("native-replica").versioningConfiguration(v -> v.status(
                    software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = ACCOUNT;
        drive.name = "native-replica"; drive.bucket = "native-replica"; drive.prefix = "root";
        drive.provider = "s3"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
        new DriveLedger(tx).insert(drive);
        new ManagedBackendLedger(tx).bind(GENERATION, profile);
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(ACCOUNT)
                .setValidationProfile(DocumentSchemaAdmission.PROFILE).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(20).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        new DocumentSchemaPolicies(tx).activate(policy, 0, () -> {});
        Files.writeString(root.resolve("drive"), drive.driveId.toString(), StandardOpenOption.CREATE_NEW);
    }

    private static void write(Tx tx, Path root, String worker, OpenedBlobStore opened, ManagedBackendLedger.Profile profile) throws Exception {
        boolean race = worker.startsWith("race-");
        var baseline = race ? DocumentPublicationResult.parseFrom(Files.readAllBytes(root.resolve("r1-0-0.result"))).getMembers(0) : null;
        var drives = new DriveLedger(tx);
        var drive = drives.findById(UUID.fromString(Files.readString(root.resolve("drive")))).orElseThrow();
        var placement = Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, GENERATION, profile));
        var budget = new PayloadBudget(128_000_000);
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID(), 8);
        try (var reader = reader(opened, profile, budget)) {
            var runtime = new DocumentPublicationRuntime(tx, drives, ledger, reader, budget, (generation, actual) -> {
                require(GENERATION.equals(generation) && profile.equals(actual), "exact upload profile");
                return new DocumentPublicationRuntime.Backend(profile.identity(), opened);
            }, LIMITS, new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25), Duration.ofMinutes(5),
                    4, 4_000_000, 8, false, new DocumentPublicationRuntime.Assessments(Path.of(env("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                            Duration.ofMinutes(2), Duration.ofSeconds(1)));
            try {
                var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
                var valid = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
                var invalid = ObservedAssessmentProbe.invalidSchema();
                Files.writeString(root.resolve(worker + ".ready"), "ready", StandardOpenOption.CREATE_NEW);
                Path release = root.resolve(worker.substring(0, worker.indexOf('-')) + ".go");
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (!Files.exists(release)) {
                    if (System.nanoTime() >= deadline) throw new AssertionError("Writer start barrier expired");
                    Thread.sleep(10);
                }
                for (int i = 0; i < (race ? 1 : 3); i++) {
                    String id = worker + "-" + i;
                    var ownership = OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("source")
                            .setSecurity(DocumentSecurity.getDefaultInstance()).build();
                    var document = Document.newBuilder().setDocId(race ? baseline.getAddress().getDocId() : id).setOwnership(ownership)
                            .setStructuredData(Any.pack(StringValue.of("payload-" + id), "type.test")).build();
                    var member = DocumentPublicationMember.newBuilder().setMemberId("document").setDriveId(drive.driveId.toString())
                            .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                            .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                                    .setAccountId(ACCOUNT).setDocId(id).setGraphId("native-replica").setGraphAddressId("source")));
                    if (race) member.setDestination(DocumentRevisionCondition.newBuilder().setAddress(baseline.getAddress())
                            .setExpectedMutationRevision(baseline.getMutationRevision()));
                    var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey,PartObject>();
                    for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
                        bodies.put(new DocumentPublicationRuntime.PayloadKey("document", member.getPartsCount()), part);
                        member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                                .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                                .setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes())).setContentType("application/protobuf")));
                    }
                    var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                            .setAccountId(ACCOUNT).setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
                    var modes = Map.of("document", DocumentPublicationRuntime.Mode.TYPED);
                    boolean rejected = i == 2;
                    var definition = rejected ? invalid : valid;
                    Object first;
                    long start = System.nanoTime();
                    try { first = runtime.execute(CALLER, command, placement, bodies, Map.of(), modes, container,
                            (caller, selected, occurrence) -> {
                                if (race) raceValidationBarrier(root, worker);
                                return definition;
                            }, RepositoryReadControl.NONE); }
                    catch (DocumentPublicationRuntime.Rejected failure) { first = failure.receipt(); }
                    long nanos = System.nanoTime() - start;
                    if (!race) require(rejected == (first instanceof DocumentPublicationRejection), "expected admission outcome");
                    Object replay;
                    try { replay = runtime.execute(CALLER, command, Map.of(), Map.of(), Map.of(), modes, container,
                            (caller, selected, occurrence) -> { throw new AssertionError("Replay performed schema lookup"); }, RepositoryReadControl.NONE); }
                    catch (DocumentPublicationRuntime.Rejected failure) { replay = failure.receipt(); }
                    require(first.equals(replay), "exact terminal replay");
                    Files.write(root.resolve(id + ".intent"), command.intent().toByteArray(), StandardOpenOption.CREATE_NEW);
                    if (first instanceof DocumentPublicationResult result) {
                        require(result.getMembersCount() == 1, "one publication");
                        Files.write(root.resolve(id + ".result"), result.toByteArray(), StandardOpenOption.CREATE_NEW);
                        Files.write(root.resolve(id + ".document"), document.toByteArray(), StandardOpenOption.CREATE_NEW);
                    } else {
                        var receipt = (DocumentPublicationRejection) first;
                        if (race) {
                            require(receipt.getReason() == DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET,
                                    "losing writer has an explicit precondition rejection");
                            require(!receipt.hasAssessment(), "revision conflict is not a schema rejection");
                        } else require(receipt.hasAssessment(), "rejection retains assessment identity");
                        Files.write(root.resolve(id + ".rejection"), receipt.toByteArray(), StandardOpenOption.CREATE_NEW);
                    }
                    Files.writeString(root.resolve(worker + ".csv"), id + "," + (first instanceof DocumentPublicationRejection ? "rejected" : "accepted") + "," + nanos + "\n",
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            } finally {
                boolean stopped = false;
                for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                require(stopped, "runtime shutdown drains");
            }
            require(ledger.outstandingReads() == 0 && budget.reservedBytes() == 0, "worker releases resources");
        }
    }

    private static void read(Tx tx, Path root, OpenedBlobStore opened, ManagedBackendLedger.Profile profile) throws Exception {
        var budget = new PayloadBudget(128_000_000);
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID(), 8);
        try (var reader = reader(opened, profile, budget)) {
            var lifecycle = new DocumentReadLifecycle(ledger, reader, 8);
            try (var files = Files.list(root)) {
                var history = new DocumentHistoricalOperations(ledger, reader, budget);
                for (var file : files.filter(p -> p.toString().endsWith(".result")).toList()) {
                    var result = DocumentPublicationResult.parseFrom(Files.readAllBytes(file));
                    var revision = result.getMembers(0);
                    var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Files.readAllBytes(
                            Path.of(file.toString().replace(".result", ".intent")))));
                    require(new DocumentPublicationReplay(tx).observe(CALLER, command).result().orElseThrow().equals(result),
                            "fresh-process terminal result observation");
                    if (file.getFileName().toString().startsWith("race-")) {
                        var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(revision.getAddress());
                        var current = new DocumentLedger(tx).findByNodeId(node).orElseThrow();
                        require(current.mutationRevision == revision.getMutationRevision(), "current head is the winning mutation");
                        require(new DocumentPublicationLedger(tx).findForRead(current).orElseThrow().revisionId()
                                .equals(UUID.fromString(revision.getRevisionId())), "current head is the winning immutable revision");
                        long revisions = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_revision_commits WHERE node_id=:node")
                                .setParameter("node", node).getSingleResult()).longValue());
                        require(revisions == 2, "exactly original plus one winning revision");
                    }
                    var expected = Document.parseFrom(Files.readAllBytes(Path.of(file.toString().replace(".result", ".document"))));
                    try (var archived = history.readValidated(CALLER, revision.getAddress(), UUID.fromString(revision.getRevisionId()), RepositoryReadControl.NONE)) {
                        require(archived.document().equals(expected), "cross-process retained historical document");
                        require(archived.publicationRevision() == revision.getMutationRevision(), "original mutation revision");
                        require(archived.commandSha256().equals(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()))),
                                "historical proof binds original command");
                        require(archived.manifest().getPartsList().stream().filter(p -> p.getState() == PartState.PART_STATE_PRESENT)
                                .allMatch(p -> !p.getSha256().isBlank()), "manifest checksums retained");
                        require(archived.metadata().getKnown().getAccountId().equals(ACCOUNT), "cross-process historical ownership");
                    }
                }
                try (var rejected = Files.list(root)) {
                    for (var file : rejected.filter(p -> p.toString().endsWith(".rejection")).toList()) {
                        var receipt = DocumentPublicationRejection.parseFrom(Files.readAllBytes(file));
                        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Files.readAllBytes(
                                Path.of(file.toString().replace(".rejection", ".intent")))));
                        require(new DocumentPublicationReplay(tx).observe(CALLER, command).rejection().orElseThrow().equals(receipt),
                                "fresh-process retained rejection observation");
                        var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(command.intent().getMembers(0).getDestination().getAddress());
                        if (command.intent().getMembers(0).getDestination().hasIfAbsent())
                            require(new DocumentLedger(tx).findByNodeId(node).isEmpty(), "rejected create has no normal document");
                        else require(receipt.getReason() == DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET,
                                "rejected update preserves the winning document");
                        long revisions = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:operation")
                                .setParameter("operation", command.operationId()).getSingleResult()).longValue());
                        require(revisions == 0, "rejected create has no published revision");
                    }
                }
            } finally {
                boolean stopped = false;
                for (int pass = 0; pass < 4 && !stopped; pass++) stopped = lifecycle.shutdownStep(Duration.ofSeconds(5));
                require(stopped, "reader shutdown drains");
            }
            require(ledger.outstandingReads() == 0 && budget.reservedBytes() == 0, "reader releases resources");
        }
    }
    private static void raceValidationBarrier(Path root, String worker) {
        try {
            Files.writeString(root.resolve(worker + ".schema-ready"), "schema-ready", StandardOpenOption.CREATE_NEW);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!Files.exists(root.resolve("race-0.schema-ready")) || !Files.exists(root.resolve("race-1.schema-ready"))) {
                if (System.nanoTime() >= deadline) throw new AssertionError("Competing writers did not both reach schema resolution");
                Thread.sleep(10);
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt(); throw new AssertionError("Race barrier interrupted", failure);
        } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    private static DocumentPartReader reader(OpenedBlobStore opened, ManagedBackendLedger.Profile profile, PayloadBudget budget) {
        return new DocumentPartReader((generation, actual) -> {
            require(GENERATION.equals(generation) && profile.equals(actual), "exact read profile"); return opened.store();
        }, 2, 16_000_000, budget);
    }
    private static String env(String key) { return Objects.requireNonNull(System.getenv(key), key); }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
