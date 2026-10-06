package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;

/** Mixed native traffic with explicit warmup, correctness checks and measured operation boundaries. */
final class NativeMixedTrafficProbe {
    private static final RepositoryCaller CALLER = new RepositoryCaller("native-worker", true);
    private record Work(DocumentPublicationCommand command, Map<DocumentPublicationRuntime.PayloadKey,PartObject> bodies, Document document) {}

    static void run(Tx tx, DataSource dataSource, Path root, String worker, OpenedBlobStore provider,
            ManagedBackendLedger.Profile profile) throws Exception {
        int clients = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_CLIENTS"));
        String journalMode = System.getenv("PROTOMOLT_NATIVE_JOURNALED");
        if (!List.of("true", "false").contains(journalMode)) throw new IllegalArgumentException("Explicit journaled mode required");
        boolean journaled = Boolean.parseBoolean(journalMode);
        if (clients < 1 || clients > 16) throw new IllegalArgumentException("Invalid client count");
        int payloadBytes = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_PAYLOAD_BYTES"));
        int measuredIterations = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_ITERATIONS"));
        if (payloadBytes != 0 && (payloadBytes < 256 || payloadBytes > 786_432)) throw new IllegalArgumentException("Invalid payload bytes");
        if (measuredIterations < 8 || measuredIterations > 256 || measuredIterations % 8 != 0) throw new IllegalArgumentException("Invalid iteration count");
        String content = payloadBytes == 0 ? null : payload(worker, payloadBytes);
        int readSlots = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_READ_SLOTS"));
        if (readSlots < 1 || readSlots > 64) throw new IllegalArgumentException("Invalid reader slot count");
        int readHandles = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_READ_HANDLES"));
        if (readHandles < 1 || readHandles > 256) throw new IllegalArgumentException("Invalid read handle count");
        int requestedPool = Integer.parseInt(System.getenv("PROTOMOLT_NATIVE_POOL"));
        require(dataSource instanceof com.zaxxer.hikari.HikariDataSource, "actual Hikari source");
        var pool = (com.zaxxer.hikari.HikariDataSource) dataSource;
        require(pool.getMaximumPoolSize() == requestedPool, "configured SQL pool applied");
        Files.writeString(root.resolve(worker + "-config.txt"), "clients=" + clients + "\npool=" + pool.getMaximumPoolSize() + "\nread_slots=" + readSlots + "\nread_handles=" + readHandles + "\n",
                StandardOpenOption.CREATE_NEW);
        Files.writeString(root.resolve(worker + "-config.txt"), "payload_string_bytes=" + payloadBytes
                + "\niterations_per_client=" + measuredIterations + "\njournaled=" + journaled + "\n", StandardOpenOption.APPEND);
        var telemetry = new NativeTrafficTelemetry(); telemetry.attach(dataSource);
        var measuredStore = telemetry.wrap(provider.store());
        var measuredProvider = new OpenedBlobStore(measuredStore, provider, provider.capabilities(), provider::ensureNamespace, provider.reclaimer());
        var budget = new PayloadBudget(128_000_000);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), readHandles);
        var drives = new DriveLedger(tx);
        var drive = drives.findById(UUID.fromString(Files.readString(root.resolve("drive")))).orElseThrow();
        var placement = Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, "native-replica-s3", profile));
        var reference = DocumentPublicationResult.parseFrom(Files.readAllBytes(root.resolve("r1-0-0.result"))).getMembers(0);
        var expected = Document.parseFrom(Files.readAllBytes(root.resolve("r1-0-0.document")));
        var definition = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        var invalid = ObservedAssessmentProbe.invalidSchema();
        var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
        var output = new ConcurrentLinkedQueue<String>();
        try (var reader = new DocumentPartReader((generation, actual) -> {
            require(generation.equals("native-replica-s3") && actual.equals(profile), "exact read backend"); return measuredStore;
        }, readSlots, 16_000_000, budget)) {
            DocumentPublicationRuntime.Backends backends = (generation, actual) -> {
                require(generation.equals("native-replica-s3") && actual.equals(profile), "exact upload backend");
                return new DocumentPublicationRuntime.Backend(profile.identity(), measuredProvider);
            };
            var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
            var timeouts = new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15));
            var assessments = new DocumentPublicationRuntime.Assessments(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                    Duration.ofMinutes(2), Duration.ofSeconds(1));
            var runtime = journaled ? DocumentPublicationRuntime.journaled(tx, drives, reads, reader, budget, backends, limits,
                    timeouts, 8, Duration.ofMillis(25), Duration.ofMinutes(5),16,4_000_000,32,false,assessments,key -> CALLER)
                    : new DocumentPublicationRuntime(tx, drives, reads, reader, budget, backends, limits,
                    new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 8, Duration.ofMillis(25), Duration.ofMinutes(5),
                    16, 4_000_000, 32, false, assessments);
            try {
                var readReferences = new ArrayList<DocumentPublishedRevision>();
                var readDocuments = new ArrayList<Document>();
                for (int client = 0; client < clients; client++) {
                    var seed = content == null ? null : command(drive.driveId, worker + "-read-seed-" + client, content);
                    readReferences.add(seed == null ? reference : runtime.execute(CALLER, seed.command(), placement, seed.bodies(), Map.of(),
                            Map.of("document", DocumentPublicationRuntime.Mode.TYPED), container,
                            (caller, member, occurrence) -> definition, RepositoryReadControl.NONE).getMembers(0));
                    readDocuments.add(seed == null ? expected : seed.document());
                    Files.writeString(root.resolve(worker + "-config.txt"), "read_document_bytes_" + client + "="
                            + readDocuments.get(client).getSerializedSize() + "\nread_part_bytes_" + client + "="
                            + (seed == null ? DocumentPartCodec.split(expected, PartLayouts.document()).stream().mapToInt(part -> part.bytes().length).sum()
                                    : seed.bodies().values().stream().mapToInt(part -> part.bytes().length).sum())
                            + "\n", StandardOpenOption.APPEND);
                }
                Map<String,long[]> measuredBaseline = null;
                try (var maintenance = new NativeTrafficMaintenance(runtime)) {
                var history = new DocumentHistoricalOperations(reads, reader, budget);
                for (String phase : List.of("warmup", "measure")) {
                    if (phase.equals("measure")) {
                        Files.writeString(root.resolve(worker + ".ready"), "ready", StandardOpenOption.CREATE_NEW);
                        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                        while (!Files.exists(root.resolve(worker.substring(0, worker.lastIndexOf('-')) + ".go"))) {
                            if (System.nanoTime() >= deadline) throw new AssertionError("Traffic start barrier expired");
                            Thread.sleep(10);
                        }
                    }
                    var before = telemetry.snapshot();
                    int iterations = phase.equals("warmup") ? 8 : measuredIterations;
                    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                        var jobs = new ArrayList<Future<?>>();
                        for (int client = 0; client < clients; client++) {
                            final int number = client;
                            jobs.add(executor.submit(() -> {
                                var readReference = readReferences.get(number);
                                var readExpected = readDocuments.get(number);
                                for (int iteration = 0; iteration < iterations; iteration++) {
                                    maintenance.check();
                                    String id = worker + "-" + phase + "-" + number + "-" + iteration;
                                    boolean read = iteration % 2 == 0, reject = iteration % 8 == 7;
                                    Work work = read ? null : command(drive.driveId, id, content);
                                    Object result;
                                    long start = System.nanoTime();
                                    if (read) {
                                        try (var archived = history.readValidated(CALLER, readReference.getAddress(),
                                                UUID.fromString(readReference.getRevisionId()), RepositoryReadControl.NONE)) {
                                            require(archived.document().equals(readExpected), "concurrent historical bytes");
                                            require(archived.publicationRevision() == readReference.getMutationRevision(), "concurrent historical identity");
                                        }
                                        result = null;
                                    } else {
                                        try { result = runtime.execute(CALLER, work.command(), placement, work.bodies(), Map.of(),
                                                Map.of("document", DocumentPublicationRuntime.Mode.TYPED), container,
                                                (caller, member, occurrence) -> reject ? invalid : definition, RepositoryReadControl.NONE); }
                                        catch (DocumentPublicationRuntime.Rejected failure) { result = failure.receipt(); }
                                    }
                                    long elapsed = System.nanoTime() - start;
                                    if (!read) {
                                        require(reject == (result instanceof DocumentPublicationRejection), "expected traffic outcome");
                                        if (reject) {
                                            var receipt = (DocumentPublicationRejection) result;
                                            require(receipt.hasAssessment() && receipt.getReason() == DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_ADMISSION_REJECTED,
                                                    "invalid traffic produces retained admission rejection");
                                        } else require(result instanceof DocumentPublicationResult && ((DocumentPublicationResult) result).getMembersCount() == 1,
                                                "typed traffic produces one publication");
                                        Object replay;
                                        try { replay = runtime.execute(CALLER, work.command(), Map.of(), Map.of(), Map.of(),
                                                Map.of("document", DocumentPublicationRuntime.Mode.TYPED), container,
                                                (caller, member, occurrence) -> { throw new AssertionError("Replay schema lookup"); }, RepositoryReadControl.NONE); }
                                        catch (DocumentPublicationRuntime.Rejected failure) { replay = failure.receipt(); }
                                        require(result.equals(replay), "exact traffic replay");
                                    }
                                    output.add(phase + "," + number + "," + iteration + "," + (read ? "read" : reject ? "reject" : "publish") + "," + start + "," + elapsed);
                                }
                                return null;
                            }));
                        }
                        for (var job : jobs) job.get(90, TimeUnit.SECONDS);
                    }
                    if (phase.equals("warmup")) telemetry.writeDelta(root.resolve(worker + "-warmup-metrics.csv"), before);
                    else measuredBaseline = before;
                }
                Files.writeString(root.resolve(worker + "-operations.csv"), "phase,client,iteration,operation,start_nanos,elapsed_nanos\n"
                        + String.join("\n", output) + "\n", StandardOpenOption.CREATE_NEW);
                }
                telemetry.writeDelta(root.resolve(worker + "-measure-metrics.csv"), Objects.requireNonNull(measuredBaseline));
                Files.writeString(root.resolve(worker + ".done"), "done", StandardOpenOption.CREATE_NEW);
                long releaseDeadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                while (!Files.exists(root.resolve(worker.substring(0, worker.lastIndexOf('-')) + ".release"))) {
                    if (System.nanoTime() >= releaseDeadline) throw new AssertionError("Parent did not release measured SQL window");
                    Thread.sleep(10);
                }
            } finally {
                boolean stopped = false;
                for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                require(stopped, "traffic runtime drains");
            }
            require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0, "traffic resources released");
        }
    }

    private static Work command(UUID drive, String id, String content) {
        var ownership = OwnershipContext.newBuilder().setAccountId("native-replica").setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId(id).setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of(content == null ? "payload-" + id : content), "type.test")).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("document").setDriveId(drive.toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("native-replica").setDocId(id).setGraphId("native-traffic").setGraphAddressId("source")));
        var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey,PartObject>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            bodies.put(new DocumentPublicationRuntime.PayloadKey("document", member.getPartsCount()), part);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes())).setContentType("application/protobuf")));
        }
        return new Work(new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("native-replica")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build()), Map.copyOf(bodies), document);
    }
    private static String payload(String id, int bytes) {
        if (bytes == 0) return "payload-" + id;
        var random = new Random(id.hashCode());
        var chars = new char[bytes];
        for (int i = 0; i < chars.length; i++) chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars); // ASCII: requested bytes are exact before protobuf framing.
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
