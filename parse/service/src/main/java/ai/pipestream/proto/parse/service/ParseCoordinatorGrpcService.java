package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.service.ContentTypeSniffer.Sniff;
import ai.pipestream.proto.parse.service.ParserClient.ParseOutcome;
import ai.pipestream.proto.parse.service.RoutingRules.RoutingContext;
import ai.pipestream.proto.parse.v1.ParseCoordinatorServiceGrpc;
import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.parse.v1.ParseDocumentResponse;
import ai.pipestream.proto.parse.v1.PlannedParse;
import ai.pipestream.proto.parse.v1.RouteDocumentRequest;
import ai.pipestream.proto.parse.v1.RouteDocumentResponse;
import ai.pipestream.proto.parse.v1.SearchMetadataFold;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.ParseStatus;
import ai.pipestream.proto.repo.v1.ParserResult;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The parsing coordinator: routes stored documents to parsers by CEL rule,
 * fans the parse out concurrently on virtual threads, records one
 * {@link ParserResult} per parser (a failed parse is stored, never silently
 * absent), folds the parsers' document claims into the arbitrated
 * {@link SearchMetadata}, and persists the PARSED part (and folded CORE)
 * back through repo-service.
 *
 * <p>Reads are part-masked: the coordinator fetches CORE+BLOBS only — PARSED
 * and CHUNKS are its outputs, not its inputs. Writes are serialized per
 * document node (a per-address lock) because parser results share the PARSED
 * part: until repo grows a {@code parser_results_written} selector,
 * concurrent partial saves of the same row would drop sibling entries.
 */
public final class ParseCoordinatorGrpcService
        extends ParseCoordinatorServiceGrpc.ParseCoordinatorServiceImplBase {

    /** The connector identity the coordinator stamps on its saves. */
    public static final String CONNECTOR_ID = "parse-coordinator";

    /** How many leading bytes the content sniffer looks at. */
    static final int SNIFF_HEAD_BYTES = 512;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final RoutingRules rules;
    private final ParserRegistry parsers;
    private final String fallbackDrive;
    private final Duration parseDeadline;
    private final ConcurrentHashMap<String, ReentrantLock> saveLocks = new ConcurrentHashMap<>();

    /**
     * @param documents the repo-service document stub every read and save
     *        goes through
     * @param rules the compiled routing-rule set
     * @param parsers the parser fleet
     * @param fallbackDrive drive for the parsed-part save when the loaded
     *        document's read response does not name one
     * @param parseDeadline per-parser deadline of one {@code Parse} stream
     */
    public ParseCoordinatorGrpcService(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            RoutingRules rules,
            ParserRegistry parsers,
            String fallbackDrive,
            Duration parseDeadline) {
        if (documents == null) {
            throw new IllegalArgumentException("documents stub must not be null");
        }
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        if (parsers == null) {
            throw new IllegalArgumentException("parsers must not be null");
        }
        if (fallbackDrive == null || fallbackDrive.isBlank()) {
            throw new IllegalArgumentException("fallbackDrive must not be blank");
        }
        if (parseDeadline == null || parseDeadline.isZero() || parseDeadline.isNegative()) {
            throw new IllegalArgumentException("parseDeadline must be positive");
        }
        this.documents = documents;
        this.rules = rules;
        this.parsers = parsers;
        this.fallbackDrive = fallbackDrive;
        this.parseDeadline = parseDeadline;
    }

    @Override
    public void routeDocument(
            RouteDocumentRequest request, StreamObserver<RouteDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> route(request));
    }

    @Override
    public void parseDocument(
            ParseDocumentRequest request, StreamObserver<ParseDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> parse(request));
    }

    // ------------------------------------------------------------- RouteDocument

    private RouteDocumentResponse route(RouteDocumentRequest request) {
        Document document = switch (request.getSourceCase()) {
            case ADDRESS -> load(requireAddress(request.getAddress())).document();
            case DOCUMENT -> request.getDocument();
            case SOURCE_NOT_SET -> throw GrpcErrors.invalidArgument(
                    "source is required: set address or document");
        };
        Routing routing = routingOf(document);
        return RouteDocumentResponse.newBuilder()
                .addAllPlannedParses(rules.plan(routing.context()))
                .setContentType(routing.sniff().mimeType())
                .setContentTypeSniffed(routing.sniff().sniffed())
                .build();
    }

    // ------------------------------------------------------------- ParseDocument

    private ParseDocumentResponse parse(ParseDocumentRequest request) {
        NodeAddress address = requireAddress(request.getAddress());
        Loaded loaded = load(address);
        Routing routing = routingOf(loaded.document());

        List<PlannedParse> planned = request.getParserOverrideCount() > 0
                ? overridePlan(request.getParserOverrideList())
                : rules.plan(routing.context());
        planned = dedupeByParser(planned);

        List<TaskResult> outcomes = fanOut(planned, loaded.document(), routing);
        Fold fold = fold(outcomes);

        persist(address, loaded, outcomes, fold);

        ParseDocumentResponse.Builder response = ParseDocumentResponse.newBuilder();
        outcomes.forEach(t -> response.putParserResults(t.result().getParserName(), t.result()));
        SearchMetadataFold.Builder foldMessage = SearchMetadataFold.newBuilder()
                .addAllFoldedFields(fold.values().keySet())
                .putAllWinners(fold.winners());
        return response.setSearchMetadataFold(foldMessage).build();
    }

    /** One PlannedParse per override name: rule matching is BYPASSED. */
    private static List<PlannedParse> overridePlan(List<String> parserNames) {
        List<PlannedParse> planned = new ArrayList<>(parserNames.size());
        for (String name : parserNames) {
            if (name.isBlank()) {
                throw GrpcErrors.invalidArgument("parser_override entries must not be blank");
            }
            planned.add(PlannedParse.newBuilder().setParserName(name).build());
        }
        return planned;
    }

    /**
     * The parser_results map keys on parser name, so only one planned entry
     * per parser can run meaningfully; the highest-priority entry wins.
     */
    private static List<PlannedParse> dedupeByParser(List<PlannedParse> planned) {
        Map<String, PlannedParse> byParser = new LinkedHashMap<>();
        planned.forEach(p -> byParser.putIfAbsent(p.getParserName(), p));
        return List.copyOf(byParser.values());
    }

    /** Runs every planned parse concurrently on virtual threads, in plan order. */
    private List<TaskResult> fanOut(List<PlannedParse> planned, Document document, Routing routing) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<TaskResult>> futures = new ArrayList<>(planned.size());
            for (PlannedParse plan : planned) {
                futures.add(executor.submit(() -> runOne(plan, document, routing)));
            }
            List<TaskResult> outcomes = new ArrayList<>(futures.size());
            for (Future<TaskResult> future : futures) {
                outcomes.add(future.get());
            }
            return outcomes;
        } catch (ExecutionException e) {
            throw new IllegalStateException("parse fan-out task failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("parse fan-out interrupted", e);
        }
    }

    /** One parser's run, reduced to the ParserResult that will be stored. */
    private TaskResult runOne(PlannedParse plan, Document document, Routing routing) {
        Timestamp now = timestampNow();
        ParserResult.Builder result = ParserResult.newBuilder()
                .setParserName(plan.getParserName())
                .setParsedDate(now)
                .setConfigFingerprint(
                        ConfigFingerprints.fingerprint(plan.getMatchedRuleId(), plan.getParserConfig()));

        Optional<ParserClient> client = parsers.lookup(plan.getParserName());
        if (client.isEmpty()) {
            return new TaskResult(plan,
                    result.setStatus(ParseStatus.PARSE_STATUS_FAILED)
                            .setError("no registered parser '" + plan.getParserName() + "'")
                            .build(),
                    Map.of());
        }
        try {
            result.setParserVersion(client.get().info().getParserVersion());
            ParseOptions options = ParseOptions.newBuilder()
                    .setDocumentId(document.getDocId())
                    .setFilename(routing.context().filename())
                    .setContentType(routing.context().mimeType())
                    .setConfig(plan.getParserConfig())
                    .build();
            ParseOutcome outcome = client.get().parse(options, routing.payload(), parseDeadline);
            if (outcome.failed()) {
                return new TaskResult(plan,
                        result.setStatus(ParseStatus.PARSE_STATUS_FAILED)
                                .setError(outcome.error())
                                .build(),
                        Map.of());
            }
            List<String> warnings = outcome.output().getWarningsList();
            if (warnings.isEmpty()) {
                result.setStatus(ParseStatus.PARSE_STATUS_OK);
            } else {
                result.setStatus(ParseStatus.PARSE_STATUS_PARTIAL)
                        .setError(String.join("; ", warnings));
            }
            result.setDocument(outcome.output().getDocument());
            // Later claims replace earlier ones per key, per the contract.
            Map<String, Value> claims = new LinkedHashMap<>();
            outcome.claims().forEach(c -> claims.putAll(c.getClaims().getFieldsMap()));
            return new TaskResult(plan, result.build(), claims);
        } catch (RuntimeException e) {
            // A crashed task is still a recorded FAILED result.
            String message = e.getMessage();
            return new TaskResult(plan,
                    result.setStatus(ParseStatus.PARSE_STATUS_FAILED)
                            .setError(message == null || message.isBlank()
                                    ? e.getClass().getSimpleName() : message)
                            .build(),
                    Map.of());
        }
    }

    // ------------------------------------------------------------- the fold

    /**
     * Arbitrates the claims: for each claimed key naming a string-valued
     * {@link SearchMetadata} field, the claim of the highest-priority parser
     * (plan order) wins. Unknown keys, non-string claims, and blank strings
     * are ignored — a blank claim must never fold over a real value.
     */
    private static Fold fold(List<TaskResult> outcomes) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        LinkedHashMap<String, String> winners = new LinkedHashMap<>();
        for (TaskResult outcome : outcomes) {
            for (Map.Entry<String, Value> claim : outcome.claims().entrySet()) {
                if (values.containsKey(claim.getKey())
                        || claim.getValue().getKindCase() != Value.KindCase.STRING_VALUE
                        || claim.getValue().getStringValue().isBlank()) {
                    continue;
                }
                FieldDescriptor field =
                        SearchMetadata.getDescriptor().findFieldByName(claim.getKey());
                if (field == null || field.isRepeated()
                        || field.getJavaType() != FieldDescriptor.JavaType.STRING) {
                    continue;
                }
                values.put(claim.getKey(), claim.getValue().getStringValue());
                winners.put(claim.getKey(), outcome.result().getParserName());
            }
        }
        return new Fold(values, winners);
    }

    // ------------------------------------------------------------- persistence

    /**
     * Persists the run: the loaded document plus the new parser_results and
     * the folded SearchMetadata fields, saved as a partial write of
     * [PARSED, CORE] with everything else carried forward from the source
     * address. Serialized per document node — see the class javadoc.
     */
    private void persist(NodeAddress address, Loaded loaded, List<TaskResult> outcomes, Fold fold) {
        Document.Builder merged = loaded.document().toBuilder();
        outcomes.forEach(t -> merged.putParserResults(t.result().getParserName(), t.result()));
        if (!fold.values().isEmpty()) {
            SearchMetadata.Builder metadata = merged.getSearchMetadataBuilder();
            fold.values().forEach((name, value) -> metadata
                    .setField(SearchMetadata.getDescriptor().findFieldByName(name), value));
        }
        SaveDocumentRequest save = SaveDocumentRequest.newBuilder()
                .setDocument(merged.build())
                .setDrive(loaded.drive().isBlank() ? fallbackDrive : loaded.drive())
                .setConnectorId(CONNECTOR_ID)
                .setUseDatasourceId(true)
                .setGraphId(address.getGraphId())
                .setWrittenBy(WriteProvenance.newBuilder().setModuleId(CONNECTOR_ID))
                .addPartsWritten(DocumentPart.DOCUMENT_PART_PARSED)
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CORE)
                .setCopyUnwrittenPartsFrom(address)
                .build();
        ReentrantLock lock = saveLocks.computeIfAbsent(lockKey(address), key -> new ReentrantLock());
        lock.lock();
        try {
            documents.saveDocument(save);
        } finally {
            lock.unlock();
        }
    }

    private static String lockKey(NodeAddress address) {
        return address.getDocId() + '|' + address.getGraphAddressId() + '|'
                + address.getAccountId() + '|' + address.getGraphId();
    }

    // ------------------------------------------------------------- loading

    private Loaded load(NodeAddress address) {
        GetDocumentResponse response = documents.getDocumentByReference(
                GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(address)
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_BLOBS)
                        .build());
        return new Loaded(response.getDocument(), response.getDrive());
    }

    private static NodeAddress requireAddress(NodeAddress address) {
        if (address.getDocId().isBlank()) {
            throw GrpcErrors.invalidArgument("address.doc_id is required");
        }
        if (address.getGraphAddressId().isBlank()) {
            throw GrpcErrors.invalidArgument("address.graph_address_id is required");
        }
        if (address.getAccountId().isBlank()) {
            throw GrpcErrors.invalidArgument("address.account_id is required");
        }
        if (address.getGraphId().isBlank()) {
            throw GrpcErrors.invalidArgument("address.graph_id is required");
        }
        return address;
    }

    // ------------------------------------------------------------- routing context

    /** Builds the routing context (sniff + declared + identity) for one document. */
    private Routing routingOf(Document document) {
        Blob blob = firstBlob(document);
        byte[] payload = payloadOf(document);
        byte[] head = payload.length <= SNIFF_HEAD_BYTES
                ? payload
                : Arrays.copyOf(payload, SNIFF_HEAD_BYTES);

        String declared = "";
        String filename = "";
        long sizeBytes = payload.length;
        if (blob != null) {
            declared = blob.hasMimeType() ? blob.getMimeType() : "";
            filename = blob.hasFilename() ? blob.getFilename() : "";
            if (blob.getSizeBytes() > 0) {
                sizeBytes = blob.getSizeBytes();
            }
        }
        if (declared.isBlank() && document.getSearchMetadata().hasSourceMimeType()) {
            declared = document.getSearchMetadata().getSourceMimeType();
        }
        Sniff sniff = ContentTypeSniffer.resolve(head, filename, declared);
        RoutingContext context = new RoutingContext(
                sniff.mimeType(),
                declared,
                filename,
                ContentTypeSniffer.extensionOf(filename),
                sizeBytes,
                document.getOwnership().getAccountId());
        return new Routing(context, sniff, payload);
    }

    /**
     * The payload bytes of the document's first blob: inline data verbatim,
     * a storage_ref fetched through {@code DocumentService.GetBlob} (the
     * coordinator never touches object storage itself). No blob → empty.
     */
    private byte[] payloadOf(Document document) {
        Blob blob = firstBlob(document);
        if (blob == null) {
            return new byte[0];
        }
        return switch (blob.getContentCase()) {
            case DATA -> blob.getData().toByteArray();
            case STORAGE_REF -> documents.getBlob(GetBlobRequest.newBuilder()
                            .setStorageRef(blob.getStorageRef())
                            .build())
                    .getData()
                    .toByteArray();
            case CONTENT_NOT_SET -> new byte[0];
        };
    }

    /** The FIRST blob of the bag — the routing representative. */
    private static Blob firstBlob(Document document) {
        BlobBag bag = document.getBlobBag();
        return switch (bag.getBlobDataCase()) {
            case BLOB -> bag.getBlob();
            case BLOBS -> bag.getBlobs().getBlobCount() > 0 ? bag.getBlobs().getBlob(0) : null;
            case BLOBDATA_NOT_SET -> null;
        };
    }

    private static Timestamp timestampNow() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    /** A document loaded by reference, with the drive the row lives on. */
    private record Loaded(Document document, String drive) {
    }

    /** The routing decision inputs for one document. */
    private record Routing(RoutingContext context, Sniff sniff, byte[] payload) {
    }

    /** One planned parse's reduced outcome: the stored result + final claims. */
    private record TaskResult(PlannedParse plan, ParserResult result, Map<String, Value> claims) {
    }

    /** The arbitrated fold: field → value, field → winning parser. */
    private record Fold(Map<String, String> values, Map<String, String> winners) {
    }
}
