package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.codec.DocumentFragmentConfinement;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Typed admission of the parts a v2 save writes. Carried parts keep their
 * references and are not read: document.proto declares no rule that spans
 * parts, so the written fragments are the whole candidate the rules can see.
 * <p>
 * Runs the same engine as publication admission: strict fragment decoding and
 * confinement, the known-Any-location walk, ProtoValidator over the written
 * view, and {@link DocumentPayloadCheck} over every CORE and PARSED Any root,
 * nested Any values included. A rule violation is a {@link Rejected} result; a
 * missing schema, undecodable payload bytes or a registry outage throw distinct
 * exceptions. Nothing here authorizes, retains or commits.
 */
public final class DocumentCandidateAdmission {
    /** Identifies this entry and its rule dialects on the revision. */
    public static final String PROFILE = "protomolt-repo-candidate-admission/v1";

    private DocumentCandidateAdmission() {}

    /** One written fragment, exactly as it will be stored. */
    public record Fragment(DocumentPart part, ByteString bytes) {
        public Fragment { Objects.requireNonNull(part); Objects.requireNonNull(bytes); }
    }

    /** The written fragments of one candidate and the container definition they decode with. */
    public record Request(String docId, List<Fragment> fragments, DocumentSchemaAdmission.Definition container) {
        public Request {
            if (docId == null || docId.isBlank()) throw new IllegalArgumentException("Document identity required");
            fragments = List.copyOf(fragments);
            Objects.requireNonNull(container);
        }
    }

    /** One Any occurrence decoded with an exact schema: root access steps, then its occurrence path. */
    public record Occurrence(DocumentPart part, List<RepositorySchemaOccurrenceStep> steps,
                             RepositorySchemaAssetReference schema, String payloadSha256, long payloadLength) {
        public Occurrence { steps = List.copyOf(steps); }
    }

    /** One rule violation, attributed to the part that holds the field. */
    public record Violation(DocumentPart part, String fieldPath, String ruleId, String message) {}

    /** The admission verdict. Close it after the retained artifacts are persisted. */
    public sealed interface Result extends AutoCloseable {
        @Override void close();
    }

    /**
     * Every rule passed. {@code artifacts} maps SHA-256 to the retained schema bytes
     * (descriptor closures, metadata and sources) every reference names.
     */
    public static final class Accepted implements Result {
        private final RepositorySchemaAssetReference container;
        private final List<Occurrence> occurrences;
        private final Map<String, ByteString> artifacts;
        private final DocumentAdmissionResources resources;

        private Accepted(RepositorySchemaAssetReference container, List<Occurrence> occurrences,
                Map<String, ByteString> artifacts, DocumentAdmissionResources resources) {
            this.container = container; this.occurrences = List.copyOf(occurrences);
            this.artifacts = Map.copyOf(artifacts); this.resources = resources;
        }
        public RepositorySchemaAssetReference container() { return container; }
        public List<Occurrence> occurrences() { return occurrences; }
        public Map<String, ByteString> artifacts() { return artifacts; }
        @Override public void close() { resources.close(); }
    }

    /** At least one rule failed; nothing about the candidate may be committed as valid. */
    public record Rejected(List<Violation> violations) implements Result {
        public Rejected {
            violations = List.copyOf(violations);
            if (violations.isEmpty()) throw new IllegalArgumentException("A rejection names its violations");
        }
        @Override public void close() {}
    }

    /** An Any type URL has no registered schema. Distinct from an outage. */
    public static final class SchemaMissing extends RuntimeException {
        public SchemaMissing(String typeUrl) { super("No registered schema for " + typeUrl); }
    }

    /** Any value bytes do not decode as their declared type. */
    public static final class InvalidPayload extends RuntimeException {
        public InvalidPayload(String message, Throwable cause) { super(message, cause); }
    }

    /** Resolver hosts throw this when the registry cannot be reached; it is never a verdict. */
    public static final class SchemaUnavailable extends RuntimeException {
        public SchemaUnavailable(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * The caller's content cannot be admitted: a fragment does not parse, is
     * not confined to its part, exceeds a structural bound, or places an Any
     * where the layout allows none. A recorded refusal is the right outcome.
     */
    public static final class ContentRefused extends RuntimeException {
        public ContentRefused(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * The host or registry misbehaved: a resolver returned the wrong asset,
     * two definitions claim one identity, retained bytes differ from their
     * metadata, or a host limit was hit. Not the caller's fault and not an
     * outcome to record against its operation ID.
     */
    public static final class HostFailure extends RuntimeException {
        public HostFailure(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Admit the written fragments. The resolver returns null for an unregistered
     * type and throws {@link SchemaUnavailable} for an outage.
     */
    public static Result check(Request request, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations,
            Instant evaluatedAt, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(request); Objects.requireNonNull(resolver); Objects.requireNonNull(limits);
        Objects.requireNonNull(reservations); Objects.requireNonNull(evaluatedAt); Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("candidate admission interrupted");
            control.run();
        };
        active.run();
        if (request.fragments().isEmpty() || request.fragments().size() > limits.maxFragments())
            throw new IllegalArgumentException("candidate fragment count outside limits");
        DocumentSchemaAdmission.Resolver strict = selection -> {
            var definition = resolver.select(selection);
            if (definition == null) throw new SchemaMissing(selection.typeUrl());
            return definition;
        };

        var written = Document.newBuilder();
        long total = 0;
        var seen = new java.util.HashSet<DocumentPart>();
        for (var fragment : request.fragments()) {
            active.run();
            if (fragment.part() != DocumentPart.DOCUMENT_PART_CORE && fragment.part() != DocumentPart.DOCUMENT_PART_BLOBS
                    && fragment.part() != DocumentPart.DOCUMENT_PART_PARSED)
                throw new IllegalArgumentException("candidate admission supports CORE, BLOBS and PARSED fragments");
            if (!seen.add(fragment.part())) throw new IllegalArgumentException("duplicate candidate fragment");
            if (fragment.bytes().size() > limits.maxFragmentBytes() - total)
                throw new ContentRefused("candidate fragments exceed byte limit", null);
            total += fragment.bytes().size();
            try {
                new MessageWireBudget(1_000_000, 64, active).check(fragment.bytes(), Document.getDescriptor());
                var input = fragment.bytes().newCodedInput();
                input.setRecursionLimit(64);
                var parsed = Document.parser().parseFrom(input);
                input.checkLastTagWas(0);
                if (input.getTotalBytesRead() != fragment.bytes().size())
                    throw new InvalidProtocolBufferException("Fragment was not completely consumed");
                DocumentFragmentConfinement.requireConfined(parsed, fragment.part(), request.docId());
                written.mergeFrom(parsed);
            } catch (IllegalArgumentException | InvalidProtocolBufferException refused) {
                throw new ContentRefused(fragment.part() + " fragment: " + refused.getMessage(), refused);
            }
        }
        var view = written.build();
        try {
            DocumentSchemaAdmission.requireKnownDocument(view, "", 0, new int[1], active);
        } catch (IllegalArgumentException refused) {
            throw new ContentRefused(refused.getMessage(), refused);
        }

        var violations = new ArrayList<Violation>();
        var result = DocumentSchemaAdmission.VALIDATOR.validate(view, evaluatedAt);
        for (var violation : result.violations())
            violations.add(new Violation(partOf(violation.path()), violation.path(), violation.ruleId(), violation.message()));

        var resources = new DocumentAdmissionResources(reservations);
        boolean transferred = false;
        try {
            var definitions = new DocumentSchemaDefinitions(limits, resources, active);
            final DocumentSchemaAssetBinding container;
            try {
                container = definitions.add(request.container());
            } catch (IllegalArgumentException host) {
                throw new HostFailure("container definition: " + host.getMessage(), host);
            }
            var containerKey = new DocumentPayloadCheck.SchemaKey(request.container().metadata().getTypeUrl(),
                    request.container().metadata().getArtifactSha256());
            var occurrences = new ArrayList<Occurrence>();
            long decodedRemaining = limits.maxDecodedBytes();
            for (var fragment : request.fragments()) {
                active.run();
                if (fragment.part() == DocumentPart.DOCUMENT_PART_BLOBS) continue;
                var slot = DocumentPublicationSlot.newBuilder().setPart(fragment.part()).build();
                final DocumentAnyRootInventory.Result inventory;
                try {
                    inventory = DocumentAnyRootInventory.inspect(container.schema(), slot, fragment.bytes(), request.docId(),
                            new DocumentAnyRootInventory.Limits(Integer.MAX_VALUE, 1_000_000, 64, limits.maxRoots(), 10000), active);
                } catch (IllegalArgumentException refused) {
                    throw new ContentRefused(fragment.part() + " roots: " + refused.getMessage(), refused);
                }
                for (var root : inventory.roots()) {
                    active.run();
                    if (decodedRemaining < 1) throw new ContentRefused("candidate decoded payload budget exhausted", null);
                    var locator = DocumentSchemaRootProjection.project(inventory, root, active);
                    var envelope = root.envelope();
                    var selected = select(definitions, strict, new DocumentSchemaAdmission.Selection(0, locator,
                            envelope.getTypeUrl(), List.of(), DocumentSchemaOccurrences.sha256(envelope.getValue(), active),
                            envelope.getValue().size()));
                    final DocumentPayloadAssessment assessment;
                    try {
                        assessment = DocumentPayloadCheck.assessContextualAssets(selected, envelope, envelope.getTypeUrl(),
                                DocumentSchemaAdmission.VALIDATOR,
                                new DocumentPayloadCheck.Limits((int) decodedRemaining, 1_000_000, 64, 4096, 65536), active,
                                nested -> select(definitions, strict, new DocumentSchemaAdmission.Selection(0, locator,
                                        nested.typeUrl(), DocumentSchemaOccurrenceProjection.projectSteps(nested.prefix(),
                                                definitions.schemas, active), nested.valueSha256(), nested.valueSizeBytes())),
                                DocumentSchemaOccurrences.Limits.DEFAULT, evaluatedAt);
                    } catch (InvalidProtocolBufferException invalid) {
                        throw new InvalidPayload("Any value does not decode as " + envelope.getTypeUrl(), invalid);
                    } catch (IllegalArgumentException refused) {
                        throw new ContentRefused("payload at " + envelope.getTypeUrl() + ": " + refused.getMessage(), refused);
                    }
                    List<DocumentSchemaOccurrences.Occurrence> found;
                    if (assessment instanceof DocumentPayloadAssessment.Accepted accepted) {
                        found = accepted.checked().payload().occurrences();
                        decodedRemaining -= accepted.checked().payload().decodedBytes();
                    } else {
                        var invalid = (DocumentPayloadAssessment.Invalid) assessment;
                        found = invalid.occurrences();
                        decodedRemaining -= invalid.decodedBytes();
                        var failure = invalid.failure();
                        violations.add(new Violation(fragment.part(), failure.fieldPath(), failure.ruleId(),
                                "payload rule failed at " + failure.rulePath()));
                    }
                    for (var occurrence : found) {
                        active.run();
                        var steps = new ArrayList<>(locator.getAccessList());
                        steps.addAll(DocumentSchemaOccurrenceProjection.projectSteps(occurrence.path(), definitions.schemas, active));
                        var last = occurrence.path().getLast();
                        if (!(last instanceof DocumentSchemaOccurrences.Boundary boundary))
                            throw new IllegalStateException("occurrence path does not end at an Any boundary");
                        var reference = definitions.references.get(
                                new DocumentPayloadCheck.SchemaKey(boundary.typeUrl(), boundary.artifactSha256()));
                        if (reference == null) throw new IllegalStateException("occurrence schema was not selected");
                        occurrences.add(new Occurrence(fragment.part(), steps, reference.toProto(),
                                boundary.valueSha256(), boundary.valueSizeBytes()));
                    }
                }
            }
            active.run();
            if (!violations.isEmpty()) return new Rejected(violations);
            var accepted = new Accepted(definitions.references.get(containerKey).toProto(), occurrences,
                    definitions.artifacts, resources);
            transferred = true;
            return accepted;
        } catch (DocumentAdmissionResources.ReservationFailure failed) {
            throw failed.original;
        } finally {
            if (!transferred) resources.close();
        }
    }

    /**
     * Refuses a request document that carries fields outside this server's Document
     * schema, at any depth, or an Any at a location the layout does not allow. For
     * the request-structure step, before any outcome can be recorded; the same walk
     * runs again inside {@link #check} as a backstop.
     *
     * @throws ContentRefused when the document is not entirely within the schema
     */
    public static void requireKnownFields(Document document) {
        try {
            DocumentSchemaAdmission.requireKnownDocument(document, "", 0, new int[1], () -> {});
        } catch (IllegalArgumentException refused) {
            throw new ContentRefused(refused.getMessage(), refused);
        }
    }

    /** A resolver's answer is host state: a bad one is a host failure, not a content refusal. */
    private static DocumentSchemaAssetBinding select(DocumentSchemaDefinitions definitions,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Selection selection) {
        try {
            return definitions.select(resolver, selection);
        } catch (IllegalArgumentException host) {
            throw new HostFailure("schema resolution for " + selection.typeUrl() + ": " + host.getMessage(), host);
        }
    }

    /** The layout's part fields: anything else is CORE. */
    private static DocumentPart partOf(String path) {
        if (path.startsWith("blob_bag")) return DocumentPart.DOCUMENT_PART_BLOBS;
        if (path.startsWith("parser_results")) return DocumentPart.DOCUMENT_PART_PARSED;
        if (path.startsWith("search_metadata.semantic_results")) return DocumentPart.DOCUMENT_PART_CHUNKS;
        return DocumentPart.DOCUMENT_PART_CORE;
    }
}
