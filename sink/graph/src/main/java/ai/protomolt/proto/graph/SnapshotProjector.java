package ai.protomolt.proto.graph;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.formats.Rfc3986;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.ListLinksResponse;
import ai.protomolt.proto.repo.v2.OccurrenceLocator;
import ai.protomolt.proto.repo.v2.PartSchemaBinding;
import ai.protomolt.proto.repo.v2.ResolvedOccurrence;
import ai.protomolt.proto.repo.v2.SourceRevision;
import ai.protomolt.proto.skos.SkosConverter;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;

/**
 * Rebuildable, trusted snapshot projection. The caller owns the Dataset and its
 * lifecycle. Use a dedicated dataset: its control graph is internal derived state,
 * not repository authority or an end-user authorization mechanism.
 */
public final class SnapshotProjector {
    /** Derived vocabulary; binary literals contain existing protobuf messages. */
    public static final String NS = "urn:protomolt:graph:v1:";
    /** Internal revision guards survive document graph deletion. */
    public static final String CONTROL_GRAPH = NS + "control";
    private static final Property REVISION = property("revision");
    private static final Property DELETED = property("deleted");
    private static final Property VERSION = property("mappingVersion");
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private final Dataset dataset;
    private final String mappingVersion;
    private final Map<String, String> relations;
    private final Authorization authorization;

    /**
     * Trusted integration gate, invoked on every call including replays and deletes.
     * Must throw unless the source, snapshot and evidence belong to the same exact
     * authorized revision. References alone do not prove artifact retention. This
     * callback must check that association using genuine reader evidence; it must
     * not treat caller-supplied identifiers as permission. No permissive default exists.
     */
    @FunctionalInterface
    public interface Authorization {
        /**
         * Authorize and verify the supplied association, or throw with its cause.
         * Evidence is null for tombstones; schemeOccurrence is supplied only for schemes.
         */
        void requireAuthorized(SourceRevision source, Message snapshot, Evidence evidence, ResolvedOccurrence schemeOccurrence);
    }

    /**
     * Retained CORE binding and resolved Any occurrences from the same revision.
     * These are recorded as existing protobuf messages, not reconstructed from type URLs.
     * Links keep their own (field_path, ordinal) identities; an Any locator is not
     * fabricated for a link. A scheme must supply its genuine resolved occurrence.
     */
    public record Evidence(PartSchemaBinding core, List<ResolvedOccurrence> occurrences) {
        /** Copy the evidence list to prevent mutation after authorization. */
        public Evidence {
            Objects.requireNonNull(core, "retained CORE binding required");
            occurrences = List.copyOf(occurrences);
        }
    }

    /** Result distinguishes an exact replay from a committed replacement. */
    public enum Result { REPLACED, REPLAYED }

    /**
     * @param dataset dedicated dataset supporting atomic abortable transactions
     * @param mappingVersion explicit immutable mapping release identifier
     * @param relations exhaustive static relation-to-absolute-URI table; no inference
     * @param authorization trusted exact-revision authorization/evidence gate
     */
    public SnapshotProjector(Dataset dataset, String mappingVersion, Map<String, String> relations,
                             Authorization authorization) {
        this.dataset = Objects.requireNonNull(dataset, "dataset");
        if (!dataset.supportsTransactions() || !dataset.supportsTransactionAbort())
            throw new IllegalArgumentException("An abortable transactional Dataset is required");
        requireText(mappingVersion, "mapping version");
        this.mappingVersion = mappingVersion;
        this.relations = Map.copyOf(relations);
        this.relations.forEach((relation, iri) -> {
            requireText(relation, "relation");
            if (!Rfc3986.isUri(iri) || iri.startsWith(NS)) throw new IllegalArgumentException("Mapping predicate must be an absolute URI");
        });
        this.authorization = Objects.requireNonNull(authorization, "authorization gate required");
    }

    /** Project links from a complete exact-revision ListLinks response. */
    public Result replaceLinks(SourceRevision source, ListLinksResponse links, Evidence evidence) {
        authorize(source, links, evidence, null);
        if (links.getRevision() != source.getRevision()) throw new IllegalArgumentException("Snapshot revision mismatch");
        requireEvidence(evidence);
        try (ModelScope model = new ModelScope(base(source, evidence, "links"))) {
            var root = model.value.createResource(sourceIri(source));
            var seen = new HashSet<String>();
            var stableLinks = links.toBuilder().clearLinks();
            for (var link : links.getLinksList()) {
                String occurrenceIri = sourceIri(source) + ":link:"
                        + encode(link.getFieldPath()) + ":" + link.getOrdinal();
                if (!seen.add(occurrenceIri)) throw new IllegalArgumentException("Duplicate link occurrence");
                requireText(link.getFieldPath(), "field path");
                var occurrence = model.value.createResource(occurrenceIri);
                root.addProperty(property("occurrence"), occurrence);
                occurrence.addLiteral(property("fieldPath"), link.getFieldPath())
                        .addLiteral(property("ordinal"), link.getOrdinal())
                        .addLiteral(property("relation"), link.getRelation());
                if (link.hasLabel()) occurrence.addLiteral(property("label"), link.getLabel());
                if (link.hasStaticTarget()) {
                    var target = link.getStaticTarget();
                    stableLinks.addLinks(link.toBuilder().setStaticTarget(target.toBuilder()
                            .clearTargetTombstoned().clearTargetUnavailable()));
                    requireAddress(target.getAddress());
                    if (!target.getAddress().getAccountId().equals(source.getAddress().getAccountId()))
                        throw new IllegalArgumentException("Cross-account link refused");
                    String predicate = relations.get(link.getRelation());
                    if (predicate == null) throw new IllegalArgumentException("Unmapped static relation: " + link.getRelation());
                    String targetIri = target.hasPinnedRevision()
                            ? sourceIri(SourceRevision.newBuilder().setAddress(target.getAddress())
                                    .setRevision(target.getPinnedRevision()).build())
                            : documentIri(target.getAddress());
                    var targetResource = model.value.createResource(targetIri);
                    root.addProperty(model.value.createProperty(predicate), targetResource);
                    occurrence.addProperty(property("target"), targetResource)
                            .addProperty(property("predicate"), model.value.createResource(predicate));
                    // Deliberately exclude reader-relative availability/tombstone flags.
                    binary(targetResource, "addressProto", target.getAddress());
                    if (target.hasPinnedRevision()) targetResource.addLiteral(REVISION, target.getPinnedRevision());
                } else {
                    stableLinks.addLinks(link);
                    binary(occurrence, "selectorProto", link.getDynamicTarget());
                }
            }
            root.addLiteral(property("snapshotSha256"), MessageFingerprints.fingerprint(stableLinks.build()));
            return replace(source, model.value, false);
        }
    }

    /**
     * Export a typed scheme through the existing strict converter. The supplied
     * occurrence must be one of the retained evidence entries. The trusted gate
     * establishes payload/schema/source association; this method does not decode Any.
     */
    public Result replaceScheme(SourceRevision source, ConceptScheme scheme, Evidence evidence,
                                ResolvedOccurrence occurrence, SkosConverter converter) {
        authorize(source, scheme, evidence, occurrence);
        requireEvidence(evidence);
        if (!evidence.occurrences().contains(Objects.requireNonNull(occurrence, "scheme occurrence required")))
            throw new IllegalArgumentException("Scheme occurrence absent from retained evidence");
        String typeUrl = occurrence.getSchema().getTypeUrl();
        if (!typeUrl.substring(typeUrl.lastIndexOf('/') + 1).equals(ConceptScheme.getDescriptor().getFullName()))
            throw new IllegalArgumentException("Retained occurrence must name ConceptScheme");
        var bytes = new ByteArrayOutputStream();
        Objects.requireNonNull(converter, "converter").write(scheme, bytes, SkosConverter.Format.TURTLE);
        try (ModelScope model = new ModelScope(base(source, evidence, "scheme"))) {
            try (var rdf = new ModelScope(ModelFactory.createDefaultModel())) {
                RDFParser.fromString(bytes.toString(StandardCharsets.UTF_8), Lang.TURTLE).parse(rdf.value);
                var triples = rdf.value.getGraph().find();
                try {
                    triples.forEachRemaining(triple -> {
                        for (var node : List.of(triple.getSubject(), triple.getPredicate(), triple.getObject()))
                            if (node.isURI() && node.getURI().startsWith(NS))
                                throw new IllegalArgumentException("SKOS IRI overlaps reserved projection namespace");
                    });
                } finally { triples.close(); }
                model.value.add(rdf.value);
            }
            binary(model.value.createResource(sourceIri(source)), "schemeOccurrenceProto", occurrence);
            // Preserve typed input identity, including ordering not represented by RDF.
            model.value.createResource(sourceIri(source)).addLiteral(property("snapshotSha256"),
                    MessageFingerprints.fingerprint(scheme));
            return replace(source, model.value, false);
        }
    }

    /** Remove the document graph using a genuine authorized tombstone; retain its revision guard. */
    public Result delete(DocumentManifest tombstone) {
        Objects.requireNonNull(tombstone, "tombstone");
        var source = SourceRevision.newBuilder().setAddress(tombstone.getAddress())
                .setRevision(tombstone.getRevision()).build();
        authorize(source, tombstone, null, null);
        if (!tombstone.getTombstone()) throw new IllegalArgumentException("An explicit tombstone is required");
        try (ModelScope empty = new ModelScope(ModelFactory.createDefaultModel())) {
            return replace(source, empty.value, true);
        }
    }

    /** Stable full-address document identity, with unambiguous protobuf framing. */
    public static String documentIri(NodeAddress address) {
        requireAddress(address);
        return NS + "document:" + MessageFingerprints.fingerprint(address);
    }

    /** One mutable derived graph per full document address. Revision is recorded inside it. */
    public static String graphIri(NodeAddress address) { return documentIri(address) + ":graph"; }

    /** Exact source identity; does not resolve a mutable current revision. */
    public static String sourceIri(SourceRevision source) {
        if (source.getRevision() <= 0) throw new IllegalArgumentException("An exact positive revision is required");
        return documentIri(source.getAddress()) + ":revision:" + source.getRevision();
    }

    private void authorize(SourceRevision source, Message snapshot, Evidence evidence, ResolvedOccurrence schemeOccurrence) {
        sourceIri(source);
        Objects.requireNonNull(snapshot, "snapshot");
        authorization.requireAuthorized(source, snapshot, evidence, schemeOccurrence);
        valid(source);
        valid(snapshot);
    }

    private static void requireEvidence(Evidence evidence) {
        Objects.requireNonNull(evidence, "retained evidence required");
        valid(evidence.core());
        if (evidence.core().getPart().getPart() != DocumentPart.DOCUMENT_PART_CORE)
            throw new IllegalArgumentException("A retained CORE binding is required");
        var seen = new HashSet<OccurrenceLocator>();
        for (var occurrence : evidence.occurrences()) {
            valid(occurrence);
            if (!occurrence.getOccurrence().getPart().equals(evidence.core().getPart()))
                throw new IllegalArgumentException("Occurrence must belong to the supplied CORE part");
            if (!seen.add(occurrence.getOccurrence())) throw new IllegalArgumentException("Duplicate retained occurrence");
        }
    }

    private Model base(SourceRevision source, Evidence evidence, String kind) {
        var model = ModelFactory.createDefaultModel();
        var root = model.createResource(sourceIri(source));
        root.addProperty(property("document"), model.createResource(documentIri(source.getAddress())))
                .addLiteral(REVISION, source.getRevision()).addLiteral(VERSION, mappingVersion)
                .addLiteral(property("kind"), kind);
        binary(root, "sourceProto", source);
        binary(root, "coreSchemaProto", evidence.core());
        for (var occurrence : evidence.occurrences()) binary(root, "resolvedOccurrenceProto", occurrence);
        relations.forEach((relation, iri) -> root.addProperty(property("relationMapping"),
                model.createResource(NS + "relation:" + encode(relation)).addProperty(property("predicate"),
                        model.createResource(iri))));
        return model;
    }

    private Result replace(SourceRevision source, Model candidate, boolean deleted) {
        if (dataset.isInTransaction()) throw new IllegalStateException("Projector must own its transaction");
        String graph = graphIri(source.getAddress());
        dataset.begin(ReadWrite.WRITE);
        Throwable primaryFailure = null;
        try {
            var control = dataset.getNamedModel(CONTROL_GRAPH);
            var state = control.createResource(graph);
            var previous = state.getProperty(REVISION);
            if (previous != null) {
                long revision = previous.getLong();
                if (source.getRevision() < revision) throw new IllegalStateException("Stale snapshot revision");
                if (source.getRevision() == revision) {
                    if (state.getRequiredProperty(DELETED).getBoolean() != deleted
                            || !state.getRequiredProperty(VERSION).getString().equals(mappingVersion)
                            || !dataset.getNamedModel(graph).isIsomorphicWith(candidate))
                        throw new IllegalStateException("Conflicting snapshot replay or mapping version");
                    dataset.commit();
                    return Result.REPLAYED;
                }
            }
            dataset.removeNamedModel(graph);
            if (!deleted) dataset.addNamedModel(graph, candidate);
            state.removeProperties().addLiteral(REVISION, source.getRevision())
                    .addLiteral(DELETED, deleted).addLiteral(VERSION, mappingVersion);
            dataset.commit();
            return Result.REPLACED;
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            try { dataset.abort(); } catch (RuntimeException | Error abort) { failure.addSuppressed(abort); }
            throw failure;
        } finally {
            try { dataset.end(); }
            catch (RuntimeException | Error end) {
                if (primaryFailure == null) throw end;
                primaryFailure.addSuppressed(end);
            }
        }
    }

    private static void binary(Resource resource, String name, Message message) {
        resource.addLiteral(property(name), Base64.getEncoder().encodeToString(MessageFingerprints.deterministicBytes(message)));
    }

    private static Property property(String name) { return ResourceFactory.createProperty(NS + name); }
    private static String encode(String text) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
    private static void requireText(String text, String name) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException(name + " is required");
        requireUnicode(text, name);
    }
    private static void requireUnicode(String text, String name) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
                    throw new IllegalArgumentException("Malformed UTF-16 in " + name);
            } else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("Malformed UTF-16 in " + name);
        }
    }
    private static void requireAddress(NodeAddress address) {
        known(address);
        requireText(address.getAccountId(), "account");
        requireText(address.getGraphId(), "graph");
        requireText(address.getGraphAddressId(), "graph address");
        requireText(address.getDocId(), "document");
    }
    private static void valid(Message message) {
        known(message);
        var result = VALIDATOR.validate(message);
        if (!result.valid()) throw new IllegalArgumentException("Invalid snapshot/evidence: " + result);
    }
    private static void known(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unknown fields in " + message.getDescriptorForType().getFullName());
        message.getAllFields().forEach((field, value) -> {
            if (field.getJavaType() == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING) {
                if (field.isRepeated()) for (Object item : (List<?>) value) requireUnicode((String) item, field.getFullName());
                else requireUnicode((String) value, field.getFullName());
            }
            if (field.getJavaType() == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                if (field.isRepeated()) for (Object item : (List<?>) value) known((Message) item);
                else known((Message) value);
            }
        });
    }

    // Jena Model predates AutoCloseable. Keep every temporary model scoped.
    private static final class ModelScope implements AutoCloseable {
        private final Model value;
        private ModelScope(Model value) { this.value = value; }
        @Override public void close() { value.close(); }
    }
}
