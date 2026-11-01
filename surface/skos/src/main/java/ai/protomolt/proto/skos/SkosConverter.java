package ai.protomolt.proto.skos;

import ai.protomolt.proto.formats.Identifiers;
import ai.protomolt.proto.formats.Rfc3986;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptNotation;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.ConceptSchemeRules;
import ai.protomolt.proto.types.LangLabel;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.irix.IRIx;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.system.StreamRDFWriter;

/**
 * Strict, optional SKOS edge conversion. Internal values stay {@link ConceptScheme}
 * protobufs. Scheme identity is supplied by the caller, as on the config lane;
 * external concept bindings resolve cross-scheme IRIs without network discovery.
 *
 * <p>Inputs must declare exactly the selected scheme and type every concept with
 * {@code skos:Concept} and {@code skos:inScheme}. Unsupported statements are refused.
 * Conversion materializes one scheme in memory; this is not a streaming graph store.
 */
public final class SkosConverter {
    private static final String SKOS = "http://www.w3.org/2004/02/skos/core#";
    private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final String DEPRECATED = "http://www.w3.org/2002/07/owl#deprecated";
    private static final String REPLACED_BY = "http://purl.org/dc/terms/isReplacedBy";
    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final String XSD_BOOLEAN = "http://www.w3.org/2001/XMLSchema#boolean";
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    // UUIDv5(URL namespace, "https://protomolt.ai/ns/skos/concept"). Never change:
    // imported concept IDs are UUIDv5 of the resolved RDF IRI under this namespace.
    private static final UUID CONCEPT_NAMESPACE = UUID.fromString("6583fc39-429f-583c-9abf-cd7ddab29f54");

    /** Explicit external syntaxes; neither enables JSON nor remote fetching. */
    public enum Format {
        /** W3C Turtle. */
        TURTLE(Lang.TURTLE),
        /** W3C RDF/XML. */
        RDF_XML(Lang.RDFXML);

        private final Lang lang;
        Format(Lang lang) { this.lang = lang; }
    }

    private final String schemeId;
    private final String schemeIri;
    private final Map<String, ConceptRef> externalConcepts;
    private final Map<ConceptRef, String> externalIris;

    /**
     * @param schemeId config-lane scheme slug, independent of its external IRI
     * @param schemeIri selected scheme IRI; not persisted inside ConceptScheme
     * @param externalConcepts explicit external IRI to ConceptRef bindings; no aliases
     * @throws IllegalArgumentException for invalid or ambiguous bindings
     */
    public SkosConverter(String schemeId, String schemeIri, Map<String, ConceptRef> externalConcepts) {
        this.schemeId = Objects.requireNonNull(schemeId, "schemeId");
        if (!Identifiers.isSlug(schemeId) || schemeId.length() > 200)
            throw refused("invalid scheme_id: " + schemeId);
        this.schemeIri = requireIri(schemeIri);
        this.externalConcepts = Map.copyOf(externalConcepts);
        var reverse = new HashMap<ConceptRef, String>();
        this.externalConcepts.forEach((iri, ref) -> {
            requireIri(iri);
            if (iri.equals(schemeIri)) throw refused("scheme IRI cannot be an external concept: " + iri);
            requireKnownFields(ref);
            requireValid(VALIDATOR.validate(ref));
            if (ref.getSchemeId().equals(schemeId))
                throw refused("external mapping names this scheme: " + iri);
            if (reverse.putIfAbsent(ref, iri) != null)
                throw refused("ambiguous IRIs for external ConceptRef: " + ref.getSchemeId() + "/" + ref.getConceptId());
        });
        externalIris = Map.copyOf(reverse);
    }

    /**
     * Parses and validates a whole scheme. Distinct RDF statements retain parser order
     * in repeated fields; repeated identical triples have RDF set semantics.
     *
     * @param input caller-supplied RDF bytes; no URL is fetched
     * @param format explicit RDF syntax
     * @return a validated protobuf scheme; no config or repository write occurs
     * @throws IllegalArgumentException for unsupported, malformed or invalid input
     * @throws UncheckedIOException when the input cannot be read
     */
    public ConceptScheme read(InputStream input, Format format) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(format, "format");
        var triples = new LinkedHashSet<Triple>();
        var text = format == Format.TURTLE ? new Utf8Input(input) : null;
        try {
            var parser = RDFParser.create().lang(format.lang).base(schemeIri);
            if (format == Format.TURTLE) {
                // RIOT's default byte decoder replaces malformed UTF-8. A replacement
                // must not become valid label text or a different concept identity.
                parser.source(text);
            } else {
                // RDF/XML's parser owns decoding according to the XML declaration.
                parser.source(input);
            }
            parser
                    .errorHandler(ErrorHandlerFactory.errorHandlerStrict).parse(new StreamRDFBase() {
                        @Override public void triple(Triple triple) { triples.add(triple); }
                    });
        } catch (RuntimeException failure) {
            // The RIOT Reader tokenizer discards the original IOException cause.
            if (text != null && text.failure != null) throw inputFailure(text.failure);
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof IOException io) throw inputFailure(io);
            }
            if (failure instanceof RiotException)
                throw new IllegalArgumentException("SKOS RDF input could not be parsed", failure);
            throw failure;
        }
        if (text != null && text.failure != null) throw inputFailure(text.failure);
        return fromTriples(triples);
    }

    /**
     * Validates and exports a protobuf scheme. Unsupported or unknown protobuf fields
     * fail before any output is written. A sink failure propagates and may leave a prefix.
     * RDF does not preserve protobuf field ordering or unknown fields.
     *
     * @param scheme validated config document to export
     * @param output destination for the selected RDF syntax
     * @param format explicit RDF syntax
     * @throws IllegalArgumentException if the scheme cannot be exported losslessly as supported RDF
     */
    public void write(ConceptScheme scheme, OutputStream output, Format format) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(format, "format");
        requireScheme(scheme);
        var triples = toTriples(scheme);
        if (format == Format.TURTLE) {
            var writer = StreamRDFWriter.getWriterStream(output, RDFFormat.TURTLE_BLOCKS);
            writer.start();
            triples.forEach(writer::triple);
            writer.finish();
        } else {
            var graph = GraphMemFactory.createDefaultGraph();
            try {
                triples.forEach(graph::add);
                RDFDataMgr.write(output, graph, RDFFormat.RDFXML_PLAIN);
            } finally {
                graph.close();
            }
        }
    }

    /**
     * Stable imported ID, independent of label, notation, or scheme membership.
     * @param iri resolved RDF concept IRI, without further normalization
     * @return canonical UUIDv5 under ProtoMolt's fixed concept namespace
     */
    public static String conceptId(String iri) {
        requireIri(iri);
        try {
            var digest = MessageDigest.getInstance("SHA-1");
            digest.update(ByteBuffer.allocate(16).putLong(CONCEPT_NAMESPACE.getMostSignificantBits())
                    .putLong(CONCEPT_NAMESPACE.getLeastSignificantBits()).array());
            byte[] bytes = digest.digest(iri.getBytes(StandardCharsets.UTF_8));
            bytes[6] = (byte) ((bytes[6] & 0x0f) | 0x50);
            bytes[8] = (byte) ((bytes[8] & 0x3f) | 0x80);
            var buffer = ByteBuffer.wrap(bytes);
            return new UUID(buffer.getLong(), buffer.getLong()).toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK SHA-1 unavailable for UUIDv5", impossible);
        }
    }

    private ConceptScheme fromTriples(Set<Triple> triples) {
        var concepts = new LinkedHashMap<String, Concept.Builder>();
        boolean declared = false;
        for (var triple : triples) {
            String subject = iri(triple.getSubject());
            String predicate = iri(triple.getPredicate());
            if (!predicate.equals(RDF_TYPE)) continue;
            String type = iri(triple.getObject());
            if (subject.equals(schemeIri) && type.equals(SKOS + "ConceptScheme")) {
                declared = true;
            } else if (!subject.equals(schemeIri) && type.equals(SKOS + "Concept")) {
                if (externalConcepts.containsKey(subject))
                    throw refused("local concept also has an external binding: " + subject);
                concepts.put(subject, Concept.newBuilder().setId(conceptId(subject)).setIri(subject));
            } else {
                throw refused("unsupported rdf:type " + type + " on " + subject);
            }
        }
        if (!declared) throw refused("missing skos:ConceptScheme declaration for " + schemeIri);
        var scheme = ConceptScheme.newBuilder();
        var members = new HashSet<String>();
        var broader = new LinkedHashSet<Edge>();
        var narrower = new LinkedHashSet<Edge>();
        var deprecated = new HashMap<String, Boolean>();
        for (var triple : triples) {
            String subject = iri(triple.getSubject());
            String predicate = iri(triple.getPredicate());
            Node value = triple.getObject();
            if (predicate.equals(RDF_TYPE)) continue;
            if (subject.equals(schemeIri)) {
                if (!predicate.equals(SKOS + "prefLabel")) throw unsupported(subject, predicate);
                scheme.addPrefLabel(label(value, predicate));
                continue;
            }
            var concept = concepts.get(subject);
            if (concept == null) throw refused("statement on undeclared concept " + subject);
            switch (predicate) {
                case SKOS + "inScheme" -> {
                    if (!iri(value).equals(schemeIri)) throw refused("foreign skos:inScheme on " + subject);
                    members.add(subject);
                }
                case SKOS + "prefLabel" -> concept.addPrefLabel(label(value, predicate));
                case SKOS + "altLabel" -> concept.addAltLabel(label(value, predicate));
                case SKOS + "hiddenLabel" -> concept.addHiddenLabel(label(value, predicate));
                case SKOS + "definition" -> concept.addDefinition(label(value, predicate));
                case SKOS + "scopeNote" -> concept.addScopeNote(label(value, predicate));
                case SKOS + "notation" -> concept.addNotation(notation(value));
                case SKOS + "broader" -> broader.add(new Edge(subject, localTarget(value, concepts)));
                case SKOS + "narrower" -> narrower.add(new Edge(localTarget(value, concepts), subject));
                case SKOS + "related" -> concept.addRelated(concepts.get(localTarget(value, concepts)).getId());
                case SKOS + "exactMatch" -> concept.addExactMatch(external(value));
                case SKOS + "closeMatch" -> concept.addCloseMatch(external(value));
                case SKOS + "broadMatch" -> concept.addBroadMatch(external(value));
                case SKOS + "narrowMatch" -> concept.addNarrowMatch(external(value));
                case SKOS + "relatedMatch" -> concept.addRelatedMatch(external(value));
                case REPLACED_BY -> concept.addReplacedBy(concepts.get(localTarget(value, concepts)).getId());
                case DEPRECATED -> {
                    boolean flag = booleanValue(value);
                    Boolean previous = deprecated.putIfAbsent(subject, flag);
                    if (previous != null && previous != flag) throw refused("conflicting owl:deprecated on " + subject);
                    concept.setDeprecated(flag);
                }
                default -> throw unsupported(subject, predicate);
            }
        }
        if (!broader.isEmpty() && !narrower.isEmpty() && !broader.equals(narrower))
            throw refused("broader/narrower declarations disagree: both directions must name the same edges");
        broader.addAll(narrower);
        broader.forEach(edge -> concepts.get(edge.child()).addBroader(concepts.get(edge.parent()).getId()));
        concepts.forEach((subject, builder) -> {
            if (!members.contains(subject)) throw refused("missing skos:inScheme on " + subject);
            scheme.addConcepts(builder);
        });
        var result = scheme.build();
        requireScheme(result);
        return result;
    }

    private List<Triple> toTriples(ConceptScheme scheme) {
        var triples = new ArrayList<Triple>();
        var iris = new HashMap<String, String>();
        var used = new HashSet<String>();
        for (var concept : scheme.getConceptsList()) {
            if (!concept.hasIri()) throw refused("missing export IRI for concept " + concept.getId());
            String iri = requireIri(concept.getIri());
            if (!used.add(iri) || iri.equals(schemeIri) || externalConcepts.containsKey(iri))
                throw refused("ambiguous export concept IRI " + iri);
            iris.put(concept.getId(), iri);
        }
        add(triples, schemeIri, RDF_TYPE, uri(SKOS + "ConceptScheme"));
        labels(triples, schemeIri, "prefLabel", scheme.getPrefLabelList());
        for (var concept : scheme.getConceptsList()) {
            String subject = iris.get(concept.getId());
            add(triples, subject, RDF_TYPE, uri(SKOS + "Concept"));
            add(triples, subject, SKOS + "inScheme", uri(schemeIri));
            labels(triples, subject, "prefLabel", concept.getPrefLabelList());
            labels(triples, subject, "altLabel", concept.getAltLabelList());
            labels(triples, subject, "hiddenLabel", concept.getHiddenLabelList());
            labels(triples, subject, "definition", concept.getDefinitionList());
            labels(triples, subject, "scopeNote", concept.getScopeNoteList());
            for (var notation : concept.getNotationList()) {
                if (notation.getDatatypeIri().equals(XSD_STRING))
                    throw refused("xsd:string notation must use the canonical empty datatype_iri: " + concept.getId());
                String datatype = notation.getDatatypeIri().isEmpty() ? XSD_STRING : requireIri(notation.getDatatypeIri());
                add(triples, subject, SKOS + "notation", NodeFactory.createLiteralDT(notation.getValue(),
                        TypeMapper.getInstance().getSafeTypeByName(datatype)));
            }
            for (String parent : concept.getBroaderList()) {
                add(triples, subject, SKOS + "broader", uri(iris.get(parent)));
                add(triples, iris.get(parent), SKOS + "narrower", uri(subject));
            }
            for (String related : concept.getRelatedList()) add(triples, subject, SKOS + "related", uri(iris.get(related)));
            mappings(triples, subject, "exactMatch", concept.getExactMatchList());
            mappings(triples, subject, "closeMatch", concept.getCloseMatchList());
            mappings(triples, subject, "broadMatch", concept.getBroadMatchList());
            mappings(triples, subject, "narrowMatch", concept.getNarrowMatchList());
            mappings(triples, subject, "relatedMatch", concept.getRelatedMatchList());
            if (concept.getDeprecated()) add(triples, subject, DEPRECATED,
                    NodeFactory.createLiteralDT("true", XSDDatatype.XSDboolean));
            for (String target : concept.getReplacedByList()) add(triples, subject, REPLACED_BY, uri(iris.get(target)));
        }
        if (new HashSet<>(triples).size() != triples.size())
            throw refused("duplicate protobuf values cannot survive RDF set semantics");
        return triples;
    }

    private void mappings(List<Triple> triples, String subject, String predicate, List<ConceptRef> refs) {
        for (var ref : refs) {
            String target = externalIris.get(ref);
            if (target == null) throw refused("unresolved export mapping " + ref.getSchemeId() + "/" + ref.getConceptId());
            add(triples, subject, SKOS + predicate, uri(target));
        }
    }

    private static void labels(List<Triple> triples, String subject, String predicate, List<LangLabel> labels) {
        for (var label : labels) add(triples, subject, SKOS + predicate,
                NodeFactory.createLiteralLang(label.getText(), label.getLanguage()));
    }

    private static void add(List<Triple> triples, String subject, String predicate, Node value) {
        triples.add(Triple.create(uri(subject), uri(predicate), value));
    }

    private ConceptRef external(Node node) {
        String target = iri(node);
        var ref = externalConcepts.get(target);
        if (ref == null) throw refused("unresolved external mapping " + target);
        return ref;
    }

    private static String localTarget(Node node, Map<String, Concept.Builder> concepts) {
        String target = iri(node);
        if (!concepts.containsKey(target)) throw refused("unknown local concept " + target);
        return target;
    }

    private static LangLabel label(Node node, String predicate) {
        if (!node.isLiteral() || node.getLiteralLanguage().isEmpty() || node.getLiteralBaseDirection() != null)
            throw refused(predicate + " requires a language-tagged, non-directional literal: " + node);
        return LangLabel.newBuilder().setLanguage(node.getLiteralLanguage()).setText(node.getLiteralLexicalForm()).build();
    }

    private static ConceptNotation notation(Node node) {
        if (!node.isLiteral() || !node.getLiteralLanguage().isEmpty())
            throw refused("skos:notation requires a plain or typed literal: " + node);
        String datatype = node.getLiteralDatatypeURI();
        return ConceptNotation.newBuilder().setValue(node.getLiteralLexicalForm())
                .setDatatypeIri(datatype == null || datatype.equals(XSD_STRING) ? "" : datatype).build();
    }

    private static boolean booleanValue(Node node) {
        if (!node.isLiteral() || !XSD_BOOLEAN.equals(node.getLiteralDatatypeURI()))
            throw refused("owl:deprecated requires xsd:boolean: " + node);
        return switch (node.getLiteralLexicalForm()) {
            case "true", "1" -> true;
            case "false", "0" -> false;
            default -> throw refused("invalid owl:deprecated boolean: " + node);
        };
    }

    private void requireScheme(ConceptScheme scheme) {
        requireKnownFields(Objects.requireNonNull(scheme, "scheme"));
        requireValid(VALIDATOR.validate(scheme));
        requireValid(ConceptSchemeRules.check(schemeId, scheme));
    }

    private static void requireKnownFields(Message root) {
        var pending = new ArrayDeque<Message>();
        pending.add(root);
        while (!pending.isEmpty()) {
            var message = pending.removeFirst();
            if (!message.getUnknownFields().asMap().isEmpty())
                throw refused("unknown protobuf fields on " + message.getDescriptorForType().getFullName());
            for (var value : message.getAllFields().values()) {
                if (value instanceof Message nested) pending.add(nested);
                else if (value instanceof String text) requireUnicode(text);
                else if (value instanceof List<?> items) {
                    for (var item : items) {
                        if (item instanceof Message nested) pending.add(nested);
                        else if (item instanceof String text) requireUnicode(text);
                    }
                }
            }
        }
    }

    private static void requireValid(ValidationResult result) {
        if (!result.valid()) throw refused(result.violations().toString());
    }

    private static String iri(Node node) {
        if (!node.isURI()) throw refused("expected IRI; blank nodes and RDF triple terms are unsupported: " + node);
        return requireIri(node.getURI());
    }

    private static String requireIri(String value) {
        Objects.requireNonNull(value, "iri");
        requireUnicode(value);
        if (!Rfc3986.isUri(value)) throw refused("invalid absolute IRI " + value);
        if (!IRIx.create("https://protomolt.ai/").resolve(value).str().equals(value))
            throw refused("IRI is not resolved; RDF resolution would change its identity: " + value);
        var uri = URI.create(value);
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("urn") && !uri.getSchemeSpecificPart().isEmpty()) return value;
        if ((scheme.equals("http") || scheme.equals("https")) && uri.getRawAuthority() != null
                && !uri.getRawAuthority().isEmpty()) return value;
        throw refused("IRI must use http(s) with authority or urn: " + value);
    }

    private static void requireUnicode(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isHighSurrogate(ch)) {
                if (i + 1 == value.length() || !Character.isLowSurrogate(value.charAt(++i)))
                    throw refused("unpaired UTF-16 surrogate in protobuf string");
            } else if (Character.isLowSurrogate(ch)) {
                throw refused("unpaired UTF-16 surrogate in protobuf string");
            }
        }
    }

    private static RuntimeException inputFailure(IOException cause) {
        return cause instanceof CharacterCodingException
                ? new IllegalArgumentException("SKOS malformed UTF-8 input", cause)
                : new UncheckedIOException("SKOS input could not be read", cause);
    }

    private static final class Utf8Input extends Reader {
        private final Reader reader;
        private IOException failure;

        private Utf8Input(InputStream input) {
            reader = new InputStreamReader(input, StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT));
        }

        @Override public int read(char[] target, int offset, int length) throws IOException {
            try {
                return reader.read(target, offset, length);
            } catch (IOException unavailable) {
                failure = unavailable;
                throw unavailable;
            }
        }

        @Override public void close() throws IOException { reader.close(); }
    }

    private static Node uri(String iri) { return NodeFactory.createURI(iri); }
    private static IllegalArgumentException unsupported(String subject, String predicate) {
        return refused("unsupported predicate " + predicate + " on " + subject);
    }
    private static IllegalArgumentException refused(String message) { return new IllegalArgumentException("SKOS: " + message); }
    private record Edge(String child, String parent) {}
}
