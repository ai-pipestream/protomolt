package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.signature.SignatureSequence;
import ai.protomolt.proto.asset.characterize.signature.SignatureSyntaxException;
import ai.protomolt.proto.asset.characterize.signature.SignatureXml;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The published rules for telling containers apart by what is inside them.
 *
 * <p>Each rule names a format and lists the members that prove it: a
 * member that has to be present, or one whose opening bytes have to match
 * a pattern. All of a rule's members have to hold for the rule to hold.
 *
 * <p>A rule that will not compile is left out by name rather than
 * discarded silently, and the rest of the set still loads. Signature
 * definitions are data published on a schedule; one malformed pattern
 * should cost one format, not every format.
 */
public final class ContainerSignatures {

    private static final String BUNDLED = "container-signatures.xml";

    private static volatile ContainerSignatures bundled;

    /**
     * One member a rule tests.
     *
     * @param path the member's path inside the container
     * @param pattern the pattern its opening bytes must match, or null when
     *        the rule asks only that the member be present
     */
    public record MemberRule(String path, SignatureSequence pattern) {
    }

    /**
     * One rule: a format, and what proves it.
     *
     * @param id the rule's identifier in the published set
     * @param kind the container structure the rule applies to
     * @param description the format's name, as published
     * @param formatId the registry's identifier for the format
     * @param members the members that must all hold
     */
    public record Rule(String id, ContainerKind kind, String description, String formatId,
                       List<MemberRule> members) {
    }

    private final String version;
    private final Map<ContainerKind, List<Rule>> byKind = new EnumMap<>(ContainerKind.class);
    private final List<String> skipped;

    private ContainerSignatures(String version, List<Rule> rules, List<String> skipped) {
        this.version = version;
        this.skipped = List.copyOf(skipped);
        for (ContainerKind kind : ContainerKind.values()) {
            byKind.put(kind, rules.stream().filter(rule -> rule.kind() == kind).toList());
        }
    }

    /**
     * The bundled rule set, loaded once.
     *
     * @return the rules
     */
    public static ContainerSignatures bundled() {
        ContainerSignatures loaded = bundled;
        if (loaded == null) {
            synchronized (ContainerSignatures.class) {
                loaded = bundled;
                if (loaded == null) {
                    loaded = loadBundled();
                    bundled = loaded;
                }
            }
        }
        return loaded;
    }

    private static ContainerSignatures loadBundled() {
        try (InputStream in = ContainerSignatures.class.getResourceAsStream(BUNDLED)) {
            if (in == null) {
                throw new IllegalStateException("the bundled rule set is not on the classpath");
            }
            return load(in);
        } catch (IOException unreadable) {
            throw new UncheckedIOException("the bundled rule set could not be read", unreadable);
        }
    }

    /**
     * Loads a rule set.
     *
     * @param in the published XML; the caller closes it
     * @return the rules
     * @throws IOException if the document cannot be read
     */
    public static ContainerSignatures load(InputStream in) throws IOException {
        Document document = SignatureXml.parse(in);
        Element root = document.getDocumentElement();
        Map<String, String> formatIds = new HashMap<>();
        NodeList mappings = root.getElementsByTagName("FileFormatMapping");
        for (int i = 0; i < mappings.getLength(); i++) {
            Element mapping = (Element) mappings.item(i);
            formatIds.put(mapping.getAttribute("signatureId"), mapping.getAttribute("Puid"));
        }
        List<Rule> rules = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        NodeList declared = root.getElementsByTagName("ContainerSignature");
        for (int i = 0; i < declared.getLength(); i++) {
            Element element = (Element) declared.item(i);
            String id = element.getAttribute("Id");
            try {
                Rule rule = rule(element, id, formatIds.get(id));
                if (rule != null) {
                    rules.add(rule);
                }
            } catch (SignatureSyntaxException malformed) {
                skipped.add(id + ": " + malformed.getMessage());
            }
        }
        return new ContainerSignatures(root.getAttribute("signatureVersion"), rules, skipped);
    }

    private static Rule rule(Element element, String id, String formatId)
            throws SignatureSyntaxException {
        ContainerKind kind = kindOf(element.getAttribute("ContainerType"));
        if (kind == null) {
            return null;
        }
        List<MemberRule> members = new ArrayList<>();
        NodeList files = element.getElementsByTagName("File");
        for (int i = 0; i < files.getLength(); i++) {
            Element file = (Element) files.item(i);
            NodeList paths = file.getElementsByTagName("Path");
            if (paths.getLength() == 0) {
                continue;
            }
            String path = paths.item(0).getTextContent().trim();
            List<Element> sequences = SignatureXml.byteSequencesUnder(file);
            if (sequences.isEmpty()) {
                members.add(new MemberRule(path, null));
                continue;
            }
            for (Element sequence : sequences) {
                members.add(new MemberRule(path, SignatureXml.sequence(sequence)));
            }
        }
        if (members.isEmpty()) {
            return null;
        }
        NodeList descriptions = element.getElementsByTagName("Description");
        String description = descriptions.getLength() == 0
                ? "" : descriptions.item(0).getTextContent().trim();
        return new Rule(id, kind, description, formatId == null ? "" : formatId, members);
    }

    private static ContainerKind kindOf(String declared) {
        if ("ZIP".equals(declared)) {
            return ContainerKind.ZIP;
        }
        if ("OLE2".equals(declared)) {
            return ContainerKind.OLE2;
        }
        return null;
    }

    /**
     * The rules for one container structure.
     *
     * @param kind the structure
     * @return the rules, in published order
     */
    public List<Rule> rulesFor(ContainerKind kind) {
        return byKind.getOrDefault(kind, List.of());
    }

    /** The published version of this rule set, for provenance. */
    public String version() {
        return version;
    }

    /** The rules left out because a pattern in them would not compile. */
    public List<String> skipped() {
        return skipped;
    }
}
