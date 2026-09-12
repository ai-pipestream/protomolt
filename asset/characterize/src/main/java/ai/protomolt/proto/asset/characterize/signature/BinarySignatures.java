package ai.protomolt.proto.asset.characterize.signature;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * The published set of byte-pattern signatures for formats generally: a
 * couple of thousand of them, from PDF versions to instrument telemetry,
 * each naming a format the preservation community has written a signature
 * for.
 *
 * <p>The registry this platform routes on names eighteen formats, and
 * these are not a replacement for it. A format earns a registry entry by
 * having something that reads it; a signature hit here is an observation
 * about the bytes and stays one. What it buys is triage: an asset nothing
 * can classify is far more useful when the record says the bytes look like
 * a particular published format than when it says only that the bytes are
 * not any of eighteen.
 *
 * <p>A definition that will not compile is left out by name and the rest
 * of the set still loads.
 */
public final class BinarySignatures {

    private static final String BUNDLED = "binary-signatures.xml.gz";

    private static volatile BinarySignatures bundled;

    /**
     * One published format.
     *
     * @param id the set's own numeric identifier, which priority uses
     * @param formatId the registry's identifier, such as {@code fmt/95}
     * @param name the format's name
     * @param version the format's version, blank when unversioned
     * @param mediaType the format's media type, blank when it has none
     * @param signatureIds the signatures any one of which identifies it
     * @param outranks the numeric identifiers of formats this one
     *        supersedes when both match
     */
    public record Format(String id, String formatId, String name, String version,
                         String mediaType, List<String> signatureIds, List<String> outranks) {

        /** Validates and copies. */
        public Format {
            signatureIds = List.copyOf(signatureIds);
            outranks = List.copyOf(outranks);
        }

        /** The format's name with the version, when it has one. */
        public String label() {
            return version.isBlank() ? name : name + " " + version;
        }
    }

    private final String version;
    private final Map<String, List<SignatureSequence>> signatures;
    private final List<Format> formats;
    private final List<String> skipped;

    private BinarySignatures(String version, Map<String, List<SignatureSequence>> signatures,
                             List<Format> formats, List<String> skipped) {
        this.version = version;
        this.signatures = Map.copyOf(signatures);
        this.formats = List.copyOf(formats);
        this.skipped = List.copyOf(skipped);
    }

    /**
     * The bundled set, decompressed and compiled once.
     *
     * @return the signatures
     */
    public static BinarySignatures bundled() {
        BinarySignatures loaded = bundled;
        if (loaded == null) {
            synchronized (BinarySignatures.class) {
                loaded = bundled;
                if (loaded == null) {
                    loaded = loadBundled();
                    bundled = loaded;
                }
            }
        }
        return loaded;
    }

    private static BinarySignatures loadBundled() {
        try (InputStream in = BinarySignatures.class.getResourceAsStream(BUNDLED)) {
            if (in == null) {
                throw new IllegalStateException("the bundled signature set is not on the "
                        + "classpath");
            }
            return load(new GZIPInputStream(in));
        } catch (IOException unreadable) {
            throw new UncheckedIOException("the bundled signature set could not be read",
                    unreadable);
        }
    }

    /**
     * Loads a signature set.
     *
     * @param in the published XML, uncompressed; the caller closes it
     * @return the signatures
     * @throws IOException if the document cannot be read
     */
    public static BinarySignatures load(InputStream in) throws IOException {
        Document document = SignatureXml.parse(in);
        Element root = document.getDocumentElement();
        List<String> skipped = new ArrayList<>();
        Map<String, List<SignatureSequence>> signatures = new LinkedHashMap<>();
        NodeList declared = root.getElementsByTagName("InternalSignature");
        for (int i = 0; i < declared.getLength(); i++) {
            Element element = (Element) declared.item(i);
            String id = element.getAttribute("ID");
            try {
                List<SignatureSequence> sequences = new ArrayList<>();
                for (Element sequence : SignatureXml.byteSequencesUnder(element)) {
                    sequences.add(SignatureXml.sequence(sequence));
                }
                if (!sequences.isEmpty()) {
                    signatures.put(id, List.copyOf(sequences));
                }
            } catch (SignatureSyntaxException malformed) {
                skipped.add(id + ": " + malformed.getMessage());
            }
        }
        List<Format> formats = new ArrayList<>();
        NodeList published = root.getElementsByTagName("FileFormat");
        for (int i = 0; i < published.getLength(); i++) {
            Element element = (Element) published.item(i);
            List<String> ids = textsOf(element, "InternalSignatureID");
            if (ids.isEmpty()) {
                // A format identified only by extension is not a byte
                // signature, and a name is not evidence about content.
                continue;
            }
            formats.add(new Format(element.getAttribute("ID"),
                    element.getAttribute("PUID"), element.getAttribute("Name"),
                    element.getAttribute("Version"), element.getAttribute("MIMEType"),
                    ids, textsOf(element, "HasPriorityOverFileFormatID")));
        }
        return new BinarySignatures(root.getAttribute("Version"),
                signatures, formats, skipped);
    }

    private static List<String> textsOf(Element parent, String name) {
        List<String> values = new ArrayList<>();
        NodeList children = parent.getElementsByTagName(name);
        for (int i = 0; i < children.getLength(); i++) {
            String text = children.item(i).getTextContent();
            if (text != null && !text.isBlank()) {
                values.add(text.trim());
            }
        }
        return values;
    }

    /** The compiled signatures, by their published identifier. */
    public Map<String, List<SignatureSequence>> signatures() {
        return signatures;
    }

    /** The published formats that a signature can identify. */
    public List<Format> formats() {
        return formats;
    }

    /** Formats by the set's own numeric identifier, for priority lookups. */
    public Map<String, Format> byId() {
        Map<String, Format> index = new HashMap<>();
        for (Format format : formats) {
            index.put(format.id(), format);
        }
        return index;
    }

    /** The published version of this set, for provenance. */
    public String version() {
        return version;
    }

    /** The definitions left out because a pattern in them would not compile. */
    public List<String> skipped() {
        return skipped;
    }
}
