package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Anchor;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Fragment;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Subsequence;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reading signature definitions out of the XML the registries publish
 * them in.
 *
 * <p>Both published sets describe a pattern the same way: a byte sequence
 * with an anchor, holding subsequences with distances, each optionally
 * flanked by fragments. This turns that description into a
 * {@link SignatureSequence}, so the container set and the binary set share
 * one reader as well as one matcher.
 */
public final class SignatureXml {

    private static final String ANCHOR_ATTRIBUTE = "Reference";
    private static final String TRAILING_ANCHOR = "EOFoffset";
    private static final String UNANCHORED = "Variable";

    private SignatureXml() {
    }

    /**
     * Parses a signature document with external entity resolution off.
     *
     * @param in the document; the caller closes it
     * @return the parsed document
     * @throws IOException if the document cannot be read or parsed
     */
    public static Document parse(InputStream in) throws IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            // Signature sets arrive as data. Data does not get to name
            // files for the parser to go and fetch.
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setEntityResolver((publicId, systemId) -> {
                throw new SAXException("external entities are not resolved");
            });
            return builder.parse(in);
        } catch (ParserConfigurationException | SAXException unreadable) {
            throw new IOException("the signature document could not be read", unreadable);
        }
    }

    /**
     * Builds a signature from one {@code ByteSequence} element.
     *
     * @param byteSequence the element
     * @return the compiled signature
     * @throws SignatureSyntaxException if any pattern in it will not compile
     */
    public static SignatureSequence sequence(Element byteSequence)
            throws SignatureSyntaxException {
        Anchor anchor = anchorOf(byteSequence.getAttribute(ANCHOR_ATTRIBUTE));
        List<Element> parts = childrenNamed(byteSequence, "SubSequence");
        if (parts.isEmpty()) {
            throw new SignatureSyntaxException("", 0, "the byte sequence has no subsequence");
        }
        parts.sort(Comparator.comparingInt(part -> number(part, "Position", 1)));
        List<Subsequence> subsequences = new ArrayList<>();
        for (Element part : parts) {
            subsequences.add(subsequence(part));
        }
        return new SignatureSequence(anchor, subsequences);
    }

    /**
     * Every {@code ByteSequence} under an element, in document order.
     *
     * @param parent the element to look under
     * @return the elements
     */
    public static List<Element> byteSequencesUnder(Element parent) {
        List<Element> found = new ArrayList<>();
        NodeList all = parent.getElementsByTagName("ByteSequence");
        for (int i = 0; i < all.getLength(); i++) {
            found.add((Element) all.item(i));
        }
        return found;
    }

    /**
     * An absent anchor means unanchored, not leading.
     *
     * <p>A leading anchor is written {@code BOFoffset} and a trailing one
     * {@code EOFoffset}; anything else, including nothing at all, is a
     * search. The container definitions rely on this: the rules for OOXML
     * presentations state a distance of zero and no anchor, and the string
     * they look for sits a few hundred bytes into the member. Reading the
     * absent attribute as "starts at the beginning" makes those rules match
     * nothing at all.
     */
    private static Anchor anchorOf(String reference) {
        if (reference == null || reference.isBlank() || UNANCHORED.equals(reference)) {
            return Anchor.ANYWHERE;
        }
        if (TRAILING_ANCHOR.equals(reference)) {
            return Anchor.TRAILING;
        }
        return Anchor.LEADING;
    }

    private static Subsequence subsequence(Element part) throws SignatureSyntaxException {
        int min = number(part, "SubSeqMinOffset", 0);
        int max = Math.max(min, number(part, "SubSeqMaxOffset", 0));
        Element sequence = firstChildNamed(part, "Sequence");
        if (sequence == null) {
            throw new SignatureSyntaxException("", 0, "the subsequence has no pattern");
        }
        return new Subsequence(min, max,
                SignatureExpression.compile(textOf(sequence)),
                fragments(part, "LeftFragment"),
                fragments(part, "RightFragment"));
    }

    private static List<Fragment> fragments(Element part, String name)
            throws SignatureSyntaxException {
        List<Fragment> fragments = new ArrayList<>();
        for (Element element : childrenNamed(part, name)) {
            int min = number(element, "MinOffset", 0);
            int max = Math.max(min, number(element, "MaxOffset", 0));
            fragments.add(new Fragment(number(element, "Position", 1), min, max,
                    SignatureExpression.compile(textOf(element))));
        }
        return fragments;
    }

    private static int number(Element element, String attribute, int fallback) {
        String value = element.getAttribute(attribute);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException notANumber) {
            return fallback;
        }
    }

    private static String textOf(Element element) {
        String text = element.getTextContent();
        return text == null ? "" : text.trim();
    }

    private static List<Element> childrenNamed(Element parent, String name) {
        List<Element> found = new ArrayList<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE && name.equals(child.getNodeName())) {
                found.add((Element) child);
            }
        }
        return found;
    }

    private static Element firstChildNamed(Element parent, String name) {
        List<Element> found = childrenNamed(parent, name);
        return found.isEmpty() ? null : found.getFirst();
    }
}
