package ai.protomolt.proto.parse.document;

import ai.protomolt.proto.parse.document.v1.BaseTextItem;
import ai.protomolt.proto.parse.document.v1.TextItemBase;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Projections between the parser-fleet document model
 * ({@link ai.protomolt.proto.parse.document.v1.Document}, docling-core v2 parity) and
 * the repo document contract.
 *
 * <p>Forward ({@link #toParserDocument}): the docling document rides
 * losslessly as the {@code ParserDocument.shape} {@link Any}, and the
 * doc-level facts a search index wants (title, body text, page count, source
 * identity) are extracted into {@code extracted_fields} — the claims a
 * parsing coordinator arbitrates into {@link SearchMetadata}. Every extracted
 * field records its docling source path in the returned provenance map, so a
 * consumer can always answer "where did this value come from".
 *
 * <p>Reverse ({@link #fromParserDocument}): unpacking the shape returns the
 * exact docling document that was projected — the forward direction loses
 * nothing, which the round-trip tests pin byte-for-byte.
 */
public final class DoclingProjection {

    /** The extracted-fields key carrying the document title. */
    public static final String FIELD_TITLE = "title";

    /** The extracted-fields key carrying the concatenated body text. */
    public static final String FIELD_BODY = "body";

    /** The extracted-fields key carrying the page count. */
    public static final String FIELD_PAGE_COUNT = "page_count";

    /** The extracted-fields key carrying the source MIME type. */
    public static final String FIELD_MIME_TYPE = "source_mime_type";

    /** The extracted-fields key carrying the source filename. */
    public static final String FIELD_FILENAME = "filename";

    /** The extracted-fields key carrying the source URI. */
    public static final String FIELD_SOURCE_URI = "source_uri";

    private DoclingProjection() {
    }

    /**
     * One projection outcome: what was produced plus, per produced field,
     * the docling path it came from.
     *
     * @param document the parser document (lossless shape + extracted fields)
     * @param provenance extracted-field key → docling source path
     */
    public record Projected(ParserDocument document, Map<String, String> provenance) {}

    /**
     * Projects a docling document into the repo parser-document shape.
     *
     * @param docling the parsed document in the fleet model
     * @return the projection with per-field provenance
     */
    public static Projected toParserDocument(ai.protomolt.proto.parse.document.v1.Document docling) {
        if (docling == null) {
            throw new IllegalArgumentException("docling document must not be null");
        }
        Map<String, String> provenance = new LinkedHashMap<>();
        Struct.Builder fields = Struct.newBuilder();

        String title = titleOf(docling);
        if (!title.isBlank()) {
            fields.putFields(FIELD_TITLE, Value.newBuilder().setStringValue(title).build());
            provenance.put(FIELD_TITLE, titleProvenance(docling));
        }
        String body = bodyTextOf(docling);
        if (!body.isBlank()) {
            fields.putFields(FIELD_BODY, Value.newBuilder().setStringValue(body).build());
            provenance.put(FIELD_BODY, "texts[*].text");
        }
        if (docling.getPagesCount() > 0) {
            fields.putFields(
                    FIELD_PAGE_COUNT,
                    Value.newBuilder().setNumberValue(docling.getPagesCount()).build());
            provenance.put(FIELD_PAGE_COUNT, "pages");
        }
        if (docling.hasOrigin()) {
            if (!docling.getOrigin().getMimetype().isBlank()) {
                fields.putFields(
                        FIELD_MIME_TYPE,
                        Value.newBuilder().setStringValue(docling.getOrigin().getMimetype()).build());
                provenance.put(FIELD_MIME_TYPE, "origin.mimetype");
            }
            if (!docling.getOrigin().getFilename().isBlank()) {
                fields.putFields(
                        FIELD_FILENAME,
                        Value.newBuilder().setStringValue(docling.getOrigin().getFilename()).build());
                provenance.put(FIELD_FILENAME, "origin.filename");
            }
            if (docling.getOrigin().hasUri() && !docling.getOrigin().getUri().isBlank()) {
                fields.putFields(
                        FIELD_SOURCE_URI,
                        Value.newBuilder().setStringValue(docling.getOrigin().getUri()).build());
                provenance.put(FIELD_SOURCE_URI, "origin.uri");
            }
        }

        ParserDocument document =
                ParserDocument.newBuilder()
                        .setShape(Any.pack(docling))
                        .setExtractedFields(fields)
                        .build();
        return new Projected(document, Map.copyOf(provenance));
    }

    /**
     * Unpacks the docling document a {@link #toParserDocument} projection
     * carried. Empty when the shape holds something other than the fleet
     * document model — the caller decides whether that is an error.
     *
     * @param document the stored parser document
     * @return the docling document, when the shape carries one
     */
    public static Optional<ai.protomolt.proto.parse.document.v1.Document> fromParserDocument(
            ParserDocument document) {
        if (document == null || !document.hasShape()) {
            return Optional.empty();
        }
        Any shape = document.getShape();
        if (!shape.is(ai.protomolt.proto.parse.document.v1.Document.class)) {
            return Optional.empty();
        }
        try {
            return Optional.of(shape.unpack(ai.protomolt.proto.parse.document.v1.Document.class));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "shape claims " + shape.getTypeUrl() + " but does not parse as it", e);
        }
    }

    /**
     * Applies a projection's extracted fields onto repo search metadata.
     * Only fills fields the projection produced; existing values on the
     * builder are overwritten deliberately — arbitration between parsers is
     * the coordinator's job, this is the single-parser application.
     *
     * @param projected a {@link #toParserDocument} outcome
     * @param metadata the search-metadata builder to fill
     */
    public static void applyTo(Projected projected, SearchMetadata.Builder metadata) {
        Map<String, Value> fields = projected.document().getExtractedFields().getFieldsMap();
        Value title = fields.get(FIELD_TITLE);
        if (title != null) {
            metadata.setTitle(title.getStringValue());
        }
        Value body = fields.get(FIELD_BODY);
        if (body != null) {
            metadata.setBody(body.getStringValue());
        }
        Value mime = fields.get(FIELD_MIME_TYPE);
        if (mime != null) {
            metadata.setSourceMimeType(mime.getStringValue());
        }
        Value uri = fields.get(FIELD_SOURCE_URI);
        if (uri != null) {
            metadata.setSourceUri(uri.getStringValue());
        }
        Value pages = fields.get(FIELD_PAGE_COUNT);
        if (pages != null) {
            metadata.putMetadata(FIELD_PAGE_COUNT, String.valueOf((long) pages.getNumberValue()));
        }
    }

    // ------------------------------------------------------------------

    private static String titleOf(ai.protomolt.proto.parse.document.v1.Document docling) {
        for (BaseTextItem item : docling.getTextsList()) {
            if (item.getItemCase() == BaseTextItem.ItemCase.TITLE) {
                String text = item.getTitle().getBase().getText();
                if (!text.isBlank()) {
                    return text;
                }
            }
        }
        return docling.getName();
    }

    private static String titleProvenance(ai.protomolt.proto.parse.document.v1.Document docling) {
        for (BaseTextItem item : docling.getTextsList()) {
            if (item.getItemCase() == BaseTextItem.ItemCase.TITLE
                    && !item.getTitle().getBase().getText().isBlank()) {
                return "texts[" + docling.getTextsList().indexOf(item) + "].title.text";
            }
        }
        return "name";
    }

    /** Concatenates every text item's text in list order, newline-separated. */
    private static String bodyTextOf(ai.protomolt.proto.parse.document.v1.Document docling) {
        StringBuilder body = new StringBuilder();
        for (BaseTextItem item : docling.getTextsList()) {
            String text = textOf(item);
            if (!text.isBlank()) {
                if (!body.isEmpty()) {
                    body.append('\n');
                }
                body.append(text);
            }
        }
        return body.toString();
    }

    private static String textOf(BaseTextItem item) {
        // CodeItem inlines its fields instead of embedding TextItemBase.
        if (item.getItemCase() == BaseTextItem.ItemCase.CODE) {
            return item.getCode().getText();
        }
        TextItemBase base =
                switch (item.getItemCase()) {
                    case TITLE -> item.getTitle().getBase();
                    case SECTION_HEADER -> item.getSectionHeader().getBase();
                    case LIST_ITEM -> item.getListItem().getBase();
                    case FORMULA -> item.getFormula().getBase();
                    case TEXT -> item.getText().getBase();
                    case FIELD_HEADING -> item.getFieldHeading().getBase();
                    case FIELD_VALUE -> item.getFieldValue().getBase();
                    case CODE, ITEM_NOT_SET -> null;
                };
        return base == null ? "" : base.getText();
    }
}
