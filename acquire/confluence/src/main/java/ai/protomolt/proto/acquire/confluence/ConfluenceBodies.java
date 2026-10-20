package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.Body;
import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.BodyType;
import ai.protomolt.proto.acquire.confluence.v1.SpaceDescription;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;

import static ai.protomolt.proto.acquire.confluence.ConfluenceJsonFields.text;

/**
 * Storage-format body translation for the Confluence REST JSON: the body
 * slots ({@code storage}, {@code atlas_doc_format}, {@code view},
 * {@code raw}, {@code plain}) become {@link Body}, {@link BodyType} and
 * {@link SpaceDescription}, and the API's representation strings become
 * {@link BodyFormat}.
 *
 * <p>The markup itself is carried through verbatim as the {@code value}
 * string; nothing here parses or rewrites XHTML or ADF, so malformed markup
 * reaches the proto unchanged. A slot that is not a JSON object is skipped,
 * a missing representation falls back to the slot's format, and an unknown
 * representation maps to {@link BodyFormat#BODY_FORMAT_UNSPECIFIED} while
 * keeping its value.</p>
 */
final class ConfluenceBodies {

    private ConfluenceBodies() {
    }

    /**
     * The content body of a page, blog post, comment or custom content, read
     * from the {@code storage}, {@code atlas_doc_format}, {@code view} and
     * {@code raw} slots of {@code body}.
     *
     * @param body the entity's {@code body} node, an object
     * @return the populated body; slots absent from the JSON stay unset
     */
    static Body contentBody(JsonNode body) {
        Body.Builder b = Body.newBuilder();
        JsonNode storage = body.path("storage");
        if (storage.isObject()) {
            b.setStorage(bodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
        }
        JsonNode adf = body.path("atlas_doc_format");
        if (adf.isObject()) {
            b.setAtlasDocFormat(bodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
        }
        JsonNode view = body.path("view");
        if (view.isObject()) {
            b.setView(bodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        JsonNode raw = body.path("raw");
        if (raw.isObject()) {
            b.setRaw(bodyType(raw, BodyFormat.BODY_FORMAT_RAW));
        }
        return b.build();
    }

    /**
     * The body of a task, read from the {@code view}, {@code storage} and
     * {@code atlas_doc_format} slots of {@code body}; tasks have no
     * {@code raw} slot.
     *
     * @param body the task's {@code body} node, an object
     * @return the populated body; slots absent from the JSON stay unset
     */
    static Body taskBody(JsonNode body) {
        Body.Builder b = Body.newBuilder();
        JsonNode view = body.path("view");
        if (view.isObject()) {
            b.setView(bodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        JsonNode storage = body.path("storage");
        if (storage.isObject()) {
            b.setStorage(bodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
        }
        JsonNode adf = body.path("atlas_doc_format");
        if (adf.isObject()) {
            b.setAtlasDocFormat(bodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
        }
        return b.build();
    }

    /**
     * A space description, read from its {@code plain} and {@code view} slots.
     *
     * @param description the space's {@code description} node, an object
     * @return the populated description; slots absent from the JSON stay unset
     */
    static SpaceDescription spaceDescription(JsonNode description) {
        SpaceDescription.Builder b = SpaceDescription.newBuilder();
        JsonNode plain = description.path("plain");
        if (plain.isObject()) {
            b.setPlain(bodyType(plain, BodyFormat.BODY_FORMAT_PLAIN_TEXT));
        }
        JsonNode view = description.path("view");
        if (view.isObject()) {
            b.setView(bodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        return b.build();
    }

    /**
     * One body slot. The slot's own {@code representation} string wins over
     * {@code slotDefault}; the {@code value} is copied verbatim.
     */
    static BodyType bodyType(JsonNode slot, BodyFormat slotDefault) {
        String representation = text(slot, "representation");
        BodyFormat format = representation.isEmpty() ? slotDefault : format(representation);
        return BodyType.newBuilder()
                .setFormat(format)
                .setValue(text(slot, "value"))
                .build();
    }

    /** The API's representation strings to the BodyFormat enum; unknown maps to UNSPECIFIED. */
    static BodyFormat format(String representation) {
        return switch (representation == null ? ""
                : representation.trim().toLowerCase(Locale.ROOT)) {
            case "storage" -> BodyFormat.BODY_FORMAT_STORAGE_XHTML;
            case "atlas_doc_format" -> BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT;
            case "view" -> BodyFormat.BODY_FORMAT_RENDERED_XHTML;
            case "export_view" -> BodyFormat.BODY_FORMAT_EXPORT_XHTML;
            case "anonymous_export_view" -> BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML;
            case "styled_view" -> BodyFormat.BODY_FORMAT_STYLED_XHTML;
            case "editor" -> BodyFormat.BODY_FORMAT_EDITOR;
            case "wiki" -> BodyFormat.BODY_FORMAT_WIKI;
            case "raw" -> BodyFormat.BODY_FORMAT_RAW;
            case "plain" -> BodyFormat.BODY_FORMAT_PLAIN_TEXT;
            default -> BodyFormat.BODY_FORMAT_UNSPECIFIED;
        };
    }
}
