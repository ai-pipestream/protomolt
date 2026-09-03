package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.AccountStatus;
import ai.protomolt.proto.acquire.confluence.v1.AccountType;
import ai.protomolt.proto.acquire.confluence.v1.Attachment;
import ai.protomolt.proto.acquire.confluence.v1.BlogPost;
import ai.protomolt.proto.acquire.confluence.v1.BlogPostContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.Body;
import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.BodyType;
import ai.protomolt.proto.acquire.confluence.v1.ClassificationLevel;
import ai.protomolt.proto.acquire.confluence.v1.ClassificationLevelColor;
import ai.protomolt.proto.acquire.confluence.v1.ClassificationLevelStatus;
import ai.protomolt.proto.acquire.confluence.v1.Comment;
import ai.protomolt.proto.acquire.confluence.v1.ContentProperty;
import ai.protomolt.proto.acquire.confluence.v1.ContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.CustomContent;
import ai.protomolt.proto.acquire.confluence.v1.Database;
import ai.protomolt.proto.acquire.confluence.v1.Folder;
import ai.protomolt.proto.acquire.confluence.v1.Icon;
import ai.protomolt.proto.acquire.confluence.v1.InlineCommentProperties;
import ai.protomolt.proto.acquire.confluence.v1.InlineCommentResolutionStatus;
import ai.protomolt.proto.acquire.confluence.v1.Label;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import ai.protomolt.proto.acquire.confluence.v1.ParentContentType;
import ai.protomolt.proto.acquire.confluence.v1.PropertyKey;
import ai.protomolt.proto.acquire.confluence.v1.PropertyValue;
import ai.protomolt.proto.acquire.confluence.v1.Space;
import ai.protomolt.proto.acquire.confluence.v1.SpaceDescription;
import ai.protomolt.proto.acquire.confluence.v1.SpaceIcon;
import ai.protomolt.proto.acquire.confluence.v1.SpaceProperty;
import ai.protomolt.proto.acquire.confluence.v1.SpacePropertyVersion;
import ai.protomolt.proto.acquire.confluence.v1.SpaceStatus;
import ai.protomolt.proto.acquire.confluence.v1.SpaceType;
import ai.protomolt.proto.acquire.confluence.v1.Task;
import ai.protomolt.proto.acquire.confluence.v1.TaskStatus;
import ai.protomolt.proto.acquire.confluence.v1.User;
import ai.protomolt.proto.acquire.confluence.v1.Version;
import ai.protomolt.proto.acquire.confluence.v1.Whiteboard;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Jackson {@link JsonNode} to domain proto translation for the Confluence
 * Cloud REST API v2 shapes. One method per crawled entity kind; every method
 * is null-tolerant (missing fields stay unset) and never throws on unknown
 * enum wire values - those map to the {@code UNSPECIFIED} member, per the
 * domain model's field-level fidelity rules.
 *
 * <p>REST response decoration is dropped here: {@code _links} blocks collapse
 * to the entity's {@code web_url} (composed absolute from the tenant base URL
 * when the API returns a server-relative path), and pagination transport
 * never reaches the protos. Properties translate the API's string keys into
 * {@link PropertyKey} ({@code CUSTOM} plus {@code custom_key} as the escape
 * hatch) and the JSON value into the matching {@link PropertyValue} arm.</p>
 */
public final class ConfluenceMapper {

    private final String baseUrl;
    private final String basePath;
    private final String origin;

    /**
     * @param baseUrl the tenant base URL including {@code /wiki} (e.g.
     *        {@code https://pipestreamai.atlassian.net/wiki}); used to compose
     *        absolute {@code web_url} / {@code download_url} values
     */
    public ConfluenceMapper(String baseUrl) {
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
        URI uri = URI.create(this.baseUrl);
        this.basePath = uri.getPath() == null ? "" : uri.getPath();
        this.origin = uri.getScheme() + "://" + uri.getAuthority();
    }

    // ======================================================================
    // SPACES
    // ======================================================================

    /** Maps a Space (Bulk or Single read shape; the Single-only fields stay unset on Bulk). */
    public Space toSpace(JsonNode node) {
        Space.Builder b = Space.newBuilder()
                .setId(text(node, "id"))
                .setKey(text(node, "key"))
                .setName(text(node, "name"))
                .setType(wireEnum(SpaceType.class, "SPACE_TYPE_", text(node, "type"),
                        SpaceType.SPACE_TYPE_UNSPECIFIED))
                .setStatus(wireEnum(SpaceStatus.class, "SPACE_STATUS_", text(node, "status"),
                        SpaceStatus.SPACE_STATUS_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setSpaceOwnerId(text(node, "spaceOwnerId"))
                .setCurrentActiveAlias(text(node, "currentActiveAlias"))
                .setHomepageId(text(node, "homepageId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        JsonNode description = node.path("description");
        if (description.isObject()) {
            b.setDescription(toSpaceDescription(description));
        }
        JsonNode icon = node.path("icon");
        if (icon.isObject()) {
            b.setIcon(SpaceIcon.newBuilder()
                    .setPath(text(icon, "path"))
                    .setApiDownloadUrl(absolute(text(icon, "apiDownloadLink")))
                    .build());
        }
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    private SpaceDescription toSpaceDescription(JsonNode node) {
        SpaceDescription.Builder b = SpaceDescription.newBuilder();
        JsonNode plain = node.path("plain");
        if (plain.isObject()) {
            b.setPlain(toBodyType(plain, BodyFormat.BODY_FORMAT_PLAIN_TEXT));
        }
        JsonNode view = node.path("view");
        if (view.isObject()) {
            b.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        return b.build();
    }

    // ======================================================================
    // PAGES AND BLOG POSTS
    // ======================================================================

    /** Maps a Page (Bulk or Single read shape). */
    public Page toPage(JsonNode node) {
        Page.Builder b = Page.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"));
        if (hasText(node, "subtype")) {
            b.setSubtype(text(node, "subtype"));
        }
        if (hasText(node, "ownerId")) {
            b.setOwnerId(text(node, "ownerId"));
        }
        if (hasText(node, "lastOwnerId")) {
            b.setLastOwnerId(text(node, "lastOwnerId"));
        }
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    /** Maps a BlogPost (Bulk or Single read shape). */
    public BlogPost toBlogPost(JsonNode node) {
        BlogPost.Builder b = BlogPost.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(BlogPostContentStatus.class, "BLOG_POST_CONTENT_STATUS_",
                        text(node, "status"), BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    // ======================================================================
    // COMMENTS
    // ======================================================================

    /**
     * Maps a footer or inline comment; one proto covers both, the inline-only
     * fields stay unset on footer comments.
     */
    public Comment toComment(JsonNode node) {
        Comment.Builder b = Comment.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setBlogPostId(text(node, "blogPostId"))
                .setPageId(text(node, "pageId"))
                .setAttachmentId(text(node, "attachmentId"))
                .setCustomContentId(text(node, "customContentId"))
                .setParentCommentId(text(node, "parentCommentId"))
                .setResolutionStatus(wireEnum(InlineCommentResolutionStatus.class,
                        "INLINE_COMMENT_RESOLUTION_STATUS_", text(node, "resolutionStatus"),
                        InlineCommentResolutionStatus.INLINE_COMMENT_RESOLUTION_STATUS_UNSPECIFIED));
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        if (hasText(node, "resolutionLastModifierId")) {
            b.setResolutionLastModifierId(text(node, "resolutionLastModifierId"));
        }
        if (hasText(node, "resolutionLastModifiedAt")) {
            Timestamp at = timestamp(text(node, "resolutionLastModifiedAt"));
            if (at != null) {
                b.setResolutionLastModifiedAt(at);
            }
        }
        // Inline list responses carry the highlighted-text properties under
        // "properties" (single reads reuse that name for content properties).
        JsonNode inline = node.path("properties");
        if (!inline.has("inlineMarkerRef") && !inline.has("inlineOriginalSelection")) {
            inline = node.path("inlineProperties");
        }
        if (inline.isObject() && (inline.has("inlineMarkerRef")
                || inline.has("inlineOriginalSelection"))) {
            b.setInlineProperties(InlineCommentProperties.newBuilder()
                    .setInlineMarkerRef(text(inline, "inlineMarkerRef"))
                    .setInlineOriginalSelection(text(inline, "inlineOriginalSelection"))
                    .build());
        }
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    // ======================================================================
    // ATTACHMENTS
    // ======================================================================

    /**
     * Maps an attachment metadata record (Bulk or Single). Bytes are not part
     * of list responses; the crawler fills {@code content} from a separate
     * {@link ConfluenceClient#downloadAttachmentBytes} call when it wants the
     * binary.
     */
    public Attachment toAttachment(JsonNode node) {
        Attachment.Builder b = Attachment.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setCustomContentId(text(node, "customContentId"))
                .setMediaType(text(node, "mediaType"))
                .setMediaTypeDescription(text(node, "mediaTypeDescription"))
                .setComment(text(node, "comment"))
                .setFileId(text(node, "fileId"))
                .setFileSize(node.path("fileSize").isIntegralNumber()
                        ? node.path("fileSize").longValue() : 0L);
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        // Attachments carry their links as direct fields as well as _links;
        // the direct fields win.
        String webui = hasText(node, "webuiLink") ? text(node, "webuiLink") : webui(node);
        b.setWebUrl(absolute(webui));
        String download = hasText(node, "downloadLink") ? text(node, "downloadLink")
                : text(node.path("_links"), "download");
        b.setDownloadUrl(absolute(download));
        return b.build();
    }

    // ======================================================================
    // LABELS, TASKS, USERS
    // ======================================================================

    public Label toLabel(JsonNode node) {
        return Label.newBuilder()
                .setId(text(node, "id"))
                .setName(text(node, "name"))
                .setPrefix(text(node, "prefix"))
                .build();
    }

    public Task toTask(JsonNode node) {
        Task.Builder b = Task.newBuilder()
                .setId(text(node, "id"))
                .setLocalId(text(node, "localId"))
                .setSpaceId(text(node, "spaceId"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setStatus(wireEnum(TaskStatus.class, "TASK_STATUS_", text(node, "status"),
                        TaskStatus.TASK_STATUS_UNSPECIFIED))
                .setCreatedBy(text(node, "createdBy"))
                .setAssignedTo(text(node, "assignedTo"))
                .setCompletedBy(text(node, "completedBy"));
        JsonNode body = node.path("body");
        if (body.isObject()) {
            Body.Builder bb = Body.newBuilder();
            JsonNode view = body.path("view");
            if (view.isObject()) {
                bb.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
            }
            JsonNode storage = body.path("storage");
            if (storage.isObject()) {
                bb.setStorage(toBodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
            }
            JsonNode adf = body.path("atlas_doc_format");
            if (adf.isObject()) {
                bb.setAtlasDocFormat(toBodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
            }
            b.setBody(bb.build());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setTimestamp(node, "updatedAt", b::setUpdatedAt);
        setTimestamp(node, "dueAt", b::setDueAt);
        setTimestamp(node, "completedAt", b::setCompletedAt);
        return b.build();
    }

    public User toUser(JsonNode node) {
        User.Builder b = User.newBuilder()
                .setAccountId(text(node, "accountId"))
                .setAccountType(wireEnum(AccountType.class, "ACCOUNT_TYPE_",
                        text(node, "accountType"), AccountType.ACCOUNT_TYPE_UNSPECIFIED))
                .setAccountStatus(wireEnum(AccountStatus.class, "ACCOUNT_STATUS_",
                        text(node, "accountStatus"), AccountStatus.ACCOUNT_STATUS_UNSPECIFIED))
                .setDisplayName(text(node, "displayName"))
                .setPublicName(text(node, "publicName"))
                .setEmail(text(node, "email"))
                .setTimeZone(text(node, "timeZone"))
                .setIsExternalCollaborator(node.path("isExternalCollaborator").asBoolean(false));
        if (hasText(node, "personalSpaceId")) {
            b.setPersonalSpaceId(text(node, "personalSpaceId"));
        }
        JsonNode picture = node.path("profilePicture");
        if (picture.isObject()) {
            b.setProfilePicture(Icon.newBuilder()
                    .setPath(text(picture, "path"))
                    .setIsDefault(picture.path("isDefault").asBoolean(false))
                    .build());
        }
        return b.build();
    }

    // ======================================================================
    // HIERARCHICAL CONTENT
    // ======================================================================

    public Whiteboard toWhiteboard(JsonNode node) {
        Whiteboard.Builder b = Whiteboard.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    public Database toDatabase(JsonNode node) {
        Database.Builder b = Database.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    public Folder toFolder(JsonNode node) {
        Folder.Builder b = Folder.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    public CustomContent toCustomContent(JsonNode node) {
        CustomContent.Builder b = CustomContent.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setCustomContentId(text(node, "customContentId"))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    // ======================================================================
    // PROPERTIES
    // ======================================================================

    public ContentProperty toContentProperty(JsonNode node) {
        ContentProperty.Builder b = ContentProperty.newBuilder()
                .setId(text(node, "id"));
        applyPropertyKey(node, b::setKey, b::setCustomKey);
        setPropertyValue(node, b::setValue);
        setVersion(node, b::setVersion);
        return b.build();
    }

    public SpaceProperty toSpaceProperty(JsonNode node) {
        SpaceProperty.Builder b = SpaceProperty.newBuilder()
                .setId(text(node, "id"))
                .setCreatedBy(text(node, "createdBy"));
        applyPropertyKey(node, b::setKey, b::setCustomKey);
        setPropertyValue(node, b::setValue);
        setTimestamp(node, "createdAt", b::setCreatedAt);
        JsonNode version = node.path("version");
        if (version.isObject()) {
            SpacePropertyVersion.Builder vb = SpacePropertyVersion.newBuilder()
                    .setCreatedBy(text(version, "createdBy"))
                    .setMessage(text(version, "message"))
                    .setNumber(version.path("number").isIntegralNumber()
                            ? version.path("number").intValue() : 0);
            Timestamp at = timestamp(text(version, "createdAt"));
            if (at != null) {
                vb.setCreatedAt(at);
            }
            b.setVersion(vb.build());
        }
        return b.build();
    }

    /** The well-known keys map by name; anything else is CUSTOM plus the raw key. */
    static void applyPropertyKey(JsonNode node,
            java.util.function.Consumer<PropertyKey> keySetter,
            java.util.function.Consumer<String> customKeySetter) {
        String key = text(node, "key");
        switch (key) {
            case "editor" -> keySetter.accept(PropertyKey.PROPERTY_KEY_EDITOR);
            case "content-appearance-published" ->
                    keySetter.accept(PropertyKey.PROPERTY_KEY_CONTENT_APPEARANCE_PUBLISHED);
            case "content-appearance-draft" ->
                    keySetter.accept(PropertyKey.PROPERTY_KEY_CONTENT_APPEARANCE_DRAFT);
            case "" -> keySetter.accept(PropertyKey.PROPERTY_KEY_UNSPECIFIED);
            default -> {
                keySetter.accept(PropertyKey.PROPERTY_KEY_CUSTOM);
                customKeySetter.accept(key);
            }
        }
    }

    /** The oneof arm matches the JSON value's actual type; objects/arrays go to json_value. */
    static void setPropertyValue(JsonNode node,
            java.util.function.Consumer<PropertyValue> valueSetter) {
        JsonNode value = node.path("value");
        if (value.isMissingNode() || value.isNull()) {
            return;
        }
        PropertyValue.Builder b = PropertyValue.newBuilder();
        if (value.isTextual()) {
            b.setStringValue(value.asText());
        } else if (value.isBoolean()) {
            b.setBoolValue(value.asBoolean());
        } else if (value.isIntegralNumber()) {
            b.setIntegerValue(value.longValue());
        } else if (value.isNumber()) {
            b.setDoubleValue(value.doubleValue());
        } else if (value.isObject() || value.isArray()) {
            b.setJsonValue(toStruct(value));
        } else {
            return;
        }
        valueSetter.accept(b.build());
    }

    // ======================================================================
    // VERSIONS AND CLASSIFICATION LEVELS
    // ======================================================================

    public Version toVersion(JsonNode node) {
        Version.Builder b = Version.newBuilder()
                .setMessage(text(node, "message"))
                .setNumber(node.path("number").isIntegralNumber()
                        ? node.path("number").intValue() : 0)
                .setMinorEdit(node.path("minorEdit").asBoolean(false))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        return b.build();
    }

    public ClassificationLevel toClassificationLevel(JsonNode node) {
        ClassificationLevel.Builder b = ClassificationLevel.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ClassificationLevelStatus.class, "CLASSIFICATION_LEVEL_STATUS_",
                        text(node, "status"),
                        ClassificationLevelStatus.CLASSIFICATION_LEVEL_STATUS_UNSPECIFIED))
                .setOrder(node.path("order").isNumber() ? node.path("order").doubleValue() : 0d)
                .setName(text(node, "name"))
                .setDescription(text(node, "description"))
                .setGuideline(text(node, "guideline"))
                .setColor(wireEnum(ClassificationLevelColor.class, "CLASSIFICATION_LEVEL_COLOR_",
                        text(node, "color"),
                        ClassificationLevelColor.CLASSIFICATION_LEVEL_COLOR_UNSPECIFIED));
        return b.build();
    }

    // ======================================================================
    // SHARED PIECES
    // ======================================================================

    /** Body slots: storage, atlas_doc_format, view, raw. Representation string wins over the slot default. */
    private void setBody(JsonNode node, java.util.function.Consumer<Body> bodySetter) {
        JsonNode body = node.path("body");
        if (!body.isObject()) {
            return;
        }
        Body.Builder b = Body.newBuilder();
        JsonNode storage = body.path("storage");
        if (storage.isObject()) {
            b.setStorage(toBodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
        }
        JsonNode adf = body.path("atlas_doc_format");
        if (adf.isObject()) {
            b.setAtlasDocFormat(toBodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
        }
        JsonNode view = body.path("view");
        if (view.isObject()) {
            b.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        JsonNode raw = body.path("raw");
        if (raw.isObject()) {
            b.setRaw(toBodyType(raw, BodyFormat.BODY_FORMAT_RAW));
        }
        bodySetter.accept(b.build());
    }

    private BodyType toBodyType(JsonNode node, BodyFormat slotDefault) {
        String representation = text(node, "representation");
        BodyFormat format = representation.isEmpty() ? slotDefault : bodyFormat(representation);
        return BodyType.newBuilder()
                .setFormat(format)
                .setValue(text(node, "value"))
                .build();
    }

    /** The API's representation strings to the BodyFormat enum; unknown maps to UNSPECIFIED. */
    public static BodyFormat bodyFormat(String representation) {
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

    private void setVersion(JsonNode node, java.util.function.Consumer<Version> versionSetter) {
        JsonNode version = node.path("version");
        if (version.isObject()) {
            versionSetter.accept(toVersion(version));
        }
    }

    private void setTimestamp(JsonNode node, String field,
            java.util.function.Consumer<Timestamp> setter) {
        Timestamp at = timestamp(text(node, field));
        if (at != null) {
            setter.accept(at);
        }
    }

    /**
     * RFC3339 / ISO-8601 to Timestamp. Tolerant of offset forms
     * ({@code +00:00} as well as {@code Z}); blank or unparseable input
     * yields null so the field stays unset.
     */
    public static Timestamp timestamp(String rfc3339) {
        if (rfc3339 == null || rfc3339.isBlank()) {
            return null;
        }
        String trimmed = rfc3339.trim();
        Instant instant;
        try {
            instant = Instant.parse(trimmed);
        } catch (DateTimeParseException e) {
            try {
                instant = OffsetDateTime.parse(trimmed).toInstant();
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /**
     * Composes an absolute URL from a server-relative path. Confluence Cloud
     * returns {@code webui} links without the {@code /wiki} prefix and
     * {@code _links.next} with it, so both forms are handled: paths already
     * carrying the base path resolve against the origin, bare paths against
     * the full base URL.
     */
    public String absolute(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        if (url.startsWith("http")) {
            return url;
        }
        String path = url.startsWith("/") ? url : "/" + url;
        if (!basePath.isEmpty() && path.startsWith(basePath + "/")) {
            return origin + path;
        }
        return baseUrl + path;
    }

    private static String webui(JsonNode node) {
        return text(node.path("_links"), "webui");
    }

    /** Generic enum mapping: wire value uppercased, '-' to '_', prefixed; unknown yields the fallback. */
    static <E extends Enum<E>> E wireEnum(Class<E> type, String prefix, String wire, E fallback) {
        if (wire == null || wire.isBlank()) {
            return fallback;
        }
        String name = prefix + wire.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }

    /** Jackson tree to protobuf Struct, for property values that genuinely are structured JSON. */
    static Struct toStruct(JsonNode node) {
        Struct.Builder b = Struct.newBuilder();
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            b.putFields(field.getKey(), toValue(field.getValue()));
        }
        return b.build();
    }

    private static Value toValue(JsonNode node) {
        Value.Builder b = Value.newBuilder();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return b.setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
        }
        if (node.isTextual()) {
            return b.setStringValue(node.asText()).build();
        }
        if (node.isBoolean()) {
            return b.setBoolValue(node.asBoolean()).build();
        }
        if (node.isNumber()) {
            return b.setNumberValue(node.doubleValue()).build();
        }
        if (node.isArray()) {
            com.google.protobuf.ListValue.Builder list = com.google.protobuf.ListValue.newBuilder();
            node.forEach(item -> list.addValues(toValue(item)));
            return b.setListValue(list.build()).build();
        }
        if (node.isObject()) {
            return b.setStructValue(toStruct(node)).build();
        }
        return b.setStringValue(node.asText()).build();
    }

    private static boolean hasText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() && !value.asText().isEmpty();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : "";
    }
}
