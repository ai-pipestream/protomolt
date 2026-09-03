package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.Attachment;
import ai.protomolt.proto.acquire.confluence.v1.BlogPost;
import ai.protomolt.proto.acquire.confluence.v1.BlogPostContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.ClassificationLevel;
import ai.protomolt.proto.acquire.confluence.v1.ClassificationLevelColor;
import ai.protomolt.proto.acquire.confluence.v1.Comment;
import ai.protomolt.proto.acquire.confluence.v1.ContentProperty;
import ai.protomolt.proto.acquire.confluence.v1.ContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.CustomContent;
import ai.protomolt.proto.acquire.confluence.v1.Database;
import ai.protomolt.proto.acquire.confluence.v1.Folder;
import ai.protomolt.proto.acquire.confluence.v1.InlineCommentResolutionStatus;
import ai.protomolt.proto.acquire.confluence.v1.Label;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import ai.protomolt.proto.acquire.confluence.v1.PropertyKey;
import ai.protomolt.proto.acquire.confluence.v1.PropertyValue;
import ai.protomolt.proto.acquire.confluence.v1.Space;
import ai.protomolt.proto.acquire.confluence.v1.SpaceProperty;
import ai.protomolt.proto.acquire.confluence.v1.SpaceType;
import ai.protomolt.proto.acquire.confluence.v1.Task;
import ai.protomolt.proto.acquire.confluence.v1.TaskStatus;
import ai.protomolt.proto.acquire.confluence.v1.User;
import ai.protomolt.proto.acquire.confluence.v1.AccountType;
import ai.protomolt.proto.acquire.confluence.v1.Version;
import ai.protomolt.proto.acquire.confluence.v1.Whiteboard;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapper round-trips: spec-shaped REST JSON becomes the domain protos, one
 * test per entity kind plus the shared translations (enums, timestamps,
 * property typing, web_url composition).
 */
class ConfluenceMapperTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BASE = "https://pipestreamai.atlassian.net/wiki";

    private final ConfluenceMapper mapper = new ConfluenceMapper(BASE);

    private static JsonNode tree(String json) throws Exception {
        return JSON.readTree(json);
    }

    @Test
    void mapsSpace() throws Exception {
        Space space = mapper.toSpace(tree(ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));

        assertThat(space.getId()).isEqualTo("100");
        assertThat(space.getKey()).isEqualTo("ENG");
        assertThat(space.getName()).isEqualTo("Engineering");
        assertThat(space.getType()).isEqualTo(SpaceType.SPACE_TYPE_GLOBAL);
        assertThat(space.getStatus().name()).isEqualTo("SPACE_STATUS_CURRENT");
        assertThat(space.getAuthorId()).isEqualTo("acc-1");
        assertThat(space.getHomepageId()).isEqualTo("900");
        assertThat(space.getCreatedAt().getSeconds()).isGreaterThan(0);
        assertThat(space.getDescription().getPlain().getFormat())
                .isEqualTo(BodyFormat.BODY_FORMAT_PLAIN_TEXT);
        assertThat(space.getDescription().getView().getValue()).isEqualTo("<p>Engineering space</p>");
        assertThat(space.getIcon().getApiDownloadUrl())
                .isEqualTo(BASE + "/download/avatars/ENG.png");
        // _links.webui is server-relative without /wiki: composed against the base URL.
        assertThat(space.getWebUrl()).isEqualTo(BASE + "/spaces/ENG");
    }

    @Test
    void mapsPageWithStorageBodyAndVersion() throws Exception {
        Page page = mapper.toPage(tree(
                ConfluenceFixtures.pageJson("200", "100", "Design Doc", "2024-03-01T12:00:00.000Z")));

        assertThat(page.getId()).isEqualTo("200");
        assertThat(page.getStatus()).isEqualTo(ContentStatus.CONTENT_STATUS_CURRENT);
        assertThat(page.getTitle()).isEqualTo("Design Doc");
        assertThat(page.getSpaceId()).isEqualTo("100");
        assertThat(page.getParentId()).isEmpty();
        assertThat(page.getVersion().getNumber()).isEqualTo(3);
        assertThat(page.getVersion().getAuthorId()).isEqualTo("acc-2");
        assertThat(page.getVersion().getCreatedAt().getSeconds()).isGreaterThan(0);
        assertThat(page.getBody().getStorage().getFormat())
                .isEqualTo(BodyFormat.BODY_FORMAT_STORAGE_XHTML);
        assertThat(page.getBody().getStorage().getValue()).isEqualTo("<p>Hello Design Doc</p>");
        assertThat(page.getWebUrl()).isEqualTo(BASE + "/spaces/ENG/pages/200/Design+Doc");
    }

    @Test
    void mapsPageWebuiThatAlreadyCarriesWikiPrefix() throws Exception {
        String json = """
                {"id": "1", "status": "current", "title": "t", "spaceId": "9",
                 "_links": {"webui": "/wiki/spaces/ENG/pages/1"}}
                """;
        Page page = mapper.toPage(tree(json));
        // _links.next-style paths carry /wiki already: resolve against the origin.
        assertThat(page.getWebUrl())
                .isEqualTo("https://pipestreamai.atlassian.net/wiki/spaces/ENG/pages/1");
    }

    @Test
    void mapsBlogPostWithItsOwnStatusEnum() throws Exception {
        BlogPost post = mapper.toBlogPost(tree(
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                        "2024-03-02T08:00:00.000Z")));

        assertThat(post.getId()).isEqualTo("300");
        assertThat(post.getStatus()).isEqualTo(BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_CURRENT);
        assertThat(post.getBody().getStorage().getValue()).isEqualTo("<p>Post Release notes</p>");
        assertThat(post.getVersion().getNumber()).isEqualTo(1);
    }

    @Test
    void mapsFooterAndInlineComments() throws Exception {
        Comment footer = mapper.toComment(tree(ConfluenceFixtures.footerCommentJson("c1", "200")));
        assertThat(footer.getId()).isEqualTo("c1");
        assertThat(footer.getPageId()).isEqualTo("200");
        assertThat(footer.getBody().getStorage().getValue()).isEqualTo("<p>Nice</p>");
        assertThat(footer.hasInlineProperties()).isFalse();

        Comment inline = mapper.toComment(tree(ConfluenceFixtures.inlineCommentJson("c2", "200")));
        assertThat(inline.getResolutionStatus())
                .isEqualTo(InlineCommentResolutionStatus.INLINE_COMMENT_RESOLUTION_STATUS_OPEN);
        assertThat(inline.getInlineProperties().getInlineMarkerRef()).isEqualTo("marker-1");
        assertThat(inline.getInlineProperties().getInlineOriginalSelection()).isEqualTo("teh");
    }

    @Test
    void mapsAttachmentMetadataWithLinks() throws Exception {
        Attachment attachment = mapper.toAttachment(tree(
                ConfluenceFixtures.attachmentJson("a1", "200")));

        assertThat(attachment.getId()).isEqualTo("a1");
        assertThat(attachment.getTitle()).isEqualTo("diagram.png");
        assertThat(attachment.getMediaType()).isEqualTo("image/png");
        assertThat(attachment.getFileSize()).isEqualTo(12345L);
        assertThat(attachment.getFileId()).isEqualTo("file-xyz");
        assertThat(attachment.getPageId()).isEqualTo("200");
        assertThat(attachment.getWebUrl())
                .isEqualTo(BASE + "/spaces/ENG/pages/200#attachment-a1");
        assertThat(attachment.getDownloadUrl())
                .isEqualTo(BASE + "/download/attachments/a1/diagram.png");
        // Metadata-only record: no bytes inline.
        assertThat(attachment.hasContent()).isFalse();
    }

    @Test
    void mapsLabel() throws Exception {
        Label label = mapper.toLabel(tree(ConfluenceFixtures.labelJson("l1", "design")));
        assertThat(label.getId()).isEqualTo("l1");
        assertThat(label.getName()).isEqualTo("design");
        assertThat(label.getPrefix()).isEqualTo("global");
    }

    @Test
    void mapsTask() throws Exception {
        Task task = mapper.toTask(tree("""
                {
                  "id": "t1",
                  "localId": "loc-1",
                  "spaceId": "100",
                  "pageId": "200",
                  "status": "incomplete",
                  "body": {"view": {"representation": "view", "value": "<p>do it</p>"}},
                  "createdBy": "acc-1",
                  "assignedTo": "acc-2",
                  "createdAt": "2024-02-10T10:00:00.000Z",
                  "updatedAt": "2024-02-10T11:00:00.000Z",
                  "dueAt": "2024-03-01T00:00:00.000Z"
                }
                """));
        assertThat(task.getStatus()).isEqualTo(TaskStatus.TASK_STATUS_INCOMPLETE);
        assertThat(task.getBody().getView().getFormat())
                .isEqualTo(BodyFormat.BODY_FORMAT_RENDERED_XHTML);
        assertThat(task.getDueAt().getSeconds()).isGreaterThan(0);
        assertThat(task.hasCompletedAt()).isFalse();
    }

    @Test
    void mapsUser() throws Exception {
        User user = mapper.toUser(tree("""
                {
                  "accountId": "acc-1",
                  "accountType": "atlassian",
                  "accountStatus": "active",
                  "displayName": "Ada Lovelace",
                  "publicName": "ada",
                  "email": "ada@example.com",
                  "timeZone": "UTC",
                  "personalSpaceId": "55",
                  "isExternalCollaborator": false,
                  "profilePicture": {"path": "/wiki/aa-avatar/acc-1", "isDefault": true}
                }
                """));
        assertThat(user.getAccountId()).isEqualTo("acc-1");
        assertThat(user.getAccountType()).isEqualTo(AccountType.ACCOUNT_TYPE_ATLASSIAN);
        assertThat(user.getDisplayName()).isEqualTo("Ada Lovelace");
        assertThat(user.getPersonalSpaceId()).isEqualTo("55");
        assertThat(user.getProfilePicture().getIsDefault()).isTrue();
    }

    @Test
    void mapsHierarchicalContent() throws Exception {
        String shape = """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "%s",
                  "parentId": "200",
                  "parentType": "page",
                  "position": 2,
                  "authorId": "acc-1",
                  "ownerId": "acc-1",
                  "spaceId": "100",
                  "createdAt": "2024-02-11T10:00:00.000Z",
                  "version": {"createdAt": "2024-02-11T10:00:00.000Z", "message": "",
                              "number": 1, "minorEdit": false, "authorId": "acc-1"},
                  "_links": {"webui": "/spaces/ENG/%s/%s"}
                }
                """;
        Whiteboard whiteboard = mapper.toWhiteboard(tree(shape.formatted("w1", "Board",
                "whiteboards", "w1")));
        assertThat(whiteboard.getParentType().name()).isEqualTo("PARENT_CONTENT_TYPE_PAGE");
        assertThat(whiteboard.getPosition()).isEqualTo(2);

        Database database = mapper.toDatabase(tree(shape.formatted("d1", "DB",
                "databases", "d1")));
        assertThat(database.getSpaceId()).isEqualTo("100");

        Folder folder = mapper.toFolder(tree(shape.formatted("f1", "Folder",
                "folders", "f1")));
        assertThat(folder.getWebUrl()).isEqualTo(BASE + "/spaces/ENG/folders/f1");

        CustomContent custom = mapper.toCustomContent(tree("""
                {
                  "id": "cc1",
                  "type": "com.example:poll",
                  "status": "current",
                  "title": "Poll",
                  "spaceId": "100",
                  "pageId": "200",
                  "authorId": "acc-1",
                  "createdAt": "2024-02-12T10:00:00.000Z",
                  "body": {"raw": {"representation": "raw", "value": "{\\"q\\":\\"x\\"}"}},
                  "_links": {"webui": "/spaces/ENG/custom/cc1"}
                }
                """));
        assertThat(custom.getType()).isEqualTo("com.example:poll");
        assertThat(custom.getBody().getRaw().getFormat()).isEqualTo(BodyFormat.BODY_FORMAT_RAW);
    }

    @Test
    void mapsPropertyKeysAndTypedValues() throws Exception {
        ContentProperty editor = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p1", "editor", "\"v2\"")));
        assertThat(editor.getKey()).isEqualTo(PropertyKey.PROPERTY_KEY_EDITOR);
        assertThat(editor.getValue().getStringValue()).isEqualTo("v2");

        ContentProperty appearance = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p2", "content-appearance-published", "\"dark\"")));
        assertThat(appearance.getKey())
                .isEqualTo(PropertyKey.PROPERTY_KEY_CONTENT_APPEARANCE_PUBLISHED);

        ContentProperty custom = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p3", "my-plugin-score", "42")));
        assertThat(custom.getKey()).isEqualTo(PropertyKey.PROPERTY_KEY_CUSTOM);
        assertThat(custom.getCustomKey()).isEqualTo("my-plugin-score");
        assertThat(custom.getValue().getValueCase())
                .isEqualTo(PropertyValue.ValueCase.INTEGER_VALUE);
        assertThat(custom.getValue().getIntegerValue()).isEqualTo(42L);

        ContentProperty flag = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p4", "my-plugin-flag", "true")));
        assertThat(flag.getValue().getBoolValue()).isTrue();

        ContentProperty ratio = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p5", "my-plugin-ratio", "1.5")));
        assertThat(ratio.getValue().getDoubleValue()).isEqualTo(1.5d);

        // Objects and arrays land in json_value, the schema's only Struct.
        ContentProperty structured = mapper.toContentProperty(tree(
                ConfluenceFixtures.propertyJson("p6", "my-plugin-config",
                        "{\"mode\": \"fast\", \"retries\": 3}")));
        assertThat(structured.getValue().getValueCase())
                .isEqualTo(PropertyValue.ValueCase.JSON_VALUE);
        assertThat(structured.getValue().getJsonValue().getFieldsOrThrow("mode").getStringValue())
                .isEqualTo("fast");
        assertThat(structured.getValue().getJsonValue().getFieldsOrThrow("retries")
                .getNumberValue()).isEqualTo(3d);
    }

    @Test
    void mapsSpaceProperty() throws Exception {
        SpaceProperty property = mapper.toSpaceProperty(tree(
                ConfluenceFixtures.spacePropertyJson("sp1", "custom-theme", "\"blue\"")));

        assertThat(property.getKey()).isEqualTo(PropertyKey.PROPERTY_KEY_CUSTOM);
        assertThat(property.getCustomKey()).isEqualTo("custom-theme");
        assertThat(property.getValue().getStringValue()).isEqualTo("blue");
        assertThat(property.getCreatedBy()).isEqualTo("acc-1");
        assertThat(property.getVersion().getNumber()).isEqualTo(1);
        assertThat(property.getVersion().getCreatedAt().getSeconds()).isGreaterThan(0);
    }

    @Test
    void mapsVersion() throws Exception {
        Version version = mapper.toVersion(tree("""
                {"createdAt": "2024-02-13T10:00:00.000Z", "message": "v2", "number": 2,
                 "minorEdit": true, "authorId": "acc-9"}
                """));
        assertThat(version.getNumber()).isEqualTo(2);
        assertThat(version.getMinorEdit()).isTrue();
        assertThat(version.getAuthorId()).isEqualTo("acc-9");
    }

    @Test
    void mapsClassificationLevel() throws Exception {
        ClassificationLevel level = mapper.toClassificationLevel(tree("""
                {"id": "cl1", "status": "PUBLISHED", "order": 2.5, "name": "Restricted",
                 "description": "restricted content", "guideline": "handle with care",
                 "color": "RED_BOLD"}
                """));
        assertThat(level.getStatus().name()).isEqualTo("CLASSIFICATION_LEVEL_STATUS_PUBLISHED");
        assertThat(level.getOrder()).isEqualTo(2.5d);
        assertThat(level.getColor()).isEqualTo(ClassificationLevelColor.CLASSIFICATION_LEVEL_COLOR_RED_BOLD);
    }

    @Test
    void unknownEnumWireValuesMapToUnspecified() throws Exception {
        Page page = mapper.toPage(tree("""
                {"id": "1", "status": "teleported", "title": "t", "spaceId": "9",
                 "parentType": "wormhole"}
                """));
        assertThat(page.getStatus()).isEqualTo(ContentStatus.CONTENT_STATUS_UNSPECIFIED);
        assertThat(page.getParentType().name()).isEqualTo("PARENT_CONTENT_TYPE_UNSPECIFIED");
        assertThat(ConfluenceMapper.bodyFormat("hologram"))
                .isEqualTo(BodyFormat.BODY_FORMAT_UNSPECIFIED);
    }

    @Test
    void parsesRfc3339WithOffsets() {
        assertThat(ConfluenceMapper.timestamp("2024-03-01T12:00:00.000Z").getSeconds())
                .isEqualTo(ConfluenceMapper.timestamp("2024-03-01T14:00:00+02:00").getSeconds());
        assertThat(ConfluenceMapper.timestamp("")).isNull();
        assertThat(ConfluenceMapper.timestamp("not-a-date")).isNull();
    }

    @Test
    void mapsEveryBodyRepresentation() {
        assertThat(ConfluenceMapper.bodyFormat("storage")).isEqualTo(BodyFormat.BODY_FORMAT_STORAGE_XHTML);
        assertThat(ConfluenceMapper.bodyFormat("atlas_doc_format")).isEqualTo(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT);
        assertThat(ConfluenceMapper.bodyFormat("view")).isEqualTo(BodyFormat.BODY_FORMAT_RENDERED_XHTML);
        assertThat(ConfluenceMapper.bodyFormat("export_view")).isEqualTo(BodyFormat.BODY_FORMAT_EXPORT_XHTML);
        assertThat(ConfluenceMapper.bodyFormat("anonymous_export_view")).isEqualTo(BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML);
        assertThat(ConfluenceMapper.bodyFormat("styled_view")).isEqualTo(BodyFormat.BODY_FORMAT_STYLED_XHTML);
        assertThat(ConfluenceMapper.bodyFormat("editor")).isEqualTo(BodyFormat.BODY_FORMAT_EDITOR);
        assertThat(ConfluenceMapper.bodyFormat("wiki")).isEqualTo(BodyFormat.BODY_FORMAT_WIKI);
        assertThat(ConfluenceMapper.bodyFormat("raw")).isEqualTo(BodyFormat.BODY_FORMAT_RAW);
        assertThat(ConfluenceMapper.bodyFormat("plain")).isEqualTo(BodyFormat.BODY_FORMAT_PLAIN_TEXT);
    }
}
