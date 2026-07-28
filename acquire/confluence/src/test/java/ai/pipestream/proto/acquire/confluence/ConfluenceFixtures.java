package ai.pipestream.proto.acquire.confluence;

/**
 * REST v2 response fixtures for the tests, shaped after the field names of
 * the official Confluence Cloud v2 OpenAPI spec (spec-declared camelCase,
 * ids as strings, RFC3339 date-times, {@code _links} blocks).
 */
final class ConfluenceFixtures {

    private ConfluenceFixtures() {
    }

    static String spaceJson(String id, String key, String name) {
        return """
                {
                  "id": "%s",
                  "key": "%s",
                  "name": "%s",
                  "type": "global",
                  "status": "current",
                  "authorId": "acc-1",
                  "spaceOwnerId": "acc-1",
                  "currentActiveAlias": "%s",
                  "createdAt": "2024-01-02T03:04:05.000Z",
                  "homepageId": "900",
                  "description": {
                    "plain": {"representation": "plain", "value": "%s space"},
                    "view": {"representation": "view", "value": "<p>%s space</p>"}
                  },
                  "icon": {
                    "path": "/wiki/aa-avatar/%s.png",
                    "apiDownloadLink": "/download/avatars/%s.png"
                  },
                  "_links": {"webui": "/spaces/%s"}
                }
                """.formatted(id, key, name, key, name, name, key, key, key);
    }

    /** A space list response ({@code MultiEntityResult<Space>}); {@code next} may be null. */
    static String spaceListJson(String nextCursor, String... spaceJsons) {
        return listJson(nextCursor, "/wiki/api/v2/spaces", spaceJsons);
    }

    static String pageJson(String id, String spaceId, String title, String modifiedAt) {
        return """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "%s",
                  "spaceId": "%s",
                  "parentId": null,
                  "parentType": "page",
                  "position": 1,
                  "authorId": "acc-1",
                  "ownerId": "acc-1",
                  "lastOwnerId": null,
                  "subtype": "page",
                  "createdAt": "2024-02-01T10:00:00.000Z",
                  "version": {
                    "createdAt": "%s",
                    "message": "edit",
                    "number": 3,
                    "minorEdit": false,
                    "authorId": "acc-2"
                  },
                  "body": {
                    "storage": {"representation": "storage", "value": "<p>Hello %s</p>"}
                  },
                  "_links": {"webui": "/spaces/ENG/pages/%s/%s"}
                }
                """.formatted(id, title, spaceId, modifiedAt, title, id, title.replace(' ', '+'));
    }

    static String pageListJson(String nextCursor, String... pageJsons) {
        return listJson(nextCursor, "/wiki/api/v2/pages", pageJsons);
    }

    static String blogPostJson(String id, String spaceId, String title, String modifiedAt) {
        return """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "%s",
                  "spaceId": "%s",
                  "authorId": "acc-1",
                  "createdAt": "2024-02-03T09:00:00.000Z",
                  "version": {
                    "createdAt": "%s",
                    "message": "",
                    "number": 1,
                    "minorEdit": false,
                    "authorId": "acc-1"
                  },
                  "body": {
                    "storage": {"representation": "storage", "value": "<p>Post %s</p>"}
                  },
                  "_links": {"webui": "/spaces/ENG/blogposts/%s"}
                }
                """.formatted(id, title, spaceId, modifiedAt, title, id);
    }

    static String blogPostListJson(String nextCursor, String... blogPostJsons) {
        return listJson(nextCursor, "/wiki/api/v2/blogposts", blogPostJsons);
    }

    static String footerCommentJson(String id, String pageId) {
        return """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "re: page",
                  "pageId": "%s",
                  "version": {"createdAt": "2024-02-05T11:00:00.000Z", "message": "",
                              "number": 1, "minorEdit": false, "authorId": "acc-3"},
                  "body": {"storage": {"representation": "storage", "value": "<p>Nice</p>"}},
                  "_links": {"webui": "/spaces/ENG/pages/%s#comment-%s"}
                }
                """.formatted(id, pageId, pageId, id);
    }

    static String inlineCommentJson(String id, String pageId) {
        return """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "",
                  "pageId": "%s",
                  "version": {"createdAt": "2024-02-06T11:00:00.000Z", "message": "",
                              "number": 1, "minorEdit": false, "authorId": "acc-4"},
                  "body": {"storage": {"representation": "storage", "value": "<p>Typo?</p>"}},
                  "resolutionStatus": "open",
                  "properties": {
                    "inlineMarkerRef": "marker-1",
                    "inlineOriginalSelection": "teh"
                  },
                  "_links": {"webui": "/spaces/ENG/pages/%s#comment-%s"}
                }
                """.formatted(id, pageId, pageId, id);
    }

    static String attachmentJson(String id, String pageId) {
        return """
                {
                  "id": "%s",
                  "status": "current",
                  "title": "diagram.png",
                  "createdAt": "2024-02-07T08:30:00.000Z",
                  "pageId": "%s",
                  "mediaType": "image/png",
                  "mediaTypeDescription": "PNG Image",
                  "comment": "architecture diagram",
                  "fileId": "file-xyz",
                  "fileSize": 12345,
                  "webuiLink": "/spaces/ENG/pages/%s#attachment-%s",
                  "downloadLink": "/download/attachments/%s/diagram.png",
                  "version": {"createdAt": "2024-02-07T08:30:00.000Z", "message": "",
                              "number": 1, "minorEdit": false, "authorId": "acc-1"},
                  "_links": {
                    "webui": "/spaces/ENG/pages/%s#attachment-%s",
                    "download": "/download/attachments/%s/diagram.png"
                  }
                }
                """.formatted(id, pageId, pageId, id, id, pageId, id, id);
    }

    static String labelJson(String id, String name) {
        return """
                {"id": "%s", "name": "%s", "prefix": "global"}
                """.formatted(id, name);
    }

    static String propertyJson(String id, String key, String valueJsonLiteral) {
        return """
                {
                  "id": "%s",
                  "key": "%s",
                  "value": %s,
                  "version": {"createdAt": "2024-02-08T10:00:00.000Z", "message": "",
                              "number": 2, "minorEdit": false, "authorId": "acc-1"}
                }
                """.formatted(id, key, valueJsonLiteral);
    }

    static String spacePropertyJson(String id, String key, String valueJsonLiteral) {
        return """
                {
                  "id": "%s",
                  "key": "%s",
                  "value": %s,
                  "createdAt": "2024-02-09T10:00:00.000Z",
                  "createdBy": "acc-1",
                  "version": {"createdAt": "2024-02-09T10:00:00.000Z", "createdBy": "acc-1",
                              "message": "", "number": 1}
                }
                """.formatted(id, key, valueJsonLiteral);
    }

    /** An empty list response for routes a test does not care about. */
    static String emptyListJson() {
        return "{\"results\": []}";
    }

    static String listJson(String nextCursor, String nextPath, String... results) {
        StringBuilder sb = new StringBuilder("{\"results\": [");
        sb.append(String.join(",", results));
        sb.append(']');
        if (nextCursor != null) {
            sb.append(", \"_links\": {\"next\": \"").append(nextPath)
                    .append("?cursor=").append(nextCursor).append("\"}");
        }
        sb.append('}');
        return sb.toString();
    }
}
