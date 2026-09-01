package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.Attachment;
import ai.pipestream.proto.acquire.confluence.v1.BlogPost;
import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ChangeSource;
import ai.pipestream.proto.acquire.confluence.v1.Comment;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.acquire.confluence.v1.ContentProperty;
import ai.pipestream.proto.acquire.confluence.v1.Label;
import ai.pipestream.proto.acquire.confluence.v1.Page;
import ai.pipestream.proto.acquire.confluence.v1.Space;
import ai.pipestream.proto.acquire.confluence.v1.SpaceProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The crawl orchestration: enumerate spaces (or the configured allowlist),
 * then per space sweep pages and blog posts - bodies in the configured
 * format - plus each item's footer and inline comments, attachments
 * (metadata; bytes come from {@link ConfluenceClient#downloadAttachmentBytes}
 * on demand), labels, and properties. Every entity leaves as a
 * {@code ConfluenceChange{UPSERT}} and each completed space leaves one
 * {@code ConfluenceSnapshot} with per-kind counts and the resume cursor.
 *
 * <p>Full crawls emit {@code source=CRAWL} with the run id as the change
 * cursor. {@link #crawlIncremental(String)} lists pages and blog posts with
 * {@code sort=-modified-date} (a v2 list-endpoint parameter per the spec's
 * PageSortOrder/BlogPostSortOrder), walks newest-first, stops at the stored
 * cursor timestamp, and emits {@code source=CQL_INCREMENTAL}.</p>
 *
 * <p>Work fans out on virtual threads; the sink must be thread-safe.</p>
 */
public final class ConfluenceCrawler {

    private final ConfluenceConnectorConfig config;
    private final ConfluenceClient client;
    private final ConfluenceMapper mapper;
    private final ChangeSink sink;

    public ConfluenceCrawler(ConfluenceConnectorConfig config, ConfluenceClient client,
            ChangeSink sink) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = new ConfluenceMapper(config.baseUrl());
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * A full crawl of every space the credentials can see, or of the
     * configured space-key allowlist.
     */
    public void crawl() throws IOException, InterruptedException {
        String runId = UUID.randomUUID().toString();
        for (JsonNode spaceNode : listAll("/api/v2/spaces", spacesQuery())) {
            Space space = mapper.toSpace(spaceNode);
            if (config.hasSpaceAllowlist() && !config.spaces().contains(space.getKey())) {
                continue;
            }
            emit(entity(space.getId()).setSpace(space).build(), ChangeSource.CHANGE_SOURCE_CRAWL, runId);
            crawlSpace(space, runId, ChangeSource.CHANGE_SOURCE_CRAWL, null, runId);
        }
    }

    /**
     * An incremental sync: only pages and blog posts (and their sub-entities)
     * modified after {@code sinceRfc3339} are emitted, walking the v2 list
     * endpoints newest-first via {@code sort=-modified-date} and stopping at
     * the cursor.
     *
     * @param sinceRfc3339 the cursor a previous crawl left on its snapshot
     *        (an RFC3339 timestamp)
     * @return the new cursor: the newest modification timestamp observed, or
     *         {@code sinceRfc3339} unchanged when nothing moved
     */
    public String crawlIncremental(String sinceRfc3339) throws IOException, InterruptedException {
        Objects.requireNonNull(sinceRfc3339, "sinceRfc3339");
        Timestamp since = ConfluenceMapper.timestamp(sinceRfc3339);
        AtomicReference<Timestamp> newest = new AtomicReference<>(since);
        for (JsonNode spaceNode : listAll("/api/v2/spaces", spacesQuery())) {
            Space space = mapper.toSpace(spaceNode);
            if (config.hasSpaceAllowlist() && !config.spaces().contains(space.getKey())) {
                continue;
            }
            Timestamp spaceNewest = crawlSpace(space, UUID.randomUUID().toString(),
                    ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL, since, sinceRfc3339);
            newest.accumulateAndGet(spaceNewest, ConfluenceCrawler::later);
        }
        Timestamp result = newest.get();
        return result == null ? sinceRfc3339 : rfc3339(result);
    }

    /**
     * One space sweep. When {@code stopAt} is non-null the sweep is
     * incremental: content lists walk newest-first and stop at that
     * timestamp. Returns the newest modification timestamp observed.
     */
    private Timestamp crawlSpace(Space space, String runId, ChangeSource source, Timestamp stopAt,
            String changeCursor) throws IOException, InterruptedException {
        Instant started = Instant.now();
        Map<String, Long> counts = new ConcurrentHashMap<>();
        AtomicReference<Timestamp> newest = new AtomicReference<>(stopAt);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> {
                try {
                    crawlContent(space, "page", source, stopAt, changeCursor, counts, newest);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            executor.submit(() -> {
                try {
                    crawlContent(space, "blogpost", source, stopAt, changeCursor, counts, newest);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            // Space properties ride along; they are one cheap call per space.
            executor.submit(() -> {
                try {
                    for (JsonNode node : listAll("/api/v2/spaces/" + space.getId() + "/properties",
                            Map.of("limit", String.valueOf(config.pageSize())))) {
                        SpaceProperty property = mapper.toSpaceProperty(node);
                        emit(entity(property.getId()).setSpaceProperty(property).build(), source,
                                changeCursor);
                        count(counts, "space_property");
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            executor.shutdown();
            awaitTermination(executor);
        }
        if (failure.get() != null) {
            rethrow(failure.get());
        }

        if (source == ChangeSource.CHANGE_SOURCE_CRAWL) {
            sink.snapshot(ConfluenceSnapshot.newBuilder()
                    .setSnapshotId(runId + "-" + space.getKey())
                    .setSpaceKey(space.getKey())
                    .putAllEntityCounts(sortedCounts(counts))
                    .setCursor(rfc3339(newest.get()))
                    .setStartedAt(timestamp(started))
                    .setCompletedAt(timestamp(Instant.now()))
                    .build());
        }
        return newest.get();
    }

    /** Pages or blog posts of one space, newest-first when incremental, with all sub-entities. */
    private void crawlContent(Space space, String kind, ChangeSource source, Timestamp stopAt,
            String changeCursor, Map<String, Long> counts, AtomicReference<Timestamp> newest)
            throws IOException, InterruptedException {
        Map<String, String> query = new TreeMap<>();
        query.put("space-id", space.getId());
        query.put("limit", String.valueOf(config.pageSize()));
        query.put("body-format", config.bodyFormat());
        if (stopAt != null) {
            query.put("sort", "-modified-date");
        }
        String path = "/api/v2/" + ("page".equals(kind) ? "pages" : "blogposts");

        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            String url = path;
            Map<String, String> params = query;
            boolean reachedCursor = false;
            while (url != null && !reachedCursor && failure.get() == null) {
                ConfluenceClient.ResultPage resultPage = client.getPage(url, params);
                url = resultPage.nextUrl();
                params = Map.of(); // the next URL already carries cursor and query
                for (JsonNode node : resultPage.body().path("results")) {
                    Timestamp modified = ConfluenceMapper.timestamp(
                            node.path("version").path("createdAt").asText(""));
                    if (stopAt != null && modified != null && compare(modified, stopAt) <= 0) {
                        // Content at or older than the cursor: the sweep is done.
                        reachedCursor = true;
                        break;
                    }
                    if (modified != null) {
                        newest.accumulateAndGet(modified, ConfluenceCrawler::later);
                    }
                    executor.submit(() -> {
                        try {
                            if ("page".equals(kind)) {
                                crawlPage(node, source, changeCursor, counts);
                            } else {
                                crawlBlogPost(node, source, changeCursor, counts);
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    });
                }
            }
            executor.shutdown();
            awaitTermination(executor);
        }
        if (failure.get() != null) {
            rethrow(failure.get());
        }
    }

    private void crawlPage(JsonNode node, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        Page page = mapper.toPage(node);
        emit(entity(page.getId()).setPage(page).build(), source, changeCursor);
        count(counts, "page");
        crawlComments("/api/v2/pages/" + page.getId(), source, changeCursor, counts);
        crawlSubEntities("/api/v2/pages/" + page.getId(), source, changeCursor, counts);
    }

    private void crawlBlogPost(JsonNode node, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        BlogPost blogPost = mapper.toBlogPost(node);
        emit(entity(blogPost.getId()).setBlogPost(blogPost).build(), source, changeCursor);
        count(counts, "blogpost");
        crawlComments("/api/v2/blogposts/" + blogPost.getId(), source, changeCursor, counts);
        crawlSubEntities("/api/v2/blogposts/" + blogPost.getId(), source, changeCursor, counts);
    }

    /** Footer plus inline comments of one page or blog post. */
    private void crawlComments(String contentPath, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        for (String commentKind : List.of("footer-comments", "inline-comments")) {
            for (JsonNode node : listAll(contentPath + "/" + commentKind,
                    Map.of("body-format", config.bodyFormat(),
                            "limit", String.valueOf(config.pageSize())))) {
                Comment comment = mapper.toComment(node);
                emit(entity(comment.getId()).setComment(comment).build(), source, changeCursor);
                count(counts, "comment");
            }
        }
    }

    /** Attachments (metadata only), labels and content properties of one page or blog post. */
    private void crawlSubEntities(String contentPath, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        for (JsonNode node : listAll(contentPath + "/attachments",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            Attachment attachment = mapper.toAttachment(node);
            emit(entity(attachment.getId()).setAttachment(attachment).build(), source,
                    changeCursor);
            count(counts, "attachment");
        }
        for (JsonNode node : listAll(contentPath + "/labels",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            Label label = mapper.toLabel(node);
            emit(entity(label.getId()).setLabel(label).build(), source, changeCursor);
            count(counts, "label");
        }
        for (JsonNode node : listAll(contentPath + "/properties",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            ContentProperty property = mapper.toContentProperty(node);
            emit(entity(property.getId()).setContentProperty(property).build(), source,
                    changeCursor);
            count(counts, "content_property");
        }
    }

    // ======================================================================
    // PLUMBING
    // ======================================================================

    private Map<String, String> spacesQuery() {
        Map<String, String> query = new TreeMap<>();
        query.put("limit", String.valueOf(config.pageSize()));
        query.put("description-format", "view");
        if (config.hasSpaceAllowlist()) {
            query.put("keys", String.join(",", config.spaces()));
        }
        return query;
    }

    /** Walks a cursor-paginated list endpoint to exhaustion, following _links.next. */
    private List<JsonNode> listAll(String path, Map<String, String> query)
            throws IOException, InterruptedException {
        List<JsonNode> results = new ArrayList<>();
        String url = path;
        Map<String, String> params = query;
        while (url != null) {
            ConfluenceClient.ResultPage page = client.getPage(url, params);
            page.body().path("results").forEach(results::add);
            url = page.nextUrl();
            params = Map.of();
        }
        return results;
    }

    private void emit(ConfluenceEntity entity, ChangeSource source, String cursor) {
        sink.emit(ConfluenceChange.newBuilder()
                .setChangeId(UUID.randomUUID().toString())
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(entity)
                .setCursor(cursor)
                .setSource(source)
                .setOccurredAt(timestamp(Instant.now()))
                .build());
    }

    private static ConfluenceEntity.Builder entity(String entityId) {
        return ConfluenceEntity.newBuilder()
                .setEntityId(entityId)
                .setIngestedAt(timestamp(Instant.now()));
    }

    private static void count(Map<String, Long> counts, String kind) {
        counts.merge(kind, 1L, Long::sum);
    }

    private static Map<String, Long> sortedCounts(Map<String, Long> counts) {
        return new TreeMap<>(counts);
    }

    /** Timestamp comparison, seconds first (protobuf-java only, no Timestamps util). */
    private static int compare(Timestamp a, Timestamp b) {
        int bySeconds = Long.compare(a.getSeconds(), b.getSeconds());
        return bySeconds != 0 ? bySeconds : Integer.compare(a.getNanos(), b.getNanos());
    }

    /** The later of two timestamps (null-safe). */
    private static Timestamp later(Timestamp a, Timestamp b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return compare(a, b) >= 0 ? a : b;
    }

    private static String rfc3339(Timestamp timestamp) {
        if (timestamp == null) {
            return "";
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toString();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static void awaitTermination(ExecutorService executor) throws InterruptedException {
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            // keep waiting; virtual-thread tasks are short-lived HTTP reads
        }
    }

    private static void rethrow(Throwable t) throws IOException, InterruptedException {
        switch (t) {
            case IOException e -> throw e;
            case UncheckedIOException e -> throw e.getCause();
            case InterruptedException e -> throw e;
            case RuntimeException e -> throw e;
            default -> throw new IOException("crawl task failed", t);
        }
    }
}
