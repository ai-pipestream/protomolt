package ai.pipestream.proto.acquire.s3;

import ai.pipestream.proto.acquire.pull.IntakeFeed;
import ai.pipestream.proto.acquire.pull.PullDocuments;
import ai.pipestream.proto.acquire.pull.PullReport;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import com.google.protobuf.ByteString;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One S3 pull pass: list the bucket (optionally under a prefix), take every object strictly
 * past the watermark in {@code (lastModified, key)} order, fetch it, and feed it through the
 * intake service with stable identity {@code s3://bucket/key} — so a changed object re-saves its
 * own document and an unchanged one dedupes at the repository.
 *
 * <p>The watermark is {@code <epochMillis>/<key>} of the newest object a previous pull
 * processed. Incremental pulls still list the whole keyspace (S3 lists by key, not by time;
 * listing is metadata-only) and filter by the watermark; content only transfers for what is
 * actually new or changed. The connector holds no state — the watermark travels in the request
 * and out in the {@link PullReport}.</p>
 */
public final class S3Pull {

    /** The connector identity stamped on pulled documents: {@value}. */
    public static final String CONNECTOR_ID = "s3-pull";

    private final S3Client s3;
    private final IntakeFeed feed;

    /**
     * Creates the pull core.
     *
     * @param s3 the source-side client
     * @param feed the intake submission seam
     */
    public S3Pull(S3Client s3, IntakeFeed feed) {
        if (s3 == null) {
            throw new IllegalArgumentException("s3 client must not be null");
        }
        if (feed == null) {
            throw new IllegalArgumentException("feed must not be null");
        }
        this.s3 = s3;
        this.feed = feed;
    }

    /**
     * One pull pass.
     *
     * @param bucket the source bucket
     * @param prefix key prefix to restrict the pull, or blank for the whole bucket
     * @param datasourceId the datasource pulled documents belong to
     * @param drive the target drive, or blank for intake's default
     * @param watermark the previous pull's watermark, or blank for a first pull
     * @param maxObjects cap on objects processed this pass, or 0 for no cap
     */
    public PullReport pull(String bucket, String prefix, String datasourceId, String drive,
                           String watermark, int maxObjects) {
        requireNonBlank(bucket, "bucket");
        requireNonBlank(datasourceId, "datasourceId");
        Mark from = Mark.parse(watermark);

        List<S3Object> candidates = new ArrayList<>();
        String continuation = null;
        do {
            ListObjectsV2Request.Builder list = ListObjectsV2Request.builder().bucket(bucket);
            if (prefix != null && !prefix.isBlank()) {
                list.prefix(prefix);
            }
            if (continuation != null) {
                list.continuationToken(continuation);
            }
            ListObjectsV2Response page = s3.listObjectsV2(list.build());
            for (S3Object object : page.contents()) {
                if (from == null || new Mark(object.lastModified(), object.key()).isAfter(from)) {
                    candidates.add(object);
                }
            }
            continuation = Boolean.TRUE.equals(page.isTruncated())
                    ? page.nextContinuationToken() : null;
        } while (continuation != null);

        candidates.sort(Comparator.comparing(S3Object::lastModified)
                .thenComparing(S3Object::key));
        if (maxObjects > 0 && candidates.size() > maxObjects) {
            candidates = candidates.subList(0, maxObjects);
        }

        PullReport.Accumulator report = new PullReport.Accumulator(watermark);
        for (S3Object object : candidates) {
            String sourceKey = "s3://" + bucket + "/" + object.key();
            try {
                ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(object.key()).build());
                String contentType = bytes.response().contentType();
                Map<String, String> metadata = new LinkedHashMap<>();
                metadata.put("source", "s3");
                metadata.put("bucket", bucket);
                metadata.put("key", object.key());
                metadata.put("etag", object.eTag() == null ? "" : object.eTag());
                metadata.put("last_modified", object.lastModified().toString());
                IngestDocumentResponse receipt = feed.submit(
                        PullDocuments.document(CONNECTOR_ID, datasourceId, sourceKey,
                                ByteString.copyFrom(bytes.asByteArray()),
                                filenameOf(object.key()),
                                contentType == null ? "" : contentType),
                        datasourceId, drive, metadata);
                report.success(receipt.getDeduplicated(),
                        new Mark(object.lastModified(), object.key()).format());
            } catch (RuntimeException e) {
                report.failure(sourceKey + ": " + e.getMessage());
            }
        }
        return report.report();
    }

    private static String filenameOf(String key) {
        int slash = key.lastIndexOf('/');
        return slash < 0 ? key : key.substring(slash + 1);
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    /** The watermark tuple: strictly ordered by last-modified instant, then key. */
    record Mark(Instant lastModified, String key) {

        boolean isAfter(Mark other) {
            int byInstant = lastModified.compareTo(other.lastModified());
            return byInstant != 0 ? byInstant > 0 : key.compareTo(other.key()) > 0;
        }

        String format() {
            return lastModified.toEpochMilli() + "/" + key;
        }

        /** Parses {@code <epochMillis>/<key>}; blank means no watermark (a first pull). */
        static Mark parse(String watermark) {
            if (watermark == null || watermark.isBlank()) {
                return null;
            }
            int slash = watermark.indexOf('/');
            if (slash <= 0) {
                throw new IllegalArgumentException(
                        "watermark must be <epochMillis>/<key>; got '" + watermark + "'");
            }
            try {
                return new Mark(Instant.ofEpochMilli(
                        Long.parseLong(watermark.substring(0, slash))),
                        watermark.substring(slash + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "watermark must be <epochMillis>/<key>; got '" + watermark + "'");
            }
        }
    }
}
