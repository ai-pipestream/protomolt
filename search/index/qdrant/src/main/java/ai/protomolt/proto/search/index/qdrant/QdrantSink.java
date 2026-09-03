package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.search.index.spi.IndexMapping;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import qdrant.Collections.CreateCollection;
import qdrant.Collections.CollectionExistsRequest;
import qdrant.Collections.Distance;
import qdrant.Collections.VectorParams;
import qdrant.Collections.VectorParamsMap;
import qdrant.Collections.VectorsConfig;
import qdrant.CollectionsGrpc;
import qdrant.Points.PointStruct;
import qdrant.Points.SearchPoints;
import qdrant.Points.SearchResponse;
import qdrant.Points.UpsertPoints;
import qdrant.Points.WithPayloadSelector;
import qdrant.PointsGrpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Thin gRPC sink for Qdrant: creates collections with named vectors and upserts the
 * {@link PointStruct}s the {@link QdrantPointMapper} produces. Talks to the stock Qdrant
 * gRPC API (vendored protos, port 6334 by default); no Qdrant client dependency.
 *
 * <p>{@link #ensureCollection} is idempotent: an existing collection is left untouched (its
 * vector configuration is neither compared nor updated), a missing one is created with one
 * named vector per {@link QdrantVectorSpec}. A create lost to a concurrent writer counts as
 * already existing.
 *
 * <p>{@link #upsert} sends one {@code Upsert} call with {@code wait=true}, so the write is
 * acknowledged once applied. Point ids come from the mapper and are deterministic, so a
 * re-index overwrites the same points.
 *
 * <p>The sink owns its channel when built from a {@link QdrantConfig}; {@link #close()}
 * shuts only an owned channel down. The sink is safe for concurrent use; the channel
 * multiplexes calls.
 */
public final class QdrantSink implements AutoCloseable {

    /** gRPC metadata header carrying the Qdrant API key: {@value}. */
    public static final String API_KEY_HEADER = "api-key";

    private static final long SHUTDOWN_SECONDS = 5;

    private final ManagedChannel channel;
    private final boolean ownsChannel;
    private final CollectionsGrpc.CollectionsBlockingStub collections;
    private final PointsGrpc.PointsBlockingStub points;

    /**
     * Creates a sink from a {@link QdrantConfig}, connecting eagerly to
     * {@link QdrantConfig#target()} (plaintext unless {@link QdrantConfig#useTls()}). The
     * sink owns the channel; {@link #close()} shuts it down.
     */
    public QdrantSink(QdrantConfig config) {
        this(newChannel(Objects.requireNonNull(config, "config")), true, config.apiKey());
    }

    /**
     * Creates a sink over a caller-owned channel with no API key; {@link #close()} leaves
     * {@code channel} open.
     */
    public QdrantSink(ManagedChannel channel) {
        this(channel, false, "");
    }

    // Package-private for tests (header assertions over an in-process channel).
    QdrantSink(ManagedChannel channel, boolean ownsChannel, String apiKey) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.ownsChannel = ownsChannel;
        CollectionsGrpc.CollectionsBlockingStub collectionsStub = CollectionsGrpc.newBlockingStub(channel);
        PointsGrpc.PointsBlockingStub pointsStub = PointsGrpc.newBlockingStub(channel);
        if (apiKey != null && !apiKey.isEmpty()) {
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of(API_KEY_HEADER, Metadata.ASCII_STRING_MARSHALLER), apiKey);
            collectionsStub = collectionsStub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(headers));
            pointsStub = pointsStub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        this.collections = collectionsStub;
        this.points = pointsStub;
    }

    private static ManagedChannel newChannel(QdrantConfig config) {
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(config.target());
        if (config.useTls()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        return builder.build();
    }

    /**
     * Creates the collection if it does not exist, with one named vector per spec. An
     * existing collection is left untouched.
     *
     * @param collection collection name
     * @param vectors named-vector specs (name, size, distance), e.g. from
     *        {@link QdrantPointMapper#vectorSpecs}
     * @return {@code true} when this call created the collection, {@code false} when it
     *         already existed
     * @throws StatusRuntimeException when the server refuses the create or cannot be reached
     */
    public boolean ensureCollection(String collection, Collection<QdrantVectorSpec> vectors) {
        Objects.requireNonNull(collection, "collection");
        Objects.requireNonNull(vectors, "vectors");
        boolean exists = collections.collectionExists(CollectionExistsRequest.newBuilder()
                .setCollectionName(collection)
                .build()).getResult().getExists();
        if (exists) {
            return false;
        }
        VectorParamsMap.Builder paramsMap = VectorParamsMap.newBuilder();
        vectors.forEach(spec -> paramsMap.putMap(spec.name(), VectorParams.newBuilder()
                .setSize(spec.size())
                .setDistance(spec.distance())
                .build()));
        try {
            collections.create(CreateCollection.newBuilder()
                    .setCollectionName(collection)
                    .setVectorsConfig(VectorsConfig.newBuilder().setParamsMap(paramsMap))
                    .build());
            return true;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                // Create race lost: another writer made the collection between the check and
                // the create.
                return false;
            }
            throw e;
        }
    }

    /**
     * {@link #ensureCollection(String, Collection)} with COSINE vectors sized by name, for
     * callers that already know the sizes.
     */
    public boolean ensureCollection(String collection, Map<String, Integer> vectorDimensions) {
        Objects.requireNonNull(vectorDimensions, "vectorDimensions");
        List<QdrantVectorSpec> specs = new ArrayList<>(vectorDimensions.size());
        vectorDimensions.forEach((name, size) -> specs.add(QdrantVectorSpec.cosine(name, size)));
        return ensureCollection(collection, specs);
    }

    /**
     * {@link #ensureCollection(String, Collection)} with the named-vector specs derived from
     * the points about to be written, honoring the mapping's VECTOR hint (declared dims and
     * similarity) where present. An empty batch creates nothing.
     *
     * @return {@code true} when this call created the collection
     */
    public boolean ensureCollectionForPoints(String collection, List<PointStruct> pointsBatch,
                                             IndexMapping mapping) {
        Objects.requireNonNull(pointsBatch, "pointsBatch");
        if (pointsBatch.isEmpty()) {
            return false;
        }
        return ensureCollection(collection, QdrantPointMapper.vectorSpecs(pointsBatch, mapping));
    }

    /**
     * {@link #ensureCollectionForPoints(String, List, IndexMapping)} with no mapping: sizes from
     * the data, COSINE distances.
     */
    public boolean ensureCollectionForPoints(String collection, List<PointStruct> pointsBatch) {
        return ensureCollectionForPoints(collection, pointsBatch, null);
    }

    /**
     * Upserts the points in one call with {@code wait=true}. An empty batch sends nothing.
     *
     * @throws StatusRuntimeException when the server rejects the write or cannot be reached
     */
    public void upsert(String collection, List<PointStruct> pointsBatch) {
        Objects.requireNonNull(collection, "collection");
        Objects.requireNonNull(pointsBatch, "pointsBatch");
        if (pointsBatch.isEmpty()) {
            return;
        }
        points.upsert(UpsertPoints.newBuilder()
                .setCollectionName(collection)
                .setWait(true)
                .addAllPoints(pointsBatch)
                .build());
    }

    /**
     * Dense-vector search over one named vector, returning full payloads.
     *
     * @param collection collection name
     * @param vectorName named vector to search (a sanitized model id), or {@code null} for
     *        the collection's default (unnamed) vector
     * @param vector query vector
     * @param limit max number of results
     */
    public SearchResponse search(String collection, String vectorName, List<Float> vector, int limit) {
        Objects.requireNonNull(collection, "collection");
        Objects.requireNonNull(vector, "vector");
        SearchPoints.Builder request = SearchPoints.newBuilder()
                .setCollectionName(collection)
                .addAllVector(vector)
                .setLimit(limit)
                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true));
        if (vectorName != null) {
            request.setVectorName(vectorName);
        }
        return points.search(request.build());
    }

    /** Shuts the channel down when this sink created it; caller-owned channels stay open. */
    @Override
    public void close() {
        if (ownsChannel) {
            channel.shutdown();
            try {
                channel.awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
