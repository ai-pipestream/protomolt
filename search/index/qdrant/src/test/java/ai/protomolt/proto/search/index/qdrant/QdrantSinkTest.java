package ai.protomolt.proto.search.index.qdrant;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import qdrant.Collections.CollectionExists;
import qdrant.Collections.CollectionExistsRequest;
import qdrant.Collections.CollectionExistsResponse;
import qdrant.Collections.CollectionOperationResponse;
import qdrant.Collections.CreateCollection;
import qdrant.Collections.Distance;
import qdrant.CollectionsGrpc;
import qdrant.Common.PointId;
import qdrant.Points.DenseVector;
import qdrant.Points.NamedVectors;
import qdrant.Points.PointStruct;
import qdrant.Points.PointsOperationResponse;
import qdrant.Points.SearchPoints;
import qdrant.Points.SearchResponse;
import qdrant.Points.UpsertPoints;
import qdrant.Points.Vector;
import qdrant.Points.Vectors;
import qdrant.PointsGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink against a recording in-process gRPC server: request shapes, idempotency, the
 * create-race fallback, and the api-key header.
 */
class QdrantSinkTest {

    private static final Metadata.Key<String> API_KEY =
            Metadata.Key.of(QdrantSink.API_KEY_HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private final FakeCollections collections = new FakeCollections();
    private final FakePoints points = new FakePoints();
    private final RecordingHeaders headers = new RecordingHeaders();

    private Server server;
    private ManagedChannel channel;
    private QdrantSink sink;

    @BeforeEach
    void startServer() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(collections)
                .addService(points)
                .intercept(headers)
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        sink = new QdrantSink(channel);
    }

    @AfterEach
    void stopServer() {
        sink.close(); // caller-owned channel: must stay open
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void ensureCollectionCreatesWithNamedVectors() {
        assertThat(sink.ensureCollection("chunks", Map.of("mini", 384, "e5", 768))).isTrue();

        assertThat(collections.existsChecks).containsExactly("chunks");
        assertThat(collections.creates).hasSize(1);
        CreateCollection create = collections.creates.get(0);
        assertThat(create.getCollectionName()).isEqualTo("chunks");
        Map<String, qdrant.Collections.VectorParams> params =
                create.getVectorsConfig().getParamsMap().getMapMap();
        assertThat(params.keySet()).containsExactlyInAnyOrder("mini", "e5");
        assertThat(params.get("mini").getSize()).isEqualTo(384);
        assertThat(params.get("e5").getSize()).isEqualTo(768);
        assertThat(params.get("mini").getDistance()).isEqualTo(Distance.Cosine);
    }

    @Test
    void ensureCollectionHonorsSpecDistances() {
        assertThat(sink.ensureCollection("chunks", List.of(
                new QdrantVectorSpec("mini", 384, Distance.Euclid),
                QdrantVectorSpec.cosine("e5", 768)))).isTrue();

        Map<String, qdrant.Collections.VectorParams> params =
                collections.creates.get(0).getVectorsConfig().getParamsMap().getMapMap();
        assertThat(params.get("mini").getDistance()).isEqualTo(Distance.Euclid);
        assertThat(params.get("e5").getDistance()).isEqualTo(Distance.Cosine);
    }

    @Test
    void ensureCollectionLeavesAnExistingCollectionUntouched() {
        collections.exists.set(true);

        assertThat(sink.ensureCollection("chunks", Map.of("mini", 384))).isFalse();
        assertThat(collections.creates).isEmpty();
    }

    @Test
    void ensureCollectionCreateRaceCountsAsExisting() {
        collections.failCreateWithAlreadyExists.set(true);

        assertThat(sink.ensureCollection("chunks", Map.of("mini", 384))).isFalse();
    }

    @Test
    void ensureCollectionPropagatesOtherFailures() {
        collections.failCreateWithUnavailable.set(true);

        assertThatThrownBy(() -> sink.ensureCollection("chunks", Map.of("mini", 384)))
                .isInstanceOf(io.grpc.StatusRuntimeException.class)
                .satisfies(e -> assertThat(((io.grpc.StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void ensureCollectionForPointsDerivesTheVectorSizes() {
        PointStruct point = point("p-1", "mini", 3);

        assertThat(sink.ensureCollectionForPoints("chunks", List.of(point))).isTrue();
        assertThat(collections.creates.get(0).getVectorsConfig().getParamsMap().getMapMap()
                .get("mini").getSize()).isEqualTo(3);

        // An empty batch is a no-op.
        assertThat(sink.ensureCollectionForPoints("other", List.of())).isFalse();
        assertThat(collections.existsChecks).containsExactly("chunks");
    }

    @Test
    void upsertWaitsAndSendsEveryPoint() {
        List<PointStruct> batch = List.of(point("p-1", "mini", 2), point("p-2", "mini", 2));

        sink.upsert("chunks", batch);

        assertThat(points.upserts).hasSize(1);
        UpsertPoints request = points.upserts.get(0);
        assertThat(request.getCollectionName()).isEqualTo("chunks");
        assertThat(request.getWait()).isTrue();
        assertThat(request.getPointsList()).containsExactlyElementsOf(batch);

        // An empty batch sends nothing.
        sink.upsert("chunks", List.of());
        assertThat(points.upserts).hasSize(1);
    }

    @Test
    void searchNamesTheVectorAndAsksForPayload() {
        sink.search("chunks", "mini", List.of(0.1f, 0.2f), 5);

        SearchPoints request = points.searches.get(0);
        assertThat(request.getCollectionName()).isEqualTo("chunks");
        assertThat(request.getVectorName()).isEqualTo("mini");
        assertThat(request.getVectorList()).containsExactly(0.1f, 0.2f);
        assertThat(request.getLimit()).isEqualTo(5);
        assertThat(request.getWithPayload().getEnable()).isTrue();
    }

    @Test
    void searchWithoutVectorNameOmitsIt() {
        sink.search("chunks", null, List.of(0.1f), 1);
        assertThat(points.searches.get(0).hasVectorName()).isFalse();
    }

    @Test
    void apiKeyTravelsAsMetadata() {
        QdrantSink keyed = new QdrantSink(channel, false, "secret-key");

        keyed.upsert("chunks", List.of(point("p-1", "mini", 1)));

        assertThat(headers.apiKeys).containsExactly("secret-key");
    }

    private static PointStruct point(String uuid, String vectorName, int dims) {
        DenseVector.Builder dense = DenseVector.newBuilder();
        for (int i = 0; i < dims; i++) {
            dense.addData(0.1f * (i + 1));
        }
        return PointStruct.newBuilder()
                .setId(PointId.newBuilder().setUuid(uuid))
                .setVectors(Vectors.newBuilder().setVectors(NamedVectors.newBuilder()
                        .putVectors(vectorName, Vector.newBuilder().setDense(dense).build())))
                .build();
    }

    private static final class FakeCollections extends CollectionsGrpc.CollectionsImplBase {
        final AtomicBoolean exists = new AtomicBoolean();
        final AtomicBoolean failCreateWithAlreadyExists = new AtomicBoolean();
        final AtomicBoolean failCreateWithUnavailable = new AtomicBoolean();
        final List<String> existsChecks = new ArrayList<>();
        final List<CreateCollection> creates = new ArrayList<>();

        @Override
        public void collectionExists(CollectionExistsRequest request,
                                     StreamObserver<CollectionExistsResponse> observer) {
            existsChecks.add(request.getCollectionName());
            observer.onNext(CollectionExistsResponse.newBuilder()
                    .setResult(CollectionExists.newBuilder().setExists(exists.get()))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void create(CreateCollection request,
                           StreamObserver<CollectionOperationResponse> observer) {
            if (failCreateWithAlreadyExists.get()) {
                observer.onError(Status.ALREADY_EXISTS.asRuntimeException());
                return;
            }
            if (failCreateWithUnavailable.get()) {
                observer.onError(Status.UNAVAILABLE.asRuntimeException());
                return;
            }
            creates.add(request);
            observer.onNext(CollectionOperationResponse.newBuilder().setResult(true).build());
            observer.onCompleted();
        }
    }

    private static final class FakePoints extends PointsGrpc.PointsImplBase {
        final List<UpsertPoints> upserts = new ArrayList<>();
        final List<SearchPoints> searches = new ArrayList<>();

        @Override
        public void upsert(UpsertPoints request,
                           StreamObserver<PointsOperationResponse> observer) {
            upserts.add(request);
            observer.onNext(PointsOperationResponse.newBuilder().build());
            observer.onCompleted();
        }

        @Override
        public void search(SearchPoints request, StreamObserver<SearchResponse> observer) {
            searches.add(request);
            observer.onNext(SearchResponse.newBuilder().build());
            observer.onCompleted();
        }
    }

    private static final class RecordingHeaders implements ServerInterceptor {
        final ConcurrentLinkedQueue<String> apiKeys = new ConcurrentLinkedQueue<>();

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata metadata, ServerCallHandler<ReqT, RespT> next) {
            String apiKey = metadata.get(API_KEY);
            if (apiKey != null) {
                apiKeys.add(apiKey);
            }
            return next.startCall(call, metadata);
        }
    }
}
