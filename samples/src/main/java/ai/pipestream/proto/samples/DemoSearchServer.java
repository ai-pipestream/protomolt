package ai.pipestream.proto.samples;

import demo.search.v1.DemoSearchGrpc;
import demo.search.v1.SearchHit;
import demo.search.v1.SearchRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Demo server-streaming search on localhost:9777. Streams synthetic hits one at a time,
 * best first, with a per-hit delay so a streaming consumer visibly receives them
 * incrementally. Run with {@code ./gradlew :samples:runDemoSearch}.
 */
public final class DemoSearchServer {

    private static final int PORT = 9777;

    private static final String[] TEXTS = {
        "approximate nearest neighbor search with HNSW graphs",
        "vector quantization for billion-scale indexes",
        "recall at high k: merging partial results from many shards",
        "cross-encoder reranking of the top candidates",
        "embedding equivalence across serving runtimes",
        "the shared floor: pruning early with a distributed threshold",
        "index construction for approximate nearest neighbors",
        "scoring functions: cosine, dot product, and L2",
    };

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new DemoSearchGrpc.DemoSearchImplBase() {
                    @Override
                    public void search(SearchRequest request, StreamObserver<SearchHit> out) {
                        int hits = request.getHits() <= 0 ? 5 : request.getHits();
                        long delayMs = request.getDelayMs() <= 0 ? 400 : request.getDelayMs();
                        for (int i = 0; i < hits; i++) {
                            out.onNext(SearchHit.newBuilder()
                                    .setDocId("doc-" + (i + 1))
                                    .setScore(0.98f - 0.07f * i)
                                    .setText(TEXTS[i % TEXTS.length])
                                    .build());
                            try {
                                Thread.sleep(delayMs);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                        out.onCompleted();
                    }
                })
                .build()
                .start();
        System.out.println("DemoSearch listening on localhost:" + PORT
                + " (server-streaming; Ctrl+C to stop)");
        server.awaitTermination();
    }

    private DemoSearchServer() {
    }
}
