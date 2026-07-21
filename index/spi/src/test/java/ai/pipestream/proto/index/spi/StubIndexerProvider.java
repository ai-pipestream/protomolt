package ai.pipestream.proto.index.spi;

import com.google.protobuf.Message;

/**
 * A provider registered in this module's test {@code META-INF/services} so the ServiceLoader
 * seam has something to discover; the SPI module ships no engine of its own.
 */
public final class StubIndexerProvider implements SearchEngineIndexerProvider {

    static final String ENGINE_ID = "stub";

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new StubIndexer(context);
    }

    /** Records the context it was handed so the test can prove it was passed through. */
    static final class StubIndexer implements SearchEngineIndexer {

        private final IndexerContext context;

        StubIndexer(IndexerContext context) {
            this.context = context;
        }

        IndexerContext context() {
            return context;
        }

        @Override
        public String engineId() {
            return ENGINE_ID;
        }

        @Override
        public Object map(Message message, IndexingPlan plan) {
            return plan.messageFullName() + "/" + message.getDescriptorForType().getFullName();
        }
    }
}
