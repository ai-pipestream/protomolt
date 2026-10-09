package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

/** Checks between provider calls; does not claim to abort an in-flight synchronous call. */
final class ControlledBlobStore implements BlobStore {
    private final BlobStore delegate;
    private final RepositoryOperationControl control;
    ControlledBlobStore(BlobStore delegate, RepositoryOperationControl control) {
        this.delegate = java.util.Objects.requireNonNull(delegate);
        this.control = java.util.Objects.requireNonNull(control);
    }
    private <T> T call(Supplier<T> operation) {
        control.check();
        T result = operation.get();
        control.check();
        return result;
    }
    private void run(Runnable operation) { call(() -> { operation.run(); return null; }); }
    @Override public PutResult put(PutSpec spec, byte[] body) { return call(() -> delegate.put(spec, body)); }
    @Override public PutResult put(PutSpec spec, InputStream body, long length) { return call(() -> delegate.put(spec, body, length)); }
    @Override public GetResult get(String namespace, String key, String version) { return call(() -> delegate.get(namespace, key, version)); }
    @Override public GetResult getForUpdate(String namespace, String key) { return call(() -> delegate.getForUpdate(namespace, key)); }
    @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        return call(() -> delegate.conditionalPut(spec, body, condition));
    }
    @Override public void copy(String source, String key, String target, String targetKey) { run(() -> delegate.copy(source, key, target, targetKey)); }
    @Override public boolean delete(String namespace, String key) { return call(() -> delegate.delete(namespace, key)); }
    @Override public BatchDeleteResult deleteAll(String namespace, List<String> keys) { return call(() -> delegate.deleteAll(namespace, keys)); }
    @Override public List<ListedObject> list(String namespace, String prefix) { return call(() -> delegate.list(namespace, prefix)); }
    @Override public void headBucket(String namespace) { run(() -> delegate.headBucket(namespace)); }
    @Override public void headObject(String namespace, String key) { run(() -> delegate.headObject(namespace, key)); }
}
