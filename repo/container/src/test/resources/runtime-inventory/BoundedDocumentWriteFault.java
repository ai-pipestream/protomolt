package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import java.lang.reflect.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Drops one acknowledgement only after the real Redis adapter returns from PUT. */
final class BoundedDocumentWriteFault {
    private final AtomicBoolean armed=new AtomicBoolean();
    private final RuntimeException failure=new IllegalStateException("Injected lost Redis PUT acknowledgement");
    private volatile BlobStore store;
    private volatile BlobStore.PutSpec written;
    void arm() { armed.set(true); }
    BlobStore wrap(BlobStore actual) {
        store=actual;
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},
                (proxy,method,args) -> {
                    Object result;
                    try { result=method.invoke(actual,args); }
                    catch (InvocationTargetException original) { throw original.getCause(); }
                    if (method.getName().equals("put") && armed.compareAndSet(true,false)) {
                        written=(BlobStore.PutSpec)args[0];
                        throw failure;
                    }
                    return result;
                });
    }
    boolean caused(Throwable error) {
        for (Throwable cause=error;cause!=null;cause=cause.getCause()) if (cause==failure) return true;
        return false;
    }
    void save(java.nio.file.Path directory) throws java.io.IOException {
        if (written==null) throw new AssertionError("No failed PUT to persist");
        var values=new java.util.Properties();
        values.setProperty("namespace",written.bucket()); values.setProperty("key",written.key());
        try (var output=java.nio.file.Files.newOutputStream(directory.resolve("lost-put.properties"))) {
            values.store(output,"Failed Redis PUT coordinates");
        }
    }
    void verifyStored() {
        var spec=written;
        if (spec==null || armed.get()) throw new AssertionError("Real PUT did not reach fault");
        var bytes=store.getBounded(spec.bucket(),spec.key(),null,1024*1024).data();
        if (!DocumentPartCodec.sha256Hex(bytes).equals(spec.sha256Hex()))
            throw new AssertionError("Fault was not injected after matching bytes reached Redis");
    }
}
