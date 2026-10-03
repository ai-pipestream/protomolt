package ai.protomolt.proto.repo.blob.spi;

import java.util.Map;

/** Trusted installed factory. Construction and identity lookup must perform no I/O. */
public interface BlobStoreProvider {
    String id();

    /** Validate all options before acquisition; clean up acquired resources on failure. */
    OpenedBlobStore open(Map<String, String> options);
}
