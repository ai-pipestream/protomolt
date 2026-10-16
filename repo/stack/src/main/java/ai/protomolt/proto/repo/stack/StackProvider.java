package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.v2.StackRecord;
import java.io.IOException;
import java.util.Map;

/**
 * Opens stacks for one storage provider, discovered with {@link java.util.ServiceLoader}
 * through {@link StackProviders}. A provider is chosen by the catalog record's
 * {@code provider} name; there is no fallback provider.
 */
public interface StackProvider {
    /** The provider name a {@link StackRecord} records, for example {@code s3}. */
    String id();

    /**
     * Opens the stack a catalog record names. {@code options} are host settings,
     * credentials included, and never become catalog identity. The returned stack
     * owns what the provider acquired and releases it on close.
     *
     * @param record the stack's catalog record
     * @param options provider settings
     * @return the opened stack, recording {@code record.stack_id} in its references
     * @throws IllegalArgumentException when the record or options are invalid
     * @throws IOException when the provider cannot be opened
     */
    Stack open(StackRecord record, Map<String, String> options) throws IOException;
}
