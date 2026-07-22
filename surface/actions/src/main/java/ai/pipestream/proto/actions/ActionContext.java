package ai.pipestream.proto.actions;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

/**
 * Shared machinery handed to every {@link ProtoAction}: a {@link DescriptorRegistry} for
 * resolving {@code {"type": "fully.qualified.Name"}} schema references, a Jackson
 * {@link ObjectMapper} for building result documents, and a {@link ProtobufJsonTranscoder}
 * bound to the registry for JSON &lt;-&gt; protobuf transcoding.
 *
 * <p>Actions that receive inline sources or serialized descriptor sets compile/link them per
 * call and never touch the registry.</p>
 */
public final class ActionContext {

    private final DescriptorRegistry registry;
    private final ObjectMapper objectMapper;
    private final ProtobufJsonTranscoder transcoder;

    private ActionContext(Builder builder) {
        this.registry = builder.registry != null ? builder.registry : DescriptorRegistry.create();
        this.objectMapper = builder.objectMapper != null ? builder.objectMapper : new ObjectMapper();
        this.transcoder = new ProtobufJsonTranscoder(registry);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A context with a fresh registry (well-known types only) and a default mapper. */
    public static ActionContext create() {
        return builder().build();
    }

    public DescriptorRegistry registry() {
        return registry;
    }

    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    public ProtobufJsonTranscoder transcoder() {
        return transcoder;
    }

    public static final class Builder {

        private DescriptorRegistry registry;
        private ObjectMapper objectMapper;

        private Builder() {
        }

        public Builder registry(DescriptorRegistry registry) {
            this.registry = Objects.requireNonNull(registry, "registry");
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
            return this;
        }

        public ActionContext build() {
            return new ActionContext(this);
        }
    }
}
