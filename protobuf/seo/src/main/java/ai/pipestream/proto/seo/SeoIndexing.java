package ai.pipestream.proto.seo;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingHintSource;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.index.spi.InferringIndexingHintSource;
import ai.pipestream.proto.index.spi.ProtoOptionsIndexingHintSource;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Optional;

/**
 * Builds {@link IndexingPlan}s for the search-metadata standard from the
 * {@code (ai.pipestream.proto.index.hints.v1.index)} annotations the {@code seo.v1}
 * descriptors carry.
 *
 * <p>The hint chain is: proto-option hints first (the standard's annotations are
 * authoritative), then a container-expanding source for the standard's own message-typed
 * fields, then descriptor inference for everything left. The middle link is what turns
 * {@code SearchStandard}'s nested shape into dotted leaf paths ({@code dublin_core.title},
 * {@code product.offers.price_currency}, ...): without it, an unannotated singular message
 * field infers as one opaque {@code OBJECT} entry and its annotated leaves never reach the
 * plan. Only messages declared in the {@code ai.pipestream.proto.seo.v1} package expand
 * this way — well-known types ({@code google.protobuf.Timestamp},
 * {@code google.protobuf.Duration}) keep their inferred single-entry shapes unless a field
 * annotation says otherwise.
 */
public final class SeoIndexing {

    /** The proto package whose message-typed fields expand into dotted plan paths. */
    private static final String SEO_PACKAGE = "ai.pipestream.proto.seo.v1";

    private SeoIndexing() {
    }

    /**
     * The indexing plan the {@code seo.v1} annotations declare for {@code descriptor},
     * with the standard's container messages expanded into dotted leaf paths.
     *
     * @param descriptor the message type to plan, typically
     *        {@code SearchStandard.getDescriptor()}
     * @return the plan whose entries carry the annotated field kinds, analyzers, and
     *         docValues flags
     */
    public static IndexingPlan planFor(Descriptor descriptor) {
        return factory().create(descriptor);
    }

    /**
     * The plan factory {@link #planFor(Descriptor)} uses, exposed for consumers that need
     * the factory itself (e.g. to read the resolved hint chain via
     * {@link IndexingPlanFactory#hints()}).
     */
    public static IndexingPlanFactory factory() {
        IndexingHintSource chain = new ProtoOptionsIndexingHintSource()
                .orElse(SeoIndexing::expandSeoContainers)
                .orElse(new InferringIndexingHintSource());
        return new IndexingPlanFactory(chain);
    }

    /**
     * Expands message-typed fields whose type lives in the {@code seo.v1} package. The
     * plan factory expands a message field whenever its hint kind is expandable; the
     * container's own hint never lands in the plan (each leaf resolves its own), so the
     * kind here is purely the walk directive.
     */
    private static Optional<ResolvedFieldHint> expandSeoContainers(FieldDescriptor field) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && SEO_PACKAGE.equals(field.getMessageType().getFile().getPackage())) {
            return Optional.of(ResolvedFieldHint.of(IndexFieldKind.TEXT));
        }
        return Optional.empty();
    }
}
