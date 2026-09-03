package ai.protomolt.proto.descriptors;

/**
 * Preferred public name for {@link DescriptorRegistry}.
 * Same type — use whichever reads better at the call site.
 */
public final class DescriptorCatalog extends DescriptorRegistry {

    public DescriptorCatalog() {
        super();
    }

    public DescriptorCatalog(boolean autoLoad) {
        super(autoLoad);
    }

    public static DescriptorCatalog create() {
        return new DescriptorCatalog();
    }

    public static DescriptorCatalog create(boolean autoLoad) {
        return new DescriptorCatalog(autoLoad);
    }
}
