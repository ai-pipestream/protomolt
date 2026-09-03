package ai.protomolt.proto.descriptors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DescriptorCatalogTest {
    @Test void createsDefaultCatalog() {
        assertNotNull(DescriptorCatalog.create().findDescriptor("google.protobuf.Struct"));
    }
    @Test void createsAutoLoadingCatalog() {
        assertNotNull(DescriptorCatalog.create(true).findDescriptor("google.protobuf.Any"));
    }
}
