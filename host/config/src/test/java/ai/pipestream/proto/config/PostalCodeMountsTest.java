package ai.pipestream.proto.config;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.types.PostalCodePack;
import ai.pipestream.proto.types.RegionPostalMasks;
import ai.pipestream.proto.validate.spi.PostalCodeCatalog;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The postal-code pack mount contract: one subject, the source's version as
 * the mounted version, atomic swaps, a duplicated region refused without
 * unmounting the previous pack, and a document failing the pack's own
 * declared rules never mounting.
 */
class PostalCodeMountsTest {

    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();

        @Override
        public Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }
    }

    private static RegionPostalMasks region(String code, String... masks) {
        RegionPostalMasks.Builder builder = RegionPostalMasks.newBuilder()
                .setRegionCode(code);
        for (String mask : masks) {
            builder.addMasks(mask);
        }
        return builder.build();
    }

    private static PostalCodePack pack(RegionPostalMasks... regions) {
        PostalCodePack.Builder builder = PostalCodePack.newBuilder();
        for (RegionPostalMasks entry : regions) {
            builder.addRegions(entry);
        }
        return builder.build();
    }

    @Test
    void thePackMountsAndSwapsAtomically() {
        FakeSource source = new FakeSource();
        source.documents.put(PostalCodeMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", pack(region("US", "NNNNN")).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            PostalCodeMounts mounts = PostalCodeMounts.follow(config);
            assertThat(mounts.region("US")).as("unmounted until a document applies")
                    .isEmpty();

            config.refresh();
            PostalCodeCatalog.Mounted mounted = mounts.region("US").orElseThrow();
            assertThat(mounted.version()).isEqualTo("v1");
            assertThat(mounted.masks()).containsExactly("NNNNN");

            source.documents.put(PostalCodeMounts.SUBJECT, new ConfigSource.Fetched(
                    "v2", pack(region("US", "NNNNN", "NNNNN-NNNN"),
                            region("GB", "AN NAA")).toByteArray()));
            config.refresh();
            assertThat(mounts.region("US").orElseThrow().masks())
                    .containsExactly("NNNNN", "NNNNN-NNNN");
            assertThat(mounts.region("GB").orElseThrow().version()).isEqualTo("v2");
        }
    }

    @Test
    void aDuplicatedRegionRefusesWithoutUnmountingThePreviousPack() {
        FakeSource source = new FakeSource();
        source.documents.put(PostalCodeMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", pack(region("US", "NNNNN")).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            PostalCodeMounts mounts = PostalCodeMounts.follow(config);
            config.refresh();
            assertThat(mounts.region("US")).isPresent();

            source.documents.put(PostalCodeMounts.SUBJECT, new ConfigSource.Fetched(
                    "v2", pack(region("US", "NNNNN"), region("US", "NNNNN-NNNN"))
                            .toByteArray()));
            config.refresh();
            // The listener refused the duplicate; the previous pack serves.
            assertThat(mounts.region("US").orElseThrow().version()).isEqualTo("v1");
        }
    }

    @Test
    void aPackFailingItsOwnRulesNeverMounts() {
        FakeSource source = new FakeSource();
        // An empty pack violates regions min_items.
        source.documents.put(PostalCodeMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", PostalCodePack.getDefaultInstance().toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            PostalCodeMounts mounts = PostalCodeMounts.follow(config);
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.refused()).isNotEmpty();
            assertThat(mounts.region("US")).isEmpty();
        }
    }
}
