package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The corpus builder without any container: the same seed builds the same
 * messages and blob digests, the digest of the default corpus is pinned, every
 * request passes the contract validator, and the mix covers what the console
 * and the catalog tests need.
 */
class RepositoryCorpusTest {

    @Test void spaceTokensCutsOnSingleSpacesAndSkipsEmptyTokens() {
        assertThat(RepositoryCorpus.spaceTokens("a b c")).containsExactly("a", "b", "c");
        assertThat(RepositoryCorpus.spaceTokens("single")).containsExactly("single");
        // Spacing irregularities produce no empty word: consecutive, leading
        // and trailing spaces are skipped rather than becoming tokens.
        assertThat(RepositoryCorpus.spaceTokens("a  b")).containsExactly("a", "b");
        assertThat(RepositoryCorpus.spaceTokens(" a b ")).containsExactly("a", "b");
        assertThat(RepositoryCorpus.spaceTokens("")).isEmpty();
        assertThat(RepositoryCorpus.spaceTokens("   ")).isEmpty();
    }

    @Test void spaceTokensMatchesTheReplacedSplitOnSingleSpacedText() {
        // The oracle uses the split call the fixture no longer uses; for the
        // single-spaced text the corpus generates, the outputs are identical.
        for (String text : new String[] {"a", "a b", "a b c d", "one two three"}) {
            assertThat(RepositoryCorpus.spaceTokens(text))
                    .as("text '%s'", text)
                    .containsExactlyElementsOf(java.util.Arrays.asList(text.split(" ")));
        }
    }

    @Test void sameSeedBuildsTheSameMessagesAndBlobDigests() throws IOException {
        var first = RepositoryCorpus.build();
        var second = RepositoryCorpus.build();
        assertThat(second.messages()).isEqualTo(first.messages());
        assertThat(second.blobDigests()).isEqualTo(first.blobDigests());
        assertThat(second.digest()).isEqualTo(first.digest());
        var streamedFirst = streamed(first);
        var streamedSecond = streamed(second);
        assertThat(streamedSecond.sha256()).isEqualTo(streamedFirst.sha256());
        assertThat(SyntheticBytes.sha256(streamedFirst.body().get())).isEqualTo(streamedFirst.sha256());
        assertThat(SyntheticBytes.sha256(streamedSecond.body().get())).isEqualTo(streamedFirst.sha256());
        assertThat(streamedFirst.length()).isEqualTo(RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES);
    }

    @Test void anotherSeedOrBlobLengthBuildsAnotherCorpus() {
        var base = RepositoryCorpus.build();
        var reseeded = RepositoryCorpus.build(new RepositoryCorpus.Parameters(7L, RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES));
        assertThat(reseeded.digest()).isNotEqualTo(base.digest());
        assertThat(reseeded.messages()).isNotEqualTo(base.messages());
        var longer = RepositoryCorpus.build(new RepositoryCorpus.Parameters(RepositoryCorpus.DEFAULT_SEED, 4096));
        assertThat(longer.digest()).isNotEqualTo(base.digest());
        assertThat(streamed(longer).length()).isEqualTo(4096);
        assertThat(streamed(longer).sha256()).isNotEqualTo(streamed(base).sha256());
        assertThatThrownBy(() -> new RepositoryCorpus.Parameters(1L, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void defaultCorpusDigestIsPinned() throws IOException {
        try (var pinned = getClass().getResourceAsStream("/corpus/default.sha256")) {
            assertThat(pinned).as("corpus/default.sha256 exists").isNotNull();
            var expected = new String(pinned.readAllBytes(), StandardCharsets.UTF_8).trim();
            assertThat(RepositoryCorpus.build().digest())
                    .as("the default corpus changed; update corpus/default.sha256 on purpose")
                    .isEqualTo(expected);
        }
    }

    @Test void everyRequestPassesTheContractValidator() {
        for (var message : RepositoryCorpus.build().messages()) {
            var result = DocumentSaves.VALIDATOR.validate(message);
            assertThat(result.valid()).as("%s: %s", message.getDescriptorForType().getName(), result).isTrue();
        }
    }

    @Test void theMixCoversWhatTheConsoleAndCatalogTestsNeed() {
        var corpus = RepositoryCorpus.build();
        assertThat(corpus.accounts()).extracting(RepositoryCorpus.Account::accountId)
                .containsExactlyElementsOf(RepositoryCorpus.ACCOUNTS);
        for (var account : RepositoryCorpus.ACCOUNTS) assertThat(corpus.documents(account)).isNotEmpty();
        assertThat(corpus.documents(RepositoryCorpus.ACCOUNTS.get(0)).size())
                .isNotEqualTo(corpus.documents(RepositoryCorpus.ACCOUNTS.get(1)).size())
                .isNotEqualTo(corpus.documents(RepositoryCorpus.ACCOUNTS.get(2)).size());

        var parts = new HashSet<String>();
        var owners = new HashSet<String>();
        var drives = new HashSet<String>();
        var datasources = new HashSet<String>();
        var connectors = new HashSet<String>();
        var mimeTypes = new HashSet<String>();
        var languages = new HashSet<String>();
        int withoutAuthor = 0;
        int withoutCategory = 0;
        int multiRevision = 0;
        int tombstoned = 0;
        int streamed = 0;
        int typedLater = 0;
        int multiBlob = 0;
        for (var script : corpus.documents()) {
            drives.add(script.drive());
            if (script.finalRevision() > 1) multiRevision++;
            if (script.tombstoned()) tombstoned++;
            boolean typedAtFirst = false;
            boolean typedAtLast = false;
            for (var step : script.steps()) {
                if (!(step instanceof RepositoryCorpus.Save save)) continue;
                var request = save.request();
                request.getWritesList().forEach(write -> parts.add(DocumentSaves.describe(write.getSelector())));
                owners.add(request.getOwner().hasStep() ? "step:" + request.getOwner().getStep().getModuleId()
                        : "intake:" + request.getOwner().getIntake().getDatasourceId());
                if (save.streamed().isPresent()) streamed++;
                var document = request.getDocument();
                if (document.hasOwnership()) {
                    datasources.add(document.getOwnership().getDatasourceId());
                    if (document.getOwnership().hasConnectorId()) connectors.add(document.getOwnership().getConnectorId());
                }
                if (document.getBlobBag().hasBlobs()) multiBlob++;
                if (save.expectedRevision() == 1) typedAtFirst = document.hasStructuredData();
                if (save.expectedRevision() == script.finalRevision()) typedAtLast = document.hasStructuredData();
                if (request.getWritesList().stream().noneMatch(write -> write.getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE)) continue;
                var metadata = document.getSearchMetadata();
                if (metadata.hasSourceMimeType()) mimeTypes.add(metadata.getSourceMimeType());
                if (metadata.hasLanguage()) languages.add(metadata.getLanguage());
                if (!metadata.hasAuthor()) withoutAuthor++;
                if (!metadata.hasCategory()) withoutCategory++;
            }
            if (!typedAtFirst && typedAtLast) typedLater++;
        }
        assertThat(parts).contains("CORE", "BLOBS", "PARSED").anyMatch(part -> part.startsWith("CHUNKS "))
                .anyMatch(part -> part.startsWith("EMBEDDINGS "));
        assertThat(owners).contains("step:" + RepositoryCorpus.PARSER_MODULE, "step:" + RepositoryCorpus.CHUNKER_MODULE)
                .anyMatch(owner -> owner.startsWith("intake:"));
        assertThat(drives).containsExactlyInAnyOrder(RepositoryCorpus.INTAKE_DRIVE, RepositoryCorpus.PIPELINE_DRIVE);
        assertThat(datasources).hasSizeGreaterThan(1);
        assertThat(connectors).hasSizeGreaterThan(1);
        assertThat(mimeTypes).hasSizeGreaterThan(3);
        assertThat(languages).hasSizeGreaterThan(3);
        assertThat(withoutAuthor).isPositive();
        assertThat(withoutCategory).isPositive();
        assertThat(multiRevision).isGreaterThan(2);
        assertThat(tombstoned).isGreaterThan(1);
        assertThat(streamed).isEqualTo(1);
        assertThat(typedLater).as("a document whose schema bindings change between revisions").isEqualTo(1);
        assertThat(multiBlob).isPositive();
        assertThat(corpus.archives()).hasSize(2);
        assertThat(corpus.archives().getFirst().entries().size()).isNotEqualTo(corpus.archives().getLast().entries().size());
        assertThat(corpus.documents(RepositoryCorpus.ACCOUNTS.get(2))).as("gamma has no archive")
                .noneMatch(script -> corpus.archives().stream()
                        .anyMatch(archive -> archive.archive().getAccountId().equals(script.address().getAccountId())));

        // Two documents of one account share a title, so a title sort has a tie to break.
        var titles = corpus.documents(RepositoryCorpus.ACCOUNTS.get(1)).stream()
                .map(script -> ((RepositoryCorpus.Save) script.steps().getFirst()).request().getDocument().getSearchMetadata().getTitle())
                .toList();
        assertThat(titles).hasSizeGreaterThan(new HashSet<>(titles).size());
    }

    @Test void metadataValuesMatchTheHouseFormats() {
        var seenSlug = false;
        var seenSha = false;
        var seenPathSafe = false;
        var seenRegion = false;
        var seenPostal = false;
        var seenUnicode = false;
        for (var request : firstSaves(RepositoryCorpus.build())) {
            var metadata = request.getDocument().getSearchMetadata();
            if (metadata.hasSourceMimeType()) assertThat(Formats.isMediaType(metadata.getSourceMimeType())).as(metadata.getSourceMimeType()).isTrue();
            if (metadata.hasLanguage()) assertThat(Formats.isLanguageTag(metadata.getLanguage())).as(metadata.getLanguage()).isTrue();
            if (metadata.hasSourceUri()) assertThat(Formats.isUri(metadata.getSourceUri())).as(metadata.getSourceUri()).isTrue();
            for (var entry : metadata.getMetadataMap().entrySet()) {
                switch (entry.getKey()) {
                    case "slug" -> { assertThat(Formats.isSlug(entry.getValue())).isTrue(); seenSlug = true; }
                    case "sha256" -> { assertThat(Formats.isSha256Hex(entry.getValue())).isTrue(); seenSha = true; }
                    case "path_safe_name" -> { assertThat(Formats.isPathSafeName(entry.getValue())).isTrue(); seenPathSafe = true; }
                    case "region" -> { assertThat(Formats.isRegionCode(entry.getValue())).as(entry.getValue()).isTrue(); seenRegion = true; }
                    case "postal_code" -> seenPostal = true;
                    case "mime" -> assertThat(Formats.isMediaType(entry.getValue())).isTrue();
                    case "gtin" -> assertThat(Formats.isGtin(entry.getValue())).isTrue();
                    default -> { }
                }
                if (!StandardCharsets.US_ASCII.newEncoder().canEncode(entry.getKey() + entry.getValue())) seenUnicode = true;
            }
            var bag = request.getDocument().getBlobBag();
            var blobs = bag.hasBlob() ? List.of(bag.getBlob()) : bag.getBlobs().getBlobList();
            for (var blob : blobs) {
                assertThat(Formats.isMediaType(blob.getMimeType())).as(blob.getMimeType()).isTrue();
                if (blob.hasData()) assertThat(blob.getSizeBytes()).isEqualTo(blob.getData().size());
                if (blob.hasChecksum() && blob.hasData()) assertThat(blob.getChecksum()).isEqualTo(SyntheticBytes.sha256(blob.getData().toByteArray()));
            }
        }
        assertThat(seenSlug && seenSha && seenPathSafe && seenRegion && seenPostal && seenUnicode).isTrue();
        for (var archive : RepositoryCorpus.build().archives()) {
            assertThat(Formats.isSlug(archive.archive().getName())).isTrue();
            for (var entry : archive.entries()) {
                for (var rendition : entry.getRenditionsList()) {
                    assertThat(Formats.isPathSafeName(rendition.getRendition().getName())).isTrue();
                    assertThat(Formats.isMediaType(rendition.getRendition().getMediaType())).isTrue();
                }
            }
        }
    }

    @Test void operationIdsAreNameBasedAndUniqueAcrossTheCorpus() {
        var corpus = RepositoryCorpus.build();
        var ids = new HashSet<String>();
        for (var message : corpus.messages()) {
            if (message instanceof SaveDocumentRequest save) {
                assertThat(Formats.isUuid(save.getOperationId())).isTrue();
                assertThat(ids.add(save.getOperationId())).as("operation %s is unique", save.getOperationId()).isTrue();
                if (!save.getDocument().getDocId().isEmpty()) {
                    assertThat(save.getDocument().getDocId()).isEqualTo(save.getTarget().getAddress().getDocId());
                }
            }
        }
        assertThat(RepositoryCorpus.nameUuid(1L, "a", "b")).isEqualTo(RepositoryCorpus.nameUuid(1L, "a", "b"))
                .isNotEqualTo(RepositoryCorpus.nameUuid(2L, "a", "b"));
    }

    @Test void steppedClockHandsOutTheSameSequenceTwice() {
        var start = Instant.parse("2025-03-01T08:00:00Z");
        var step = Duration.ofMinutes(90);
        var first = new SteppedClock(start, step);
        var second = new SteppedClock(start, step);
        var seen = new java.util.ArrayList<Instant>();
        for (int i = 0; i < 5; i++) seen.add(first.instant());
        assertThat(seen).containsExactly(start, start.plus(step), start.plus(step.multipliedBy(2)),
                start.plus(step.multipliedBy(3)), start.plus(step.multipliedBy(4)));
        for (var expected : seen) assertThat(second.instant()).isEqualTo(expected);
        assertThat(first.reads()).isEqualTo(5);
        assertThatThrownBy(() -> new SteppedClock(start, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void semanticPartsBindToTheBodyTheyCut() {
        var save = firstSaves(RepositoryCorpus.build()).stream()
                .filter(request -> request.getTarget().getAddress().getDocId().equals("charter")).findFirst().orElseThrow();
        var corpus = RepositoryCorpus.build();
        var chunk = (RepositoryCorpus.Save) corpus.documents().stream()
                .filter(script -> script.address().getDocId().equals("charter")).findFirst().orElseThrow().steps().get(1);
        var body = save.getDocument().getSearchMetadata().getBody();
        var segmentation = chunk.request().getDocument().getSearchMetadata().getSegmentations(0);
        assertThat(segmentation.getSource().getFieldPath()).isEqualTo("search_metadata.body");
        assertThat(segmentation.getSource().getBinding().getUtf8Length()).isEqualTo(body.getBytes(StandardCharsets.UTF_8).length);
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        for (var segment : segmentation.getSegmentsList()) {
            var slice = new String(bytes, (int) segment.getStart(), (int) (segment.getEnd() - segment.getStart()), StandardCharsets.UTF_8);
            assertThat(slice).isEqualTo(segment.getText());
        }
        var set = chunk.request().getDocument().getSearchMetadata().getEmbeddingSets(0);
        assertThat(set.getVectorsCount()).isEqualTo(segmentation.getSegmentsCount());
        assertThat(set.getSegmentationDigest()).isEqualTo(ai.protomolt.proto.repo.codec.SegmentationDigests.digest(segmentation));
        assertThat(chunk.request().getWritesList()).extracting(write -> write.getSelector().getSelectorCase())
                .contains(PartSelector.SelectorCase.SEGMENTATION_ID, PartSelector.SelectorCase.EMBEDDING_SET);
    }

    private static List<SaveDocumentRequest> firstSaves(RepositoryCorpus corpus) {
        return corpus.documents().stream().map(script -> ((RepositoryCorpus.Save) script.steps().getFirst()).request()).toList();
    }

    private static RepositoryCorpus.StreamedBlob streamed(RepositoryCorpus corpus) {
        return corpus.documents().stream().flatMap(script -> script.steps().stream())
                .filter(step -> step instanceof RepositoryCorpus.Save save && save.streamed().isPresent())
                .map(step -> ((RepositoryCorpus.Save) step).streamed().orElseThrow()).findFirst().orElseThrow();
    }
}
