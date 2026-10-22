package ai.protomolt.proto.projection;

import ai.protomolt.proto.projection.test.Address;
import ai.protomolt.proto.projection.test.ArchiveEntryView;
import ai.protomolt.proto.projection.test.BadEnumLiteral;
import ai.protomolt.proto.projection.test.Blob;
import ai.protomolt.proto.projection.test.BlobBag;
import ai.protomolt.proto.projection.test.BlobChecksumType;
import ai.protomolt.proto.projection.test.Blobs;
import ai.protomolt.proto.projection.test.BrokenChildParent;
import ai.protomolt.proto.projection.test.BrokenProjection;
import ai.protomolt.proto.projection.test.Case;
import ai.protomolt.proto.projection.test.FlatDoc;
import ai.protomolt.proto.projection.test.Loop;
import ai.protomolt.proto.projection.test.Matter;
import ai.protomolt.proto.projection.test.MisfedEntryView;
import ai.protomolt.proto.projection.test.MutualA;
import ai.protomolt.proto.projection.test.Note;
import ai.protomolt.proto.projection.test.PartState;
import ai.protomolt.proto.projection.test.Rendition;
import ai.protomolt.proto.projection.test.RenditionListView;
import ai.protomolt.proto.projection.test.RenditionState;
import ai.protomolt.proto.projection.test.RenditionStatus;
import ai.protomolt.proto.projection.test.RuntimeErrorProjection;
import ai.protomolt.proto.projection.test.ScalarZoo;
import ai.protomolt.proto.projection.test.SearchDoc;
import ai.protomolt.proto.projection.test.StoredDocument;
import ai.protomolt.proto.projection.test.StoredPart;
import ai.protomolt.proto.projection.test.TreeNode;
import ai.protomolt.proto.projection.test.TreeView;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class MessageProjectionTest {

    private final SourceResolver sources = SourceResolver.of(
            Case.getDescriptor(), Matter.getDescriptor());

    private MessageProjection searchDocProjection() {
        return MessageProjection.forTarget(SearchDoc.getDescriptor(), sources).orElseThrow();
    }

    private static Case aCase() {
        return Case.newBuilder()
                .setCaseId("24-cv-00117")
                .setStyleOfCause("State v. Example")
                .setAddress(Address.newBuilder().setState("mn"))
                .addParties("State").addParties("Example")
                .setFiledEpoch(1752900000L)
                .build();
    }

    private static Matter aMatter() {
        return Matter.newBuilder()
                .setMatterNo("M-8891")
                .setCaption("In re Example Billing")
                .setJurisdiction("8th Circuit")
                .build();
    }

    @Test
    void projectsEveryRuleKindFromThePrimarySource() throws Exception {
        DynamicMessage out = searchDocProjection().project(aCase());

        SearchDoc doc = SearchDoc.parseFrom(out.toByteString(), com.google.protobuf.ExtensionRegistry.getEmptyRegistry());
        assertThat(doc.getDocId()).isEqualTo("24-cv-00117");
        assertThat(doc.getTitle()).isEqualTo("State v. Example");
        assertThat(doc.getRegion()).isEqualTo("MN");
        assertThat(doc.getSourceSystem()).isEqualTo("filing-index");
        assertThat(doc.getPartiesList()).containsExactly("State", "Example");
        assertThat(doc.getFiledEpoch()).isEqualTo(1752900000L);
        assertThat(doc.getFileRank()).isEqualTo(1.5);
    }

    @Test
    void fallsThroughCandidatePathsForTheSecondSourceShape() throws Exception {
        DynamicMessage out = searchDocProjection().project(aMatter());

        SearchDoc doc = SearchDoc.parseFrom(out.toByteString(), com.google.protobuf.ExtensionRegistry.getEmptyRegistry());
        assertThat(doc.getDocId()).isEqualTo("M-8891");
        assertThat(doc.getTitle()).isEqualTo("In re Example Billing");
        assertThat(doc.getJurisdiction()).isEqualTo("8th Circuit");
        // Case-only CEL is absent for Matter, and vice versa.
        assertThat(doc.getRegion()).isEmpty();
        assertThat(doc.getPartiesList()).isEmpty();
        assertThat(doc.getSourceSystem()).isEqualTo("filing-index");
    }

    @Test
    void leavesFieldsAbsentWhenNoCandidateApplies() {
        DynamicMessage out = searchDocProjection().project(aCase());
        // jurisdiction resolves only on Matter.
        assertThat(out.getField(SearchDoc.getDescriptor().findFieldByName("jurisdiction"))).isEqualTo("");
    }

    @Test
    void neverPopulatesFieldsWithoutProvenance() {
        DynamicMessage out = searchDocProjection().project(aCase());
        assertThat(out.getField(SearchDoc.getDescriptor().findFieldByName("unmapped"))).isEqualTo("");
    }

    @Test
    void rejectsSourcesOutsideTheDeclaredSet() {
        assertThatThrownBy(() -> searchDocProjection().project(Address.newBuilder().setState("mn").build()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("not a declared source");
    }

    @Test
    void isEmptyForTargetsWithoutTheSourcesOption() {
        assertThat(MessageProjection.forTarget(Address.getDescriptor(), sources)).isEmpty();
    }

    @Test
    void failsFastWhenCelCompilesForNoDeclaredSource() {
        assertThatThrownBy(() -> MessageProjection.forTarget(BrokenProjection.getDescriptor(), sources))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("compiles against no declared source");
    }

    @Test
    void propagatesCelRuntimeFailures() {
        MessageProjection projection =
                MessageProjection.forTarget(RuntimeErrorProjection.getDescriptor(), sources).orElseThrow();
        assertThatThrownBy(() -> projection.project(aCase()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ratio");
    }

    @Test
    void buildsWithoutAnyResolvableSourceForEagerValidation() {
        Optional<MessageProjection> projection =
                MessageProjection.forTarget(SearchDoc.getDescriptor(), SourceResolver.of());
        assertThat(projection).isPresent();
        // Runtime resolution comes from the message itself, so projection still works.
        assertThat(projection.orElseThrow().project(aCase()).getField(
                SearchDoc.getDescriptor().findFieldByName("doc_id"))).isEqualTo("24-cv-00117");
    }

    @Test
    void reportsDeclaredSourcesAndSupport() {
        MessageProjection projection = searchDocProjection();
        assertThat(projection.targetType()).isEqualTo(SearchDoc.getDescriptor());
        assertThat(projection.declaredSources()).containsExactly(
                "ai.protomolt.proto.projection.test.v1.Case",
                "ai.protomolt.proto.projection.test.v1.Matter");
        assertThat(projection.supports(Case.getDescriptor())).isTrue();
        assertThat(projection.supports(Address.getDescriptor())).isFalse();
    }

    @Test
    void emitsTargetMaskCoveringEveryMappedField() {
        assertThat(searchDocProjection().targetMask().getPathsList())
                .containsExactly("doc_id", "title", "region", "jurisdiction",
                        "source_system", "parties", "filed_epoch", "file_rank");
    }

    @Test
    void emitsPerSourceReadMasksFilteredToResolvablePaths() {
        MessageProjection projection = searchDocProjection();

        var caseMask = projection.sourceMask(Case.getDescriptor());
        assertThat(caseMask.fieldMask().getPathsList())
                .containsExactly("case_id", "style_of_cause", "parties", "filed_epoch");
        // The Case-only CEL rule compiles here, so the mask is a lower bound.
        assertThat(caseMask.complete()).isFalse();

        var matterMask = projection.sourceMask(Matter.getDescriptor());
        assertThat(matterMask.fieldMask().getPathsList())
                .containsExactly("matter_no", "caption", "jurisdiction");
        // The CEL rule does not compile against Matter, so the mask is exact.
        assertThat(matterMask.complete()).isTrue();
    }

    @Test
    void pathsOnlyProjectionHasCompleteMasksForEverySource() {
        MessageProjection projection =
                MessageProjection.forTarget(FlatDoc.getDescriptor(), sources).orElseThrow();

        var caseMask = projection.sourceMask(Case.getDescriptor());
        assertThat(caseMask.fieldMask().getPathsList()).containsExactly("case_id");
        assertThat(caseMask.complete()).isTrue();

        var matterMask = projection.sourceMask(Matter.getDescriptor());
        assertThat(matterMask.fieldMask().getPathsList()).containsExactly("matter_no");
        assertThat(matterMask.complete()).isTrue();
    }

    /** A standalone descriptor for {@code dup.Thing}; two calls give two distinct descriptors. */
    private static com.google.protobuf.Descriptors.Descriptor thing() throws Exception {
        com.google.protobuf.DescriptorProtos.FileDescriptorProto file =
                com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("dup/thing.proto")
                        .setSyntax("proto3")
                        .setPackage("dup")
                        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto
                                .newBuilder().setName("Thing"))
                        .build();
        return com.google.protobuf.Descriptors.FileDescriptor
                .buildFrom(file, new com.google.protobuf.Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Thing");
    }

    /**
     * Duplicate names used to surface as the map collector's {@code IllegalStateException},
     * which names the colliding values but not what the caller did wrong.
     */
    @Test
    void twoDifferentDescriptorsForOneNameAreRejected() throws Exception {
        assertThatThrownBy(() -> SourceResolver.of(java.util.List.of(thing(), thing())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dup.Thing")
                .hasMessageContaining("must be unique by name");
    }

    @Test
    void theSameDescriptorListedTwiceIsAccepted() throws Exception {
        com.google.protobuf.Descriptors.Descriptor thing = thing();
        assertThat(SourceResolver.of(java.util.List.of(thing, thing)).resolve("dup.Thing"))
                .contains(thing);
    }

    @Test
    void sourceMaskRejectsUndeclaredSources() {
        assertThatThrownBy(() -> searchDocProjection().sourceMask(Address.getDescriptor()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("not a declared source");
    }
    @Test
    void evolvedSourceWithSameNameUsesItsOwnCelDescriptor() throws Exception {
        var original = ai.protomolt.proto.projection.test.EvolvingSource.getDescriptor();
        var file = original.getFile().toProto().toBuilder();
        for (int n = 0; n < file.getMessageTypeCount(); n++) {
            if (file.getMessageType(n).getName().equals("EvolvingSource")) {
                file.getMessageTypeBuilder(n).getFieldBuilder(0)
                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            }
        }
        var evolved = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file.build(),
                original.getFile().getDependencies().toArray(com.google.protobuf.Descriptors.FileDescriptor[]::new))
                .findMessageTypeByName("EvolvingSource");
        var projection = MessageProjection.forTarget(ai.protomolt.proto.projection.test.EvolvingProjection.getDescriptor(),
                SourceResolver.of(original)).orElseThrow();
        var text = ai.protomolt.proto.projection.test.EvolvingSource.newBuilder().setValue("before").build();
        var number = DynamicMessage.newBuilder(evolved).setField(evolved.findFieldByNumber(1), 42L).build();
        var rendered = ai.protomolt.proto.projection.test.EvolvingProjection.getDescriptor().findFieldByName("rendered");
        assertThat(projection.project(text).getField(rendered)).isEqualTo("before");
        assertThat(projection.project(number).getField(rendered)).isEqualTo("42");
        assertThat(projection.project(text).getField(rendered)).isEqualTo("before");
    }

    @Test void dynamicDescriptorChurnEvictsWithoutLosingProjectionBehavior() throws Exception {
        var original = ai.protomolt.proto.projection.test.EvolvingSource.getDescriptor();
        var target = ai.protomolt.proto.projection.test.EvolvingProjection.getDescriptor();
        var projection = MessageProjection.forTarget(target, SourceResolver.of(original)).orElseThrow();
        var rendered = target.findFieldByName("rendered");
        var first = ai.protomolt.proto.projection.test.EvolvingSource.newBuilder().setValue("first").build();
        assertThat(projection.project(first).getField(rendered)).isEqualTo("first");
        for (int n = 0; n < MessageProjection.MAX_CACHED_EVALUATORS + 10; n++) {
            var copy = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(original.getFile().toProto(),
                    original.getFile().getDependencies().toArray(com.google.protobuf.Descriptors.FileDescriptor[]::new))
                    .findMessageTypeByName("EvolvingSource");
            var message = DynamicMessage.newBuilder(copy).setField(copy.findFieldByNumber(1), "value-" + n).build();
            assertThat(projection.project(message).getField(rendered)).isEqualTo("value-" + n);
            assertThat(projection.cachedEvaluatorCount()).isLessThanOrEqualTo(MessageProjection.MAX_CACHED_EVALUATORS);
        }
        assertThat(projection.project(first).getField(rendered)).isEqualTo("first");
        assertThat(projection.cachedEvaluatorCount()).isEqualTo(MessageProjection.MAX_CACHED_EVALUATORS);
    }

    private static final SourceResolver STORED = SourceResolver.of(
            StoredDocument.getDescriptor(), Blob.getDescriptor());

    private static MessageProjection archiveView() {
        return MessageProjection.forTarget(ArchiveEntryView.getDescriptor(), STORED).orElseThrow();
    }

    private static Blob original() {
        return Blob.newBuilder()
                .setBlobId("blob-1")
                .setStorageKey("objects/blob-1")
                .setMimeType("application/pdf")
                .setFilename("brief.pdf")
                .setSizeBytes(4096)
                .setChecksum("abc123")
                .setChecksumType(BlobChecksumType.BLOB_CHECKSUM_TYPE_SHA256)
                .build();
    }

    /** No file name, no media type, no checksum, inline content: every fallback fires. */
    private static Blob markdown() {
        return Blob.newBuilder()
                .setBlobId("blob-2")
                .setData(ByteString.copyFromUtf8("# Brief"))
                .setSizeBytes(7)
                .build();
    }

    private static StoredDocument withBlobs(Blob... blobs) {
        return StoredDocument.newBuilder()
                .setDocId("doc-1")
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder().addAllBlob(List.of(blobs))))
                .build();
    }

    private static ArchiveEntryView parse(DynamicMessage out) throws Exception {
        return ArchiveEntryView.parseFrom(out.toByteString(), ExtensionRegistry.getEmptyRegistry());
    }

    @Test
    void projectsARepeatedListOfAnotherMessageTypeElementByElement() throws Exception {
        ArchiveEntryView view = parse(archiveView().project(withBlobs(original(), markdown())));

        assertThat(view.getEntryId()).isEqualTo("doc-1");
        assertThat(view.getRenditionsList()).containsExactly(
                Rendition.newBuilder()
                        .setName("brief.pdf")
                        .setBlobId("blob-1")
                        .setMimeType("application/pdf")
                        .setSizeBytes(4096)
                        .setChecksum("sha256:abc123")
                        .build(),
                Rendition.newBuilder()
                        .setName("blob-2")
                        .setBlobId("blob-2")
                        .setMimeType("application/octet-stream")
                        .setSizeBytes(7)
                        .setInline(true)
                        .build());
        // The bag held the list arm, so the singular arm stays unset.
        assertThat(view.hasPrimary()).isFalse();
    }

    @Test
    void projectsASingularMessageOfAnotherType() throws Exception {
        StoredDocument document = StoredDocument.newBuilder()
                .setDocId("doc-2")
                .setBlobBag(BlobBag.newBuilder().setBlob(markdown()))
                .build();

        ArchiveEntryView view = parse(archiveView().project(document));

        assertThat(view.getPrimary()).isEqualTo(Rendition.newBuilder()
                .setName("blob-2")
                .setBlobId("blob-2")
                .setMimeType("application/octet-stream")
                .setSizeBytes(7)
                .setInline(true)
                .build());
        assertThat(view.getRenditionsList()).isEmpty();
    }

    @Test
    void projectsACelBuiltListElementByElement() throws Exception {
        ArchiveEntryView view = parse(archiveView().project(withBlobs(original(), markdown())));

        // The filter keeps only the 4096-byte blob, and it arrives as a rendition.
        assertThat(view.getLargeRenditionsList())
                .extracting(Rendition::getName, Rendition::getChecksum)
                .containsExactly(tuple("brief.pdf", "sha256:abc123"));
    }

    @Test
    void copiesMessagesThatAlreadyHaveTheFieldTypeUnchanged() throws Exception {
        Rendition carried = Rendition.newBuilder()
                .setName("kept as is")
                .setBlobId("blob-9")
                .build();
        StoredDocument document = withBlobs(original()).toBuilder()
                .addRenditions(carried)
                .setCover(markdown())
                .build();

        ArchiveEntryView view = parse(archiveView().project(document));

        // A rendition fed to a rendition field is not projected again: no
        // fallback media type is added, no name is recomputed.
        assertThat(view.getCarriedList()).containsExactly(carried);
        // Blob fields declare no sources, so blobs copy into them unchanged.
        assertThat(view.getRawBlobsList()).containsExactly(original());
        assertThat(view.getCover()).isEqualTo(markdown());
    }

    @Test
    void refusesAMessageThatIsNeitherTheFieldTypeNorADeclaredChildSource() {
        MessageProjection projection =
                MessageProjection.forTarget(MisfedEntryView.getDescriptor(), STORED).orElseThrow();
        StoredDocument document = StoredDocument.newBuilder()
                .setDocId("doc-3")
                .addNotes(Note.newBuilder().setText("not a blob"))
                .build();

        assertThatThrownBy(() -> projection.project(document))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.Note")
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.MisfedEntryView.renditions")
                .hasMessageContaining("declared: ai.protomolt.proto.projection.test.v1.Blob");
    }

    @Test
    void validatesChildCelWhenTheParentIsBuilt() {
        assertThatThrownBy(() -> MessageProjection.forTarget(BrokenChildParent.getDescriptor(), STORED))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("BrokenRendition.boom")
                .hasMessageContaining("compiles against no declared source");
    }

    @Test
    void compilesAndProjectsARecursiveTarget() throws Exception {
        MessageProjection projection = MessageProjection.forTarget(
                TreeView.getDescriptor(), SourceResolver.of(TreeNode.getDescriptor())).orElseThrow();
        TreeNode tree = TreeNode.newBuilder()
                .setLabel("root")
                .addChildren(TreeNode.newBuilder().setLabel("left")
                        .addChildren(TreeNode.newBuilder().setLabel("leaf")))
                .addChildren(TreeNode.newBuilder().setLabel("right"))
                .build();

        TreeView view = TreeView.parseFrom(projection.project(tree).toByteString(),
                ExtensionRegistry.getEmptyRegistry());

        assertThat(view).isEqualTo(TreeView.newBuilder()
                .setLabel("ROOT")
                .addChildren(TreeView.newBuilder().setLabel("LEFT")
                        .addChildren(TreeView.newBuilder().setLabel("LEAF")))
                .addChildren(TreeView.newBuilder().setLabel("RIGHT"))
                .build());
    }

    @Test
    void keepsAChildFeedingPathWholeInTheParentSourceMask() {
        MessageProjection projection =
                MessageProjection.forTarget(RenditionListView.getDescriptor(), STORED).orElseThrow();

        var mask = projection.sourceMask(StoredDocument.getDescriptor());
        assertThat(mask.fieldMask().getPathsList()).containsExactly("blob_bag.blobs.blob");
        // The path names every field below it, so the child's CEL reads are
        // covered and the parent mask stays exact.
        assertThat(mask.complete()).isTrue();

        // The child's own mask, over its own source, is where its CEL shows.
        var childMask = MessageProjection.forTarget(Rendition.getDescriptor(), STORED)
                .orElseThrow().sourceMask(Blob.getDescriptor());
        assertThat(childMask.fieldMask().getPathsList())
                .containsExactly("blob_id", "mime_type", "size_bytes");
        assertThat(childMask.complete()).isFalse();
    }

    @Test
    void archiveViewMaskListsEveryResolvingPathAndIsALowerBoundUnderCel() {
        var mask = archiveView().sourceMask(StoredDocument.getDescriptor());
        assertThat(mask.fieldMask().getPathsList()).containsExactly(
                "doc_id", "blob_bag.blobs.blob", "blob_bag.blob", "renditions", "cover");
        assertThat(mask.complete()).isFalse();
    }

    @Test
    void resolvesWalksDottedPathsWithoutRegex() {
        var zoo = ScalarZoo.getDescriptor();
        assertThat(MessageProjection.resolves(zoo, "name")).isTrue();
        assertThat(MessageProjection.resolves(zoo, "nested.leaf")).isTrue();
        assertThat(MessageProjection.resolves(zoo, "  nested.leaf  ")).isTrue();
        // Anything below a map counts, because map keys are dynamic.
        assertThat(MessageProjection.resolves(zoo, "counts")).isTrue();
        assertThat(MessageProjection.resolves(zoo, "counts.any_key")).isTrue();
        assertThat(MessageProjection.resolves(StoredDocument.getDescriptor(), "blob_bag.blobs.blob.blob_id"))
                .isTrue();
    }

    @Test
    void resolvesRejectsEmptySegmentsUnknownFieldsAndPathsBelowScalars() {
        var zoo = ScalarZoo.getDescriptor();
        assertThat(MessageProjection.resolves(zoo, "")).isFalse();
        assertThat(MessageProjection.resolves(zoo, "   ")).isFalse();
        assertThat(MessageProjection.resolves(zoo, ".")).isFalse();
        assertThat(MessageProjection.resolves(zoo, ".name")).isFalse();
        // A trailing dot is an empty segment, not something to ignore.
        assertThat(MessageProjection.resolves(zoo, "name.")).isFalse();
        assertThat(MessageProjection.resolves(zoo, "nested.leaf.")).isFalse();
        assertThat(MessageProjection.resolves(zoo, "nested..leaf")).isFalse();
        assertThat(MessageProjection.resolves(zoo, "nested.missing")).isFalse();
        assertThat(MessageProjection.resolves(zoo, "no_such_field")).isFalse();
        // A scalar has no fields below it.
        assertThat(MessageProjection.resolves(zoo, "name.length")).isFalse();
        // Segments are not trimmed individually.
        assertThat(MessageProjection.resolves(zoo, "nested . leaf")).isFalse();
    }

    private static final SourceResolver EMPTY = SourceResolver.of(Empty.getDescriptor());

    @Test
    void refusesAChildThatProjectsTheSameSourceIntoItself() {
        MessageProjection projection = MessageProjection.forTarget(Loop.getDescriptor(), EMPTY).orElseThrow();

        assertThatThrownBy(() -> projection.project(Empty.getDefaultInstance()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.Loop re-enters itself")
                .hasMessageContaining("google.protobuf.Empty");
    }

    @Test
    void refusesTwoTargetsThatHandTheSameSourceBackAndForth() {
        MessageProjection projection = MessageProjection.forTarget(MutualA.getDescriptor(), EMPTY).orElseThrow();

        // A opens with Empty, B opens with Empty, then A would reopen with Empty.
        assertThatThrownBy(() -> projection.project(Empty.getDefaultInstance()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.MutualA re-enters itself");
    }

    @Test
    void projectsEqualSiblingsBecauseOnlyOpenProjectionsCount() throws Exception {
        MessageProjection projection = MessageProjection.forTarget(
                TreeView.getDescriptor(), SourceResolver.of(TreeNode.getDescriptor())).orElseThrow();
        TreeNode twin = TreeNode.newBuilder().setLabel("twin").build();
        TreeNode tree = TreeNode.newBuilder().setLabel("root").addChildren(twin).addChildren(twin).build();

        TreeView view = TreeView.parseFrom(projection.project(tree).toByteString(),
                ExtensionRegistry.getEmptyRegistry());

        assertThat(view.getChildrenList()).extracting(TreeView::getLabel).containsExactly("TWIN", "TWIN");
    }

    private static TreeNode chain(int depth) {
        TreeNode node = TreeNode.newBuilder().setLabel("n" + depth).build();
        for (int n = depth - 1; n >= 1; n--) {
            node = TreeNode.newBuilder().setLabel("n" + n).addChildren(node).build();
        }
        return node;
    }

    @Test
    void boundsNestingDepthForChainsThatNeverRepeat() {
        MessageProjection projection = MessageProjection.forTarget(
                TreeView.getDescriptor(), SourceResolver.of(TreeNode.getDescriptor())).orElseThrow();

        // Every level is a different value, so only the depth bound stops it.
        assertThat(projection.project(chain(MessageProjection.MAX_NESTING_DEPTH))).isNotNull();
        assertThatThrownBy(() -> projection.project(chain(MessageProjection.MAX_NESTING_DEPTH + 1)))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("more than " + MessageProjection.MAX_NESTING_DEPTH + " nested projections");
    }

    @Test
    void convertsEnumsOfAnotherTypeAndNumericLiteralsByNumber() throws Exception {
        MessageProjection projection = MessageProjection.forTarget(
                RenditionStatus.getDescriptor(), SourceResolver.of(StoredPart.getDescriptor())).orElseThrow();

        RenditionStatus status = RenditionStatus.parseFrom(
                projection.project(StoredPart.newBuilder().setState(PartState.PART_STATE_CARRIED).build())
                        .toByteString(),
                ExtensionRegistry.getEmptyRegistry());

        assertThat(status.getState()).isEqualTo(RenditionState.RENDITION_STATE_INHERITED);
        // maybe_state is absent, so default_from's number_value 1 applies.
        assertThat(status.getStateOrStored()).isEqualTo(RenditionState.RENDITION_STATE_STORED);
        assertThat(status.getViaCel()).isEqualTo(RenditionState.RENDITION_STATE_INHERITED);
    }

    @Test
    void refusesANumericLiteralThatIsNoValueOfTheEnum() {
        MessageProjection projection = MessageProjection.forTarget(
                BadEnumLiteral.getDescriptor(), SourceResolver.of(StoredPart.getDescriptor())).orElseThrow();

        assertThatThrownBy(() -> projection.project(StoredPart.getDefaultInstance()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.BadEnumLiteral.state");
    }
}
