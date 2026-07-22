package ai.pipestream.proto.projection;

import ai.pipestream.proto.projection.test.Address;
import ai.pipestream.proto.projection.test.BrokenProjection;
import ai.pipestream.proto.projection.test.Case;
import ai.pipestream.proto.projection.test.FlatDoc;
import ai.pipestream.proto.projection.test.Matter;
import ai.pipestream.proto.projection.test.RuntimeErrorProjection;
import ai.pipestream.proto.projection.test.SearchDoc;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                "ai.pipestream.proto.projection.test.v1.Case",
                "ai.pipestream.proto.projection.test.v1.Matter");
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
}
