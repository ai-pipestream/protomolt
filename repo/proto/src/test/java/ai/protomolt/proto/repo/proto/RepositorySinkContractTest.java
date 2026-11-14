package ai.protomolt.proto.repo.proto;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.sink.v1.ArchiveRenditionSource;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.DocumentPartSource;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.ProcessingStateSource;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenanceEntry;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.SaveAsCapability;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.sink.v1.SourcePartDigest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Contract validation, descriptor closure, and JSON Schema tests for
 * {@code ai.protomolt.proto.repo.sink.v1.repository_sink.proto} (#818).
 */
class RepositorySinkContractTest {
    private static final String SHA = "a".repeat(64);
    private static final Timestamp COMMITTED_AT = Timestamp.newBuilder().setSeconds(1_730_000_000L).build();
    private static final EntryAddress ENTRY = EntryAddress.newBuilder()
            .setAccountId("acct-1")
            .setArchive("records")
            .setEntryId("case-42")
            .build();
    private static final NodeAddress DOCUMENT = NodeAddress.newBuilder()
            .setAccountId("acct-1")
            .setGraphId("pipeline:acct-1")
            .setGraphAddressId("step-a")
            .setDocId("doc-42")
            .build();
    private static final RenditionDescriptor RENDITION = RenditionDescriptor.newBuilder()
            .setName("derived")
            .setSubKey("parquet")
            .setMediaType("application/vnd.apache.parquet")
            .build();
    private record NamedValidator(String name, ProtoValidator validator) {}

    private static final List<NamedValidator> BOTH_ENGINES = List.of(
            new NamedValidator("ProtomoltRuleSource", ProtoValidator.create(List.of(new ProtomoltRuleSource()))),
            new NamedValidator("ProtovalidateRuleSource", ProtoValidator.create(List.of(new ProtovalidateRuleSource()))));

    @Test void processingAndRepositorySourcesEnforceRequiredBoundsAcrossBothEngines() throws Exception {
        var digest = SourcePartDigest.newBuilder()
                .setIdentity("step:extract")
                .setSha256(SHA)
                .setSizeBytes(128)
                .build();
        checkBoth(digest, true);
        checkBoth(digest.toBuilder().setIdentity(" ").build(), false);
        checkBoth(digest.toBuilder().setSha256("A".repeat(64)).build(), false);
        checkBoth(digest.toBuilder().setSizeBytes(-1).build(), false);

        var processing = processingSource();
        checkBoth(processing, true);
        checkBoth(processing.toBuilder().setPayload(ByteString.EMPTY).build(), true);
        checkBoth(processing.toBuilder().setAccountId(" ").build(), false);
        checkBoth(processing.toBuilder().clearMessageType().build(), false);
        checkBoth(processing.toBuilder().clearSchemaFingerprint().build(), false);
        checkBoth(processing.toBuilder().setDescriptors(ByteString.EMPTY).build(), false);

        var archiveSource = ArchiveRenditionSource.newBuilder()
                .setAddress(ENTRY)
                .setRevision(RevisionSelector.newBuilder().setCurrent(true))
                .setRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                .build();
        checkBoth(archiveSource, true);
        checkBoth(archiveSource.toBuilder().clearAddress().build(), false);
        checkBoth(archiveSource.toBuilder().clearRendition().build(), false);

        var docSource = DocumentPartSource.newBuilder()
                .setAddress(DOCUMENT)
                .setRevision(RevisionSelector.newBuilder().setRevision(4))
                .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                .setOccurrenceFieldPath("structured_data")
                .build();
        checkBoth(docSource, true);
        checkBoth(docSource.toBuilder().clearAddress().build(), false);
        checkBoth(docSource.toBuilder().clearSelector().build(), false);

        checkBoth(RepositoryRevisionSource.getDefaultInstance(), false);
        checkBoth(RepositoryRevisionSource.newBuilder().setArchiveRendition(archiveSource).build(), true);
        checkBoth(RepositoryRevisionSource.newBuilder().setDocumentPart(docSource).build(), true);

        checkBoth(SinkSource.getDefaultInstance(), false);
        checkBoth(SinkSource.newBuilder().setProcessingState(processing).build(), true);
        checkBoth(SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder().setArchiveRendition(archiveSource))
                .build(), true);
    }

    @Test void conversionAndDestinationEnforcePathSafetyAndPreconditionConsistency() throws Exception {
        var conversion = conversionSpec();
        checkBoth(conversion, true);
        checkBoth(conversion.toBuilder().setOptions(ByteString.EMPTY).build(), true);
        checkBoth(conversion.toBuilder().setProviderId(" ").build(), false);
        checkBoth(conversion.toBuilder().setProviderVersion("").build(), false);
        checkBoth(conversion.toBuilder().setSourceProfile(" ").build(), false);
        checkBoth(conversion.toBuilder().setMediaType("not-a-mime").build(), false);
        checkBoth(conversion.toBuilder().setExtension(".parquet").build(), false);
        checkBoth(conversion.toBuilder().setExtension("sub/file.parquet").build(), false);
        checkBoth(conversion.toBuilder().setOptionsType("").build(), false);
        checkBoth(conversion.toBuilder().setOptionsFingerprint(" ").build(), false);

        var destination = destination();
        checkBoth(destination, true);
        checkBoth(destination.toBuilder().setExpectedDestinationVersion(3).build(), true);
        checkBoth(destination.toBuilder().setCreateOnly(true).build(), true);
        checkBoth(destination.toBuilder().setCreateOnly(true).setExpectedDestinationVersion(1).build(), false);
        checkBoth(destination.toBuilder().setExpectedDestinationVersion(-1).build(), false);
        checkBoth(destination.toBuilder().clearAddress().build(), false);
        checkBoth(destination.toBuilder().clearRendition().build(), false);
        checkBoth(destination.toBuilder().setContentType("   ").build(), false);
    }

    @Test void provenanceDistinguishesDirectProtobufSavesFromDerivedRenditions() throws Exception {
        var direct = RenditionProvenance.newBuilder()
                .setOperationId("op-direct-1")
                .setSourceRevision(0)
                .setProcessingRunId("run-1")
                .setProcessingStepName("normalize")
                .addSourceDigests(SourcePartDigest.newBuilder()
                        .setIdentity("step:normalize").setSha256(SHA).setSizeBytes(48))
                .setSchemaType("example.CaseRecord")
                .setSchemaFingerprint(SHA)
                .setRetainedDescriptorSha256(SHA)
                .setAuthoritativeProtobuf(true)
                .setOutputMediaType("application/x-protobuf")
                .setBytesRendered(48)
                .setBytesCommitted(48)
                .setOutputSha256(SHA)
                .setDestinationAddress(ENTRY)
                .setDestinationRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                .setDestinationVersion(1)
                .setCommittedAt(COMMITTED_AT)
                .build();
        checkBoth(direct, true);
        checkBoth(direct.toBuilder().setAuthoritativeProtobuf(false).build(), false);
        checkBoth(direct.toBuilder().setProviderId("parquet-v1").build(), false);

        var derived = direct.toBuilder()
                .setSourceRevision(5)
                .setSourceEntry(ENTRY)
                .setSourceRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                .setAuthoritativeProtobuf(false)
                .setProviderId("parquet-v1")
                .setProviderVersion("1.0.0")
                .setSourceProfile("tabular.v1")
                .setOptionsType("example.ParquetOptions")
                .setOptionsFingerprint(SHA)
                .setOptionsSha256(SHA)
                .setLossDescription("Projects tabular columns only; unmapped nested extensions are omitted.")
                .setOutputMediaType("application/vnd.apache.parquet")
                .setDestinationRendition(RENDITION)
                .setDestinationVersion(6)
                .build();
        checkBoth(derived, true);
        // Derived renditions can never claim authoritative_protobuf or omit loss_description.
        checkBoth(derived.toBuilder().setAuthoritativeProtobuf(true).build(), false);
        checkBoth(derived.toBuilder().clearLossDescription().build(), false);
        checkBoth(derived.toBuilder().setDestinationVersion(0).build(), false);
        checkBoth(derived.toBuilder().setRetainedDescriptorSha256("BAD").build(), false);
        checkBoth(derived.toBuilder().setOptionsSha256("BAD").build(), false);
        checkBoth(derived.toBuilder().clearSourceDigests().build(), false);
    }

    @Test void capabilityJobAndServiceEnvelopesValidateAcrossAllStates() throws Exception {
        var capability = RendererCapability.newBuilder()
                .setProviderId("parquet-v1")
                .setProviderVersion("1.0.0")
                .setSourceProfile("tabular.v1")
                .setMediaType("application/vnd.apache.parquet")
                .setExtension("parquet")
                .setOptionsType("example.ParquetOptions")
                .setOptionsFingerprint(SHA)
                .setLossDescription("Tabular projection only.")
                .setMaxInputBytes(1_048_576)
                .setMaxOutputBytes(4_194_304)
                .build();
        var verdict = SaveAsCapability.newBuilder()
                .setCapability(capability)
                .setStatus(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE)
                .build();
        checkBoth(verdict, true);
        checkBoth(verdict.toBuilder()
                .setStatus(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_UNSPECIFIED)
                .build(), false);
        checkBoth(ListSaveAsCapabilitiesRequest.newBuilder()
                .setSource(SinkSource.newBuilder().setProcessingState(processingSource()))
                .build(), true);
        checkBoth(ListSaveAsCapabilitiesResponse.newBuilder()
                .setResolvedSourceRevision(0)
                .setSourceSchemaType("example.CaseRecord")
                .setSourceSchemaFingerprint(SHA)
                .setSourceSizeBytes(48)
                .addCapabilities(verdict)
                .build(), true);

        var jobBase = RepositorySinkJob.newBuilder()
                .setAccountId("acct-1")
                .setOperationId("op-1")
                .setIntentSha256(SHA)
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED)
                .setDestinationAddress(ENTRY)
                .setDestinationRendition(RENDITION)
                .setCreatedAt(COMMITTED_AT)
                .setUpdatedAt(COMMITTED_AT);
        for (var state : List.of(
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED)) {
            checkBoth(jobBase.clone().setState(state).build(), true);
        }
        checkBoth(jobBase.clone()
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNSPECIFIED)
                .build(), false);
        checkBoth(jobBase.clone().setStateValue(99).build(), false);
        checkBoth(jobBase.clone().setRootChecksum("NOT-HEX").build(), false);

        var failure = RepositorySinkFailure.newBuilder()
                .setGrpcCode(10)
                .setReasonCode("DESTINATION_CONFLICT")
                .setMessage("Expected destination version 2, found 3")
                .setConflict(true)
                .build();
        checkBoth(failure, true);
        checkBoth(failure.toBuilder().setGrpcCode(0).build(), false);
        checkBoth(failure.toBuilder().setGrpcCode(17).build(), false);

        var publishReq = PublishRepositorySinkRequest.newBuilder()
                .setSource(SinkSource.newBuilder().setProcessingState(processingSource()))
                .setConversion(conversionSpec())
                .setDestination(destination())
                .build();
        checkBoth(publishReq, true);
        checkBoth(publishReq.toBuilder().setOperationId("op-explicit").setAsync(true).build(), true);
        checkBoth(publishReq.toBuilder().setOperationId("   ").build(), false);
        checkBoth(PublishRepositorySinkResponse.newBuilder().setJob(jobBase).build(), true);

        checkBoth(GetRepositorySinkJobRequest.newBuilder()
                .setAccountId("acct-1").setOperationId("op-1").build(), true);
        checkBoth(GetRepositorySinkJobResponse.newBuilder().setJob(jobBase).build(), true);
        checkBoth(CancelRepositorySinkJobRequest.newBuilder()
                .setAccountId("acct-1").setOperationId("op-1").setReason("user cancelled").build(), true);
        checkBoth(CancelRepositorySinkJobResponse.newBuilder().setJob(jobBase).build(), true);

        var listProvReq = ListRenditionProvenanceRequest.newBuilder()
                .setAddress(ENTRY)
                .setVersion(0)
                .build();
        checkBoth(listProvReq, true);
        checkBoth(listProvReq.toBuilder().setVersion(-1).build(), false);
        var listProvResp = ListRenditionProvenanceResponse.newBuilder()
                .setAddress(ENTRY)
                .setVersion(1)
                .setCurrentEntryVersion(2)
                .addEntries(RenditionProvenanceEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                        .setProvenance(RenditionProvenance.newBuilder()
                                .setOperationId("op-1")
                                .addSourceDigests(SourcePartDigest.newBuilder()
                                        .setIdentity("step:a").setSha256(SHA).setSizeBytes(10))
                                .setSchemaType("example.CaseRecord")
                                .setSchemaFingerprint(SHA)
                                .setAuthoritativeProtobuf(true)
                                .setOutputMediaType("application/x-protobuf")
                                .setOutputSha256(SHA)
                                .setDestinationAddress(ENTRY)
                                .setDestinationRendition(RenditionDescriptor.newBuilder().setName("parsed"))
                                .setDestinationVersion(1)
                                .setCommittedAt(COMMITTED_AT))
                        .setCurrentSourceRevision(0)
                        .setSourceStale(false))
                .build();
        checkBoth(listProvResp, true);
        checkBoth(listProvResp.toBuilder().setVersion(0).build(), false);
    }

    @Test void descriptorClosureAndJsonSchemaProjectionRemainSelfContained() {
        var closure = DescriptorFingerprints.closure(PublishRepositorySinkRequest.getDescriptor());
        assertThat(closure.getFileList())
                .extracting(f -> f.getName())
                .contains(
                        "ai/protomolt/proto/repo/sink/v1/repository_sink.proto",
                        "ai/protomolt/proto/emit/v1/renderer.proto",
                        "ai/protomolt/proto/repo/archive/v1/entry.proto");
        assertThat(DescriptorFingerprints.fingerprint(closure)).hasSize(64);
        assertThat(PublishRepositorySinkRequest.getDescriptor().getFile()
                .findServiceByName("RepositorySinkService").getMethods())
                .extracting(m -> m.getName())
                .containsExactly(
                        "ListSaveAsCapabilities",
                        "PublishRepositorySink",
                        "GetRepositorySinkJob",
                        "CancelRepositorySinkJob",
                        "ListRenditionProvenance");

        var provenanceSchema = new ObjectMapper().valueToTree(
                ProtoJsonSchemaGenerator.create().generateRooted(RenditionProvenance.getDescriptor()));
        assertThat(provenanceSchema.path("x-protomolt-cel").toString())
                .contains("repository_sink.provenance.derived_not_authoritative");
    }

    private static ProcessingStateSource processingSource() {
        var closure = DescriptorFingerprints.closure(EntryAddress.getDescriptor());
        return ProcessingStateSource.newBuilder()
                .setAccountId("acct-1")
                .setRunId("run-7")
                .setStepName("extract")
                .setInputId("in-3")
                .setMessageType(EntryAddress.getDescriptor().getFullName())
                .setSchemaFingerprint(DescriptorFingerprints.fingerprint(closure))
                .setDescriptors(closure.toByteString())
                .setPayload(ENTRY.toByteString())
                .build();
    }

    private static ConversionSpec conversionSpec() {
        return ConversionSpec.newBuilder()
                .setProviderId("parquet-v1")
                .setProviderVersion("1.0.0")
                .setSourceProfile("tabular.v1")
                .setMediaType("application/vnd.apache.parquet")
                .setExtension("parquet")
                .setOptionsType("google.protobuf.Empty")
                .setOptionsFingerprint(SHA)
                .setMappingId("default-columns")
                .setPolicyId("internal-only")
                .build();
    }

    private static SinkDestination destination() {
        return SinkDestination.newBuilder()
                .setAddress(ENTRY)
                .setRendition(RENDITION)
                .setWrittenBy(WriteAttribution.newBuilder().setModule("repository-sink").setActor("user:alice"))
                .setTitle("Case 42 Parquet")
                .setContentType("application/vnd.apache.parquet")
                .build();
    }

    private static void checkBoth(Message value, boolean expected) throws Exception {
        for (NamedValidator engine : BOTH_ENGINES) {
            var generatedResult = engine.validator().validate(value);
            assertThat(generatedResult.valid())
                    .as("%s generated %s: %s",
                            engine.name(),
                            value.getDescriptorForType().getName(),
                            generatedResult.violations())
                    .isEqualTo(expected);
            var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
            var dynamicResult = engine.validator().validate(dynamic);
            assertThat(dynamicResult.valid())
                    .as("%s dynamic %s: %s",
                            engine.name(),
                            value.getDescriptorForType().getName(),
                            dynamicResult.violations())
                    .isEqualTo(expected);
        }
    }
}
