package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Pure snapshot assembly tests. Physical identities are synthetic, not provider qualification. */
class DocumentCommitWriterTest {
    @Test void preservesDeclaredUploadProvenanceAndLeavesUnknownProvenanceAbsent() throws Exception {
        var provenance=WriteProvenance.newBuilder().setModuleId("parser").setNodeId("producer")
                .setGraphId("source-graph").setGraphVersion(7).build();
        for (boolean declared:new boolean[]{true,false}) {
            var ownership=OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                    .setSecurity(DocumentSecurity.getDefaultInstance()).build();
            var document=Document.newBuilder().setDocId("doc").setOwnership(ownership).build();
            var fragments=DocumentPartCodec.split(document,PartLayouts.document());
            var drive=new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account";
            drive.name="logical"; drive.provider="test-provider"; drive.bucket="namespace"; drive.status="ACTIVE";
            var profile=new ManagedBackendLedger.Profile(new BackendIdentity("test-provider","test-provider/v1",Map.of("endpoint","synthetic")),"realm");
            var member=DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                    .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                            .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId("doc")))
                    .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE);
            var bytes=new HashMap<Integer,ByteString>();
            var physical=new HashMap<DocumentCommitParts.Slot,DocumentCommitParts.Physical>();
            for (int i=0;i<fragments.size();i++) {
                var part=fragments.get(i);
                var upload=PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256()).setContentType("application/protobuf");
                if (declared) upload.setWrittenBy(provenance);
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(part.part()).setSubKey(part.subKey())).setUpload(upload));
                bytes.put(i,ByteString.copyFrom(part.bytes()));
                physical.put(new DocumentCommitParts.Slot("member",i),new DocumentCommitParts.Physical(UUID.randomUUID(),part.part().getNumber(),
                        part.subKey(),"key/"+i,part.bytes().length,part.sha256(),"application/protobuf","version","etag","generation","realm","namespace"));
            }
            var command=new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                    .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
            var plan=DocumentUploadPlan.prepare(command,Map.of(drive.driveId,DocumentUploadPlan.Placement.sample(drive,"generation",profile)),Map.of("member",UUID.randomUUID()));
            var content=DocumentCommandContent.check(command,"member",bytes,false,new DocumentRevisionAssembly.Limits(100_000,10,100,100,100_000),()->{});
            var candidate=DocumentCommitWriter.prepare(plan.members().getFirst(),content,new DocumentCommitParts.Bound(physical,Map.of("member",1L)),
                    Map.of(),new HashMap<>(),Instant.parse("2026-10-04T00:00:00Z"),()->{});
            assertThat(candidate.row().objectKey).isNull();
            for (var entry:candidate.row().readManifest().getPartsList()) {
                assertThat(entry.hasWrittenBy()).isEqualTo(declared);
                if (declared) assertThat(entry.getWrittenBy()).isEqualTo(provenance);
            }
        }
    }
}
