package ai.protomolt.proto.descriptors;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DescriptorFingerprintsTest {
    @Test void preservesExistingCanonicalIdentityAndFileOrderIndependence() {
        var a = FileDescriptorProto.newBuilder().setName("a.proto").build();
        var b = FileDescriptorProto.newBuilder().setName("b.proto").build();
        // SHA-256 of the independently fixed canonical bytes:
        // 0a090a07612e70726f746f0a090a07622e70726f746f
        String expected = "d3c4d34b5ec2e9b260ab80af41371ecb32f4d3bb6c5114204a06783c7e6c833b";
        assertThat(DescriptorFingerprints.fingerprint(FileDescriptorSet.newBuilder().addFile(a).addFile(b).build())).isEqualTo(expected);
        assertThat(DescriptorFingerprints.fingerprint(FileDescriptorSet.newBuilder().addFile(b).addFile(a).build())).isEqualTo(expected);
    }

    @Test void preservesFileUnknownFieldsButNotSetEnvelopeUnknownFields() throws Exception {
        var unknown = UnknownFieldSet.newBuilder().addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(7).build()).build();
        var original = FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder().setName("a.proto")).build();
        var changed = original.toBuilder().setFile(0, original.getFile(0).toBuilder().setUnknownFields(unknown)).build();
        assertThat(DescriptorFingerprints.fingerprint(changed)).isNotEqualTo(DescriptorFingerprints.fingerprint(original));
        assertThat(DescriptorFingerprints.fingerprint(FileDescriptorSet.parseFrom(changed.toByteArray())))
                .isEqualTo(DescriptorFingerprints.fingerprint(changed));
        assertThat(DescriptorFingerprints.fingerprint(original.toBuilder().setUnknownFields(unknown).build()))
                .isEqualTo(DescriptorFingerprints.fingerprint(original));
    }

    @Test void linkedClosureKeepsImportsAndCanDecodeWithoutRegistry() throws Exception {
        var type = com.google.protobuf.Type.getDescriptor();
        var closure = DescriptorFingerprints.closure(type);
        assertThat(closure.getFileCount()).isGreaterThan(1);
        var linked = GoogleDescriptorLoader.fromDescriptorSet(FileDescriptorSet.parseFrom(closure.toByteArray()));
        var restored = linked.stream().filter(file -> file.getName().equals(type.getFile().getName())).findFirst().orElseThrow()
                .findMessageTypeByName(type.getName());
        assertThat(DescriptorFingerprints.fingerprintOf(restored)).isEqualTo(DescriptorFingerprints.fingerprintOf(type));
        var original = com.google.protobuf.Type.newBuilder().setName("example.Record").build();
        var decoded = com.google.protobuf.DynamicMessage.parseFrom(restored, original.toByteArray());
        assertThat(decoded.getField(restored.findFieldByName("name"))).isEqualTo("example.Record");
    }
}
