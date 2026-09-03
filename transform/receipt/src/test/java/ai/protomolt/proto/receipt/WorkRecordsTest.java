package ai.protomolt.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

class WorkRecordsTest {

    @Test
    void canonicalBytesAreStableAcrossCalls() {
        WorkRecord manifest = ConformanceCorpus.manifest().build();
        assertThat(WorkRecords.canonicalBytes(manifest))
                .isEqualTo(WorkRecords.canonicalBytes(manifest))
                .isEqualTo(WorkRecords.canonicalBytes(
                        ConformanceCorpus.manifest().build()));
    }

    @Test
    void sha256HexMatchesTheKnownEmptyDigest() {
        assertThat(WorkRecords.sha256Hex(new byte[0])).isEqualTo(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void unknownFieldsAreFoundAtDepth() {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                .build();
        WorkRecord clean = ConformanceCorpus.manifest().build();
        assertThat(WorkRecords.unknownFieldPaths(clean)).isEmpty();

        WorkRecord atRoot = clean.toBuilder().setUnknownFields(unknown).build();
        assertThat(WorkRecords.unknownFieldPaths(atRoot)).containsExactly("(root)");

        WorkRecord inStep = clean.toBuilder()
                .setSteps(1, clean.getSteps(1).toBuilder().setUnknownFields(unknown))
                .build();
        assertThat(WorkRecords.unknownFieldPaths(inStep)).containsExactly("steps[1]");

        WorkRecord inSubject = clean.toBuilder()
                .setSubject(clean.getSubject().toBuilder().setUnknownFields(unknown))
                .build();
        assertThat(WorkRecords.unknownFieldPaths(inSubject)).containsExactly("subject");
    }

    @Test
    void nullsRefuse() {
        assertThatThrownBy(() -> WorkRecords.canonicalBytes(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkRecords.sha256Hex(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
