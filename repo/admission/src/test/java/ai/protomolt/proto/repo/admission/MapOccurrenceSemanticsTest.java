package ai.protomolt.proto.repo.admission;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Characterizes why a typed map key alone cannot identify duplicate decoded entries. */
class MapOccurrenceSemanticsTest {
    @Test void dynamicMapRetainsDuplicateEntriesWhileGeneratedMapKeepsLastValue() throws Exception {
        var bytes=encoded(List.of(entry("same","first"),entry("same","second")));
        var dynamic=DynamicMessage.parseFrom(Struct.getDescriptor(),bytes);
        var fields=Struct.getDescriptor().findFieldByNumber(1);
        assertThat(dynamic.getRepeatedFieldCount(fields)).isEqualTo(2);
        assertThat(Struct.parseFrom(bytes).getFieldsCount()).isEqualTo(1);
        assertThat(Struct.parseFrom(bytes).getFieldsOrThrow("same").getStringValue()).isEqualTo("second");
        assertThat(dynamic.toByteString()).isEqualTo(bytes);
    }

    @Test void omittedAndExplicitDefaultKeysReferToTheSameMapKey() throws Exception {
        var bytes=encoded(List.of(entry(null,"first"),entry("","second")));
        var dynamic=DynamicMessage.parseFrom(Struct.getDescriptor(),bytes);
        var fields=Struct.getDescriptor().findFieldByNumber(1);
        var first=(Message)dynamic.getRepeatedField(fields,0);
        var second=(Message)dynamic.getRepeatedField(fields,1);
        var key=fields.getMessageType().findFieldByNumber(1);
        assertThat(first.getField(key)).isEqualTo(second.getField(key)).isEqualTo("");
        assertThat(Struct.parseFrom(bytes).getFieldsCount()).isEqualTo(1);
        assertThat(Struct.parseFrom(bytes).getFieldsOrThrow("").getStringValue()).isEqualTo("second");
    }

    private static DynamicMessage entry(String key,String value) {
        var type=Struct.getDescriptor().findFieldByNumber(1).getMessageType();
        var entry=DynamicMessage.newBuilder(type).setField(type.findFieldByNumber(2),Value.newBuilder().setStringValue(value).build());
        if (key!=null) entry.setField(type.findFieldByNumber(1),key);
        return entry.build();
    }
    private static com.google.protobuf.ByteString encoded(List<DynamicMessage> entries) {
        var root=DynamicMessage.newBuilder(Struct.getDescriptor());
        var field=Struct.getDescriptor().findFieldByNumber(1);
        entries.forEach(entry->root.addRepeatedField(field,entry));
        return root.build().toByteString();
    }
}
