package ai.protomolt.proto.delegation.contract;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ContractPathTest {
    @Test void nestedTypeSegmentsKeepTheOriginalTrailingEmptySemantics() {
        for (String name : List.of("", ".", "..", "Outer.Inner", ".Outer", "Outer..Inner.", "Outer..."))
            assertArrayEquals(name.split("\\."), DeliverableContracts.splitPath(name), name);
    }
}
