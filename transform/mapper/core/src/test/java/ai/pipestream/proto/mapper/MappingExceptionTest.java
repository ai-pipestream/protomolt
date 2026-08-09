package ai.pipestream.proto.mapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Message formatting contract of {@link MappingException}. */
class MappingExceptionTest {

    @Test
    void ruleTextIsAppendedToTheMessage() {
        MappingException e = new MappingException("bad path", "a = b");
        assertEquals("bad path (Rule: 'a = b')", e.getMessage());
    }

    @Test
    void nullRuleLeavesTheMessageAlone() {
        MappingException e = new MappingException("bad path", (String) null);
        assertEquals("bad path", e.getMessage());
    }

    @Test
    void causeIsPreservedAlongsideTheRule() {
        RuntimeException cause = new RuntimeException("boom");
        MappingException e = new MappingException("bad path", cause, "a = b");
        assertEquals("bad path (Rule: 'a = b')", e.getMessage());
        assertSame(cause, e.getCause());
    }
}
