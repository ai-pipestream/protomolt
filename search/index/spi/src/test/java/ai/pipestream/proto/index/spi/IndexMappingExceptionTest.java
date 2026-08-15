package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IndexMappingExceptionTest {

    @Test
    void messageCarriesTheOffendingFieldPath() {
        IndexMappingException exception = new IndexMappingException("bad hint", "doc.pages");

        assertThat(exception).isInstanceOf(RuntimeException.class);
        assertThat(exception.getMessage()).isEqualTo("bad hint (Field: 'doc.pages')");
        assertThat(exception.path()).isEqualTo("doc.pages");
    }

    @Test
    void nullPathLeavesTheMessageUntouched() {
        IndexMappingException exception = new IndexMappingException("bad hint", null);

        assertThat(exception.getMessage()).isEqualTo("bad hint");
        assertThat(exception.path()).isNull();
    }
}
