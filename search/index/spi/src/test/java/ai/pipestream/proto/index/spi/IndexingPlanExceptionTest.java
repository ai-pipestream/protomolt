package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IndexingPlanExceptionTest {

    @Test
    void messageCarriesTheOffendingFieldPath() {
        IndexingPlanException exception = new IndexingPlanException("bad hint", "doc.pages");

        assertThat(exception).isInstanceOf(RuntimeException.class);
        assertThat(exception.getMessage()).isEqualTo("bad hint (Field: 'doc.pages')");
        assertThat(exception.path()).isEqualTo("doc.pages");
    }

    @Test
    void nullPathLeavesTheMessageUntouched() {
        IndexingPlanException exception = new IndexingPlanException("bad hint", null);

        assertThat(exception.getMessage()).isEqualTo("bad hint");
        assertThat(exception.path()).isNull();
    }
}
