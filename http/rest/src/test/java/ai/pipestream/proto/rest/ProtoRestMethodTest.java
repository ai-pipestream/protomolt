package ai.pipestream.proto.rest;

import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoRestMethodTest {

    private static final Function<Message, Message> INVOKER = request -> Struct.getDefaultInstance();

    @Test
    void httpMethodsAreDefensivelyCopiedAndUppercased() {
        String[] verbs = {"post", "Put"};
        ProtoRestMethod method = ProtoRestMethod.builder("S", "m", INVOKER)
                .httpMethods(verbs)
                .build();

        verbs[0] = "HACKED";
        assertThat(method.httpMethods()).containsExactly("POST", "PUT");

        method.httpMethods()[0] = "HACKED";
        assertThat(method.httpMethods()).containsExactly("POST", "PUT");
    }

    @Test
    void unsetHttpMethodsMeansAllStandardVerbs() {
        ProtoRestMethod method = ProtoRestMethod.builder("S", "m", INVOKER).build();
        assertThat(method.httpMethods()).isEmpty();
        assertThat(method.allowedHttpVerbs()).isEqualTo(ProtoRestMethod.DEFAULT_HTTP_VERBS);

        ProtoRestMethod declared = ProtoRestMethod.builder("S", "m", INVOKER)
                .httpMethods("DELETE")
                .build();
        assertThat(declared.allowedHttpVerbs()).containsExactly("DELETE");
    }

    @Test
    void equalsAndHashCodeUseArrayContents() {
        ProtoRestMethod a = ProtoRestMethod.builder("S", "m", INVOKER).httpMethods("POST").build();
        ProtoRestMethod b = ProtoRestMethod.builder("S", "m", INVOKER).httpMethods("POST").build();
        ProtoRestMethod c = ProtoRestMethod.builder("S", "m", INVOKER).httpMethods("GET").build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }
}
