package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamDeliveryFailureTest {
    @Test
    void closedOutputCannotBeSuppressedByAStreamingProvider() throws IOException {
        OutputStream output = OutputStream.nullOutputStream();
        output.close();
        var caught = new ArrayList<RuntimeException>();
        var deliveries = new AtomicInteger();
        var action = new StreamingAction() {
            public String name() { return "probe"; }
            public String description() { return "delivery failure test"; }
            public Descriptors.Descriptor requestType() { return Empty.getDescriptor(); }
            public Descriptors.Descriptor responseType() { return Empty.getDescriptor(); }
            public Message execute(Message request, ActionContext context) { return Empty.getDefaultInstance(); }
            public void executeStreaming(Message request, ActionContext context, StreamEmitter emitter)
                    throws ActionException {
                for (int i = 0; i < 2; i++) {
                    try { emitter.emit(Empty.getDefaultInstance()); }
                    catch (RuntimeException failure) { caught.add(failure); }
                }
            }
        };
        var catalog = ActionCatalog.empty(ActionContext.create()).register(action);
        assertThatThrownBy(() -> catalog.executeStreaming("probe", Empty.getDefaultInstance(), message -> {
            deliveries.incrementAndGet();
            try { output.write(message.toByteArray()); }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        })).isInstanceOfSatisfying(UncheckedIOException.class, failure -> {
            assertThat(failure.getCause()).isInstanceOf(IOException.class);
            assertThat(caught).hasSize(2);
            assertThat(caught.get(0)).isSameAs(failure);
            assertThat(caught.get(1)).isSameAs(failure);
        });
        assertThat(deliveries).hasValue(1);
    }
}
