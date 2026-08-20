package ai.pipestream.proto.http.rest;

import com.google.protobuf.Message;
import java.util.Map;

/**
 * An invoker that also needs the request's transport context — today, the headers a caller
 * identity is resolved from. A {@link ProtoRestMethod#invoker()} implementing this interface
 * is handed the normalized (lowercase-name) headers and query parameters by the gateway,
 * after token validation; a plain {@code Function} invoker is called exactly as before. The
 * optional-interface shape mirrors {@code StreamingAction} on the action catalog.
 */
public interface ProtoRestContextInvoker {

    /** Invokes with the request's normalized headers and query parameters. */
    Message invoke(Message request, Map<String, String> headers, Map<String, String> queryParams);
}
