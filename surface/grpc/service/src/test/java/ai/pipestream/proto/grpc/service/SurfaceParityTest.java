package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A verb answers the same thing whichever surface asked it.
 *
 * <p>This is the check the toolkit did not have, and the reason a handful of replies differed
 * between surfaces for as long as they did. Every verb was covered, including the branches
 * that turned out to be wrong; what no test did was ask the same verb over gRPC and compare.
 * The gRPC path parsed a verb's reply while ignoring members the response message did not
 * declare, so a field the verb wrote and the contract did not name was returned as JSON and
 * dropped over gRPC, and each surface's own tests passed.
 *
 * <p>Comparing the messages rather than the documents is deliberate: the JSON edge prints
 * fields that have no presence even at their default, so the two documents differ by design
 * while the messages must not.
 */
class SurfaceParityTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

    @Test
    void aVerbAnswersTheSameOverJsonAndOverGrpc() throws Exception {
        assertParity("compile", "Compile", """
                {"sources": {"a.proto": "syntax = \\"proto3\\"; package p; message A { string s = 1; }"}}
                """);
    }

    /** The failure branch too, which is where a reply is most likely to carry extra. */
    @Test
    void aRefusalAnswersTheSameOverJsonAndOverGrpc() throws Exception {
        assertParity("compile", "Compile", """
                {"sources": {"bad.proto": "syntax = \\"proto3\\"; message Broken { nope }"}}
                """);
    }

    @Test
    void aVerbWithNestedRepliesAnswersTheSameOverBoth() throws Exception {
        assertParity("list-types", "ListTypes", """
                {"schema": {"sources": {"a.proto":
                  "syntax = \\"proto3\\"; package p; message A { string s = 1; }"}}}
                """);
    }

    /**
     * Runs one verb both ways and holds the two replies to being the same message.
     *
     * <p>The JSON reply is read back through the response descriptor, so a member the verb
     * wrote and the contract does not declare fails here rather than being quietly dropped
     * on one side and returned on the other.
     */
    private void assertParity(String verb, String rpc, String request) throws Exception {
        MethodDescriptor method = ProtoMoltServiceSchema.service().findMethodByName(rpc);
        ObjectNode envelope = (ObjectNode) MAPPER.readTree(request);

        ObjectNode asJson = catalog.execute(verb, envelope.deepCopy());

        DynamicMessage.Builder typed = DynamicMessage.newBuilder(method.getInputType());
        JsonFormat.parser().merge(envelope.toString(), typed);
        Message overGrpc = CatalogBridge.execute(catalog, method, typed.build());

        Message reread = CatalogContract.toResponse(asJson, method.getOutputType(), verb);
        assertThat(reread)
                .as("%s answers the same message over JSON and over gRPC", verb)
                .isEqualTo(overGrpc);
    }
}
