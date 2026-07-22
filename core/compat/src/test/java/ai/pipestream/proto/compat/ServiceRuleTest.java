package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.compat.TestSchemas.diff;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;

/** Service and method rules: the gRPC surface. */
class ServiceRuleTest {

    private static String schema(String service) {
        return """
                syntax = "proto3";
                package example;
                message Req { string q = 1; }
                message Res { string a = 1; }
                message Req2 { string q = 1; }
                message Res2 { string a = 1; }
                %s
                """.formatted(service);
    }

    private static final String BASE =
            schema("service Search { rpc Query(Req) returns (Res); }");

    @Test
    void serviceRemovedBreaksForwardAndSource() throws Exception {
        SchemaChange change = single(diff(BASE, schema("")), "SERVICE_REMOVED");

        assertThat(change.path()).isEqualTo("example.Search");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.WIRE_FORWARD, Impact.SOURCE);
    }

    @Test
    void serviceAddedIsInformational() throws Exception {
        SchemaChange change = single(diff(schema(""), BASE), "SERVICE_ADDED");

        assertThat(change.path()).isEqualTo("example.Search");
        assertThat(change.isInformational()).isTrue();
    }

    @Test
    void methodRemovedBreaksForwardAndSource() throws Exception {
        String twoMethods = schema("""
                service Search {
                  rpc Query(Req) returns (Res);
                  rpc Suggest(Req) returns (Res);
                }
                """);

        SchemaChange change = single(diff(twoMethods, BASE), "METHOD_REMOVED");
        assertThat(change.path()).isEqualTo("example.Search.Suggest");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.WIRE_FORWARD, Impact.SOURCE);
    }

    @Test
    void methodAddedIsInformational() throws Exception {
        String twoMethods = schema("""
                service Search {
                  rpc Query(Req) returns (Res);
                  rpc Suggest(Req) returns (Res);
                }
                """);

        assertThat(single(diff(BASE, twoMethods), "METHOD_ADDED").isInformational()).isTrue();
    }

    @Test
    void requestTypeChangeBreaksWireAndSource() throws Exception {
        String updated = schema("service Search { rpc Query(Req2) returns (Res); }");

        SchemaChange change = single(diff(BASE, updated), "METHOD_REQUEST_TYPE_CHANGED");
        assertThat(change.path()).isEqualTo("example.Search.Query");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.SOURCE);
    }

    @Test
    void responseTypeChangeBreaksWireAndSource() throws Exception {
        String updated = schema("service Search { rpc Query(Req) returns (Res2); }");

        SchemaChange change = single(diff(BASE, updated), "METHOD_RESPONSE_TYPE_CHANGED");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.SOURCE);
    }

    @Test
    void serverStreamingFlagChangeBreaksWireAndSource() throws Exception {
        String updated = schema("service Search { rpc Query(Req) returns (stream Res); }");

        SchemaChange change = single(diff(BASE, updated), "METHOD_STREAMING_CHANGED");
        assertThat(change.path()).isEqualTo("example.Search.Query");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.SOURCE);
    }

    @Test
    void clientStreamingFlagChangeBreaksWireAndSource() throws Exception {
        String updated = schema("service Search { rpc Query(stream Req) returns (Res); }");

        SchemaChange change = single(diff(BASE, updated), "METHOD_STREAMING_CHANGED");
        assertThat(change.impacts()).contains(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD);
    }

    @Test
    void identicalServiceProducesNoChanges() throws Exception {
        assertThat(diff(BASE, BASE)).isEmpty();
    }
}
