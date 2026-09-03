package ai.protomolt.proto.formats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EndpointsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "localhost:9090",              // plain host:port
            "node.example:443",            // dotted hostname
            "127.0.0.1:80",                // IPv4 host
            "[::1]:9090",                  // bracketed IPv6 (deliberate widening: the old
                                           // pattern could not express it)
            "host-1.internal:65535",       // top of the port range
            "https://example.com/path",    // absolute URI
            "http://example.com:8080/x",   // URI with a numeric port: the slash keeps it
                                           // off the authority reading
            "http://example.com:8080",     // same without a path: the slashes in the
                                           // "host half" keep it off the authority reading
            "unix:/tmp/protomolt.sock",    // scheme with a path
            "dns:///service.example",      // gRPC-style target
            "myscheme:12345",              // ambiguous, valid under both readings
    })
    void endpointAddressAccepts(String value) {
        assertThat(Endpoints.isEndpointAddress(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "not an address",              // spaces, no scheme
            "localhost",                   // no port, no scheme
            "localhost:99999",             // authority shape: the bad port never falls
                                           // back to the URI reading
            "localhost:0",                 // port zero is not dialable
            "localhost:0080",              // leading-zero port (tightening: the old hand
                                           // check parsed it as 80)
            "12345:80",                    // all-numeric host is neither hostname nor IP
            "-host.example:80",            // hostname may not start with a dash
            "host.example:",               // empty port half reads as a URI with an
                                           // empty rest, which is not an absolute URI
            "://missing-scheme",
    })
    void endpointAddressRejects(String value) {
        assertThat(Endpoints.isEndpointAddress(value)).isFalse();
    }
}
