package ai.protomolt.proto.descriptors;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpRouteContractTest {

    private static HttpRouteContract route(String pathTemplate, List<String> methods) {
        return new HttpRouteContract(
                "test:route",
                pathTemplate,
                methods,
                List.of(),
                List.of(),
                "",
                "application/json",
                "",
                "test.Response",
                List.of(200, 404),
                true,
                StabilityTier.STABLE);
    }

    @Test
    void constructorRejectsEveryNullComponent() {
        List<String> methods = List.of("GET");
        List<String> none = List.of();
        List<Integer> codes = List.of(200);
        assertThatThrownBy(() -> new HttpRouteContract(null, "/a", methods, none, none,
                "", "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("routeId");
        assertThatThrownBy(() -> new HttpRouteContract("r", null, methods, none, none,
                "", "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("pathTemplate");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", null, none, none,
                "", "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("httpMethods");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, null, none,
                "", "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("pathParameters");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, null,
                "", "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("requiredQueryParameters");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                null, "", "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("requestMediaType");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                "", null, "", "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("responseMediaType");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                "", "", null, "", codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("requestType");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                "", "", "", null, codes, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("responseType");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                "", "", "", "", null, false, StabilityTier.STABLE))
                .isInstanceOf(NullPointerException.class).hasMessage("successStatusCodes");
        assertThatThrownBy(() -> new HttpRouteContract("r", "/a", methods, none, none,
                "", "", "", "", codes, false, null))
                .isInstanceOf(NullPointerException.class).hasMessage("stability");
    }

    @Test
    void listComponentsAreImmutableCopies() {
        List<String> methods = new ArrayList<>(List.of("GET"));
        HttpRouteContract contract = route("/a", methods);
        methods.add("DELETE");

        assertThat(contract.httpMethods()).containsExactly("GET");
        assertThatThrownBy(() -> contract.httpMethods().add("PUT"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> contract.successStatusCodes().add(500))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void allowsMethodNormalizesCaseAndRefusesNullAndUndeclaredMethods() {
        HttpRouteContract contract = route("/a", List.of("GET", "PUT"));

        assertThat(contract.allowsMethod("GET")).isTrue();
        assertThat(contract.allowsMethod("put")).isTrue();
        assertThat(contract.allowsMethod("POST")).isFalse();
        assertThat(contract.allowsMethod(null)).isFalse();
        assertThat(contract.allowHeader()).isEqualTo("GET, PUT");
    }

    @Test
    void matchesSegmentsTreatsBracedSegmentsAsParametersAndLiteralsExactly() {
        HttpRouteContract contract = route("/subjects/{subject}/versions/{version}", List.of("GET"));

        assertThat(contract.matchesSegments(List.of("subjects", "orders", "versions", "3"))).isTrue();
        assertThat(contract.matchesSegments(List.of("subjects", "orders", "versions"))).isFalse();
        assertThat(contract.matchesSegments(List.of("subjects", "orders", "version", "3"))).isFalse();
        assertThat(contract.matchesSegments(List.of("subjects", "orders", "versions", "3", "x"))).isFalse();

        // Empty template segments (leading, doubled or trailing slashes) are ignored.
        assertThat(route("//config//", List.of("GET")).matchesSegments(List.of("config"))).isTrue();
        // "{}" has no parameter name, so it is a literal segment.
        assertThat(route("/a/{}", List.of("GET")).matchesSegments(List.of("a", "b"))).isFalse();
        assertThat(route("/a/{}", List.of("GET")).matchesSegments(List.of("a", "{}"))).isTrue();
        assertThatThrownBy(() -> contract.matchesSegments(null))
                .isInstanceOf(NullPointerException.class).hasMessage("requestSegments");
    }
}
