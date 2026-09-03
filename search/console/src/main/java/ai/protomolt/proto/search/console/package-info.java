/**
 * The search console: the product page over the search service. One pure-JDK HTTP server serves a
 * single no-build-step page (the playground idiom) and bridges it onto the service's gRPC surface
 * as JSON ({@code /subjects}, {@code /search}) plus a same-origin proxy onto the registry's
 * actions route ({@code /actions/*}) for operations — replay, job inspection, connector pulls.
 */
package ai.protomolt.proto.search.console;
