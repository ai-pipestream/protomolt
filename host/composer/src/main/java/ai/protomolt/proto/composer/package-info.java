/**
 * Role-based node composition. A protomolt binary boots as any set of
 * roles ({@code PROTOMOLT_ROLES=repo,intake,parse,jobs,registry}): each
 * {@code -service} module publishes a {@link
 * ai.protomolt.proto.composer.ServiceModule} via {@link
 * java.util.ServiceLoader}, the {@link ai.protomolt.proto.composer.Composer}
 * orders them by requirements and runs the two-phase wire/start lifecycle,
 * and {@link ai.protomolt.proto.composer.Channels} pivots each
 * cross-role call between in-process (co-mounted) and remote
 * ({@code PROTOMOLT_<ROLE>_TARGET}) without the calling module knowing the
 * topology. The monolith and the specialized node are the same code booted
 * with different role lists.
 */
package ai.protomolt.proto.composer;
