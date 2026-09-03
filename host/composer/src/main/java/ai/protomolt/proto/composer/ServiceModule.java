package ai.protomolt.proto.composer;

import java.util.Set;

/**
 * One mountable role of a protomolt node. Each {@code -service} module
 * publishes an implementation via {@link java.util.ServiceLoader} so a single
 * binary can boot as any combination of roles: the full document monolith,
 * an accounts node, an inference node, or anything between. Composition is
 * data ({@code PROTOMOLT_ROLES}), not per-app wiring.
 *
 * <p>Lifecycle is two-phase. {@link #wire(NodeContext)} constructs the
 * module's services, publishes in-process endpoints, and registers
 * {@linkplain NodeContext#contributions() contributions} without serving
 * yet; {@link ServiceMount#start()} then binds network ports, starts
 * workers, and begins loops, in the same dependency order. Two phases exist
 * because contribution hosts must observe every contribution before they
 * serve: the registry's actions route can only be built after later-wired
 * modules (jobs) have contributed their verbs, while those same modules
 * need the registry's store at wire time. A single phase cannot order that.
 */
public interface ServiceModule {

    /**
     * The stable role name this module mounts as, lowercase
     * (e.g. {@code "repo"}, {@code "intake"}, {@code "jobs"}). Role names
     * are the vocabulary of {@code PROTOMOLT_ROLES} and of
     * {@code PROTOMOLT_<ROLE>_TARGET} remote-channel configuration.
     */
    String role();

    /**
     * Roles that must be wired before this one. A required role absent
     * from the boot set is not automatically an error: the module may
     * reach it remotely through {@link NodeContext#channels()}, which
     * fails loudly only when neither a co-mounted endpoint nor a
     * configured target exists.
     */
    default Set<String> requires() {
        return Set.of();
    }

    /**
     * Phase one: construct, publish in-process endpoints, contribute.
     * Must not bind network ports or start background work.
     *
     * @param context the node's shared spine
     * @return the mount whose {@link ServiceMount#start()} begins serving
     * @throws Exception construction failures propagate and abort the boot
     */
    ServiceMount wire(NodeContext context) throws Exception;
}
