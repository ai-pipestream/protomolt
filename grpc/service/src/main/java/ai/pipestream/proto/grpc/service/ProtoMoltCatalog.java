package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.chain.CheckChainAction;
import ai.pipestream.proto.chain.RunChainAction;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.emit.okf.EmitOkfAction;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ReflectAction;

import java.nio.file.Path;

/**
 * The full twenty-three-verb catalog: the sixteen built-in actions from
 * {@link ActionCatalog#defaults(ActionContext)} plus the gRPC verbs ({@code reflect},
 * {@code grpc-invoke}), {@code generate-stubs}, {@code gather-git}, the chain verbs
 * ({@code run-chain}, {@code check-chain}) and {@code emit-okf} — exactly the RPCs of
 * {@code ProtoMoltService}.
 *
 * <p>The MCP server exposes a subset: {@code protomolt-mcp} registers twenty, leaving out the
 * three that need server-side wiring ({@code run-chain}, {@code check-chain},
 * {@code emit-okf}), while the {@code /mcp} mount inside {@code protomolt-serve} carries all
 * twenty-three.
 */
public final class ProtoMoltCatalog {

    private ProtoMoltCatalog() {
    }

    public static ActionCatalog full(ActionContext context) {
        return full(context, null);
    }

    /**
     * @param gatherCacheRoot where {@code gather-git} keeps its per-repo clone caches; null
     *        for the library default under the process owner's home. Operator configuration —
     *        never taken from a request.
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot) {
        return full(context, gatherCacheRoot, null);
    }

    /**
     * @param chains where {@code run-chain} resolves stored chain names; null leaves the
     *        verb inline-only
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains) {
        return ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction(gatherCacheRoot))
                .register(new RunChainAction(new ChainRunner(), chains))
                .register(new CheckChainAction())
                .register(new EmitOkfAction());
    }
}
