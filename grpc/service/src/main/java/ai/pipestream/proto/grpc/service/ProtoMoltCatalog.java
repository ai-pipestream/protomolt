package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ReflectAction;

/**
 * The full fourteen-verb catalog: the ten built-in actions plus the gRPC verbs
 * ({@code reflect}, {@code grpc-invoke}), {@code generate-stubs}, and {@code gather-git} — the same surface the
 * MCP server exposes, and exactly the RPCs of {@code ProtoMoltService}.
 */
public final class ProtoMoltCatalog {

    private ProtoMoltCatalog() {
    }

    public static ActionCatalog full(ActionContext context) {
        return ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction())
                .register(new ReflectAction())
                .register(new GenerateStubsAction())
                .register(new GatherGitAction());
    }
}
