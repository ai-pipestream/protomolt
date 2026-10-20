package ai.protomolt.proto.repo.engine;

/** The largest serialized request, response or manifest any repository admission accepts. */
public final class MessageCeiling {

    /** 256 MiB. */
    public static final int MAX_BYTES = 256 * 1024 * 1024;

    private MessageCeiling() {
    }
}
