package ai.pipestream.proto.registry;

/** One refused config document, with the reason named. */
public final class InvalidConfigException extends Exception {

    public InvalidConfigException(String message) {
        super(message);
    }
}
